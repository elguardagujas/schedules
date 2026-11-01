#!/usr/bin/env python3

# Converts a timeschedule database into a XUXU binary format file.
# This file can be easily memory mapped for most operations.

import argparse, sqlite3, hashlib, struct

MAGIC   = b"\x58\x55\x58\x55"
VERSION = (2, 0)

def align4(n: int) -> int:
  return (n + 3) & ~3

def make_structs(e: str) -> dict:
  return {
    "header":    struct.Struct(f"{e}4s HH II HH II IIIIIIIII Q"), # 72 bytes
    "trip_day":  struct.Struct(f"{e}IIII"),                       # 16 bytes
    "trip":      struct.Struct(f"{e}II"),                         # 8 bytes: tmpl offset, trip_id string offset
    "trip_tmpl": struct.Struct(f"{e}III hh H"),                   # 18 bytes + route_sn\0 trip_sn\0, padded to 4
    "station":   struct.Struct(f"{e}Iii"),                        # 12 bytes
    "shape_hdr": struct.Struct(f"{e}II"),                         # 8 bytes
    "shape_pt":  struct.Struct(f"{e}ii"),                         # 8 bytes
    "tt_hdr":    struct.Struct(f"{e}I"),                          # 4 bytes
    "tt_stop":   struct.Struct(f"{e}Ihh"),                        # 8 bytes
  }


def pad4(s: bytes) -> bytes:
  rem = len(s) % 4
  return s + b"\x00" * (4 - rem) if rem else s


class StringPool:
  """Deduplicated pool of NUL-terminated UTF-8 strings, referenced by byte offset."""
  def __init__(self):
    self.data = bytearray(b"\x00")   # offset 0 is the empty string
    self.offs = {"": 0}

  def add(self, s: str | None) -> int:
    s = s or ""
    if s not in self.offs:
      self.offs[s] = len(self.data)
      self.data += s.encode("utf-8") + b"\x00"
    return self.offs[s]


def build_stations(conn: sqlite3.Connection, S: dict) -> bytes:
  out = bytearray()
  for stop_id, name, lat, lon, region in conn.execute(
    "SELECT stop_id, stop_name, stop_lat, stop_lon, stop_region FROM stations ORDER BY stop_id"):
    enc = name.encode("utf-8") + b"\x00" + (region or "").encode("utf-8") + b"\x00"
    out += S["station"].pack(stop_id, lat, lon)
    out += enc + b"\x00" * (align4(len(enc)) - len(enc))
  # pad blob to 8-byte boundary
  rem = len(out) % 8
  if rem:
    out += b"\x00" * (8 - rem)
  return bytes(out)


def transcode_blob(data: bytes, src: struct.Struct, dst: struct.Struct) -> bytes:
  out = bytearray()
  for i in range(len(data) // src.size):
    out += dst.pack(*src.unpack_from(data, i * src.size))
  return bytes(out)


SHAPE_PT_SRC = struct.Struct("<ii")  # SQLite storage is always LE

def build_shapes(conn: sqlite3.Connection, S: dict) -> bytes:
  out = bytearray()
  for shape_id, data in conn.execute("SELECT shape_id, data FROM shapes ORDER BY shape_id"):
    n_pts = len(data) // SHAPE_PT_SRC.size
    out += S["shape_hdr"].pack(shape_id, n_pts)
    out += transcode_blob(data, SHAPE_PT_SRC, S["shape_pt"])
  return bytes(out)


TT_STOP_SRC = struct.Struct("<Ihh")  # SQLite storage is always LE

def build_timetables(conn: sqlite3.Connection, S: dict, needed: set) -> tuple[bytes, dict[int, int]]:
  out = bytearray()
  offsets: dict[int, int] = {}
  for tt_id, data in conn.execute("SELECT timetable_id, data FROM timetable_stops ORDER BY timetable_id"):
    if tt_id not in needed:
      continue
    offsets[tt_id] = len(out)
    out += S["tt_hdr"].pack(len(data) // TT_STOP_SRC.size)
    out += transcode_blob(data, TT_STOP_SRC, S["tt_stop"])
  return bytes(out), offsets


def build_trip_days(conn: sqlite3.Connection, S: dict,
                    tt_offsets: dict[int, int],
                    wfilter: str) -> tuple[bytes, bytes, list[int], bytes, bytes]:

  pool = StringPool()                   # trip_ids
  tmpl_blob = bytearray()               # deduplicated trip templates (all fields but trip_id)
  tmpl_offs: dict[bytes, int] = {}

  def add_tmpl(t: bytes) -> int:
    if t not in tmpl_offs:
      tmpl_offs[t] = len(tmpl_blob)
      tmpl_blob.extend(t)
    return tmpl_offs[t]

  dates = [r[0] for r in conn.execute(
    "SELECT DISTINCT trip_date FROM trips " + wfilter + " ORDER BY trip_date"
  )]

  day_table = bytearray()
  trip_blob = bytearray()
  day_dates = []

  for date_int in dates:
    trip_start = len(trip_blob)

    trips = conn.execute(f"""
      SELECT trip_id, trip_short_name, route_short_name,
             origin_id, destination_id, start_time, end_time,
             timetable_id, shape_id
      FROM trips WHERE trip_date = ? ORDER BY trip_id ASC
    """, (date_int,)).fetchall()

    for trip_id, trip_sn, route_sn, orig, dest, t0, t1, tt_id, shape_id in trips:
      tmpl = S["trip_tmpl"].pack(orig, dest, tt_offsets.get(tt_id, 0), t0 or 0, t1 or 0, shape_id or 0)
      tmpl += (route_sn or "").encode("utf-8") + b"\x00" + (trip_sn or "").encode("utf-8") + b"\x00"
      trip_blob += S["trip"].pack(add_tmpl(pad4(tmpl)), pool.add(trip_id))

    day_table += S["trip_day"].pack(date_int, len(trips), trip_start, 0)
    day_dates.append(date_int)

  return bytes(day_table), bytes(trip_blob), day_dates, bytes(tmpl_blob), bytes(pool.data)


def main():
  parser = argparse.ArgumentParser(description="Convert GTFS SQLite DB to binary mmap format.")
  parser.add_argument("db",  help="Input SQLite database")
  parser.add_argument("out", help="Output binary file")
  parser.add_argument("--big-endian", action="store_true", help="Use big-endian byte order")
  parser.add_argument("--from-date", type=int, metavar="YYYYMMDD", help="First date to include (inclusive)")
  parser.add_argument("--to-date",   type=int, metavar="YYYYMMDD", help="Last date to include (inclusive)")
  args = parser.parse_args()

  e = ">" if args.big_endian else "<"
  S = make_structs(e)

  conn = sqlite3.connect(args.db)

  wfilter = ' WHERE 1==1 '
  if args.from_date:
    wfilter += ' AND trip_date >= %d' % args.from_date
  if args.to_date:
    wfilter += ' AND trip_date <= %d' % args.to_date

  needed_tt = {r[0] for r in conn.execute(f"SELECT DISTINCT timetable_id FROM trips" + wfilter)}

  print("Building stations...")  ;  stations_data         = build_stations(conn, S)
  print("Building shapes...")    ;  shapes_data           = build_shapes(conn, S)
  print("Building timetables...");  tt_data, tt_offsets   = build_timetables(conn, S, needed_tt)
  print("Building trip days...")  ; day_data, trip_data, day_dates, tmpl_data, str_data = build_trip_days(conn, S, tt_offsets, wfilter)

  n_timetables = len(needed_tt)
  n_stations   = conn.execute("SELECT COUNT(*) FROM stations").fetchone()[0]
  n_shapes     = conn.execute("SELECT COUNT(*) FROM shapes").fetchone()[0]
  conn.close()

  HEADER_SIZE  = 72
  off_stations = 0
  off_shapes   = off_stations + len(stations_data)
  off_tt       = off_shapes   + len(shapes_data)
  off_days     = off_tt       + len(tt_data)
  off_trips    = off_days     + len(day_data)
  off_tmpls    = off_trips    + len(trip_data)
  off_strings  = off_tmpls    + len(tmpl_data)   # runs to end of file

  header = S["header"].pack(
    MAGIC, VERSION[0], VERSION[1],
    day_dates[0] if day_dates else 0, day_dates[-1] if day_dates else 0,
    n_stations, n_shapes, n_timetables, len(day_dates),
    off_stations, off_shapes, off_tt, len(tt_data),
    off_days, off_trips, len(trip_data), off_tmpls, off_strings, 0,
  )
  assert len(header) == HEADER_SIZE

  total = HEADER_SIZE + len(stations_data) + len(shapes_data) + len(tt_data) + len(day_data) + len(trip_data) + len(tmpl_data) + len(str_data)
  pload = header + stations_data + shapes_data + tt_data + day_data + trip_data + tmpl_data + str_data
  # Calculate CRC and patch the payload
  crc64 = hashlib.sha256(pload).digest()[:8]
  pload = pload[:HEADER_SIZE - 8] + crc64 + pload[HEADER_SIZE:]

  with open(args.out, "wb") as f:
    f.write(pload)

  print(f"Written {total:,} bytes to {args.out}")
  print(f"  stations={n_stations}  timetables={n_timetables}  days={len(day_dates)}")


if __name__ == "__main__":
  main()

