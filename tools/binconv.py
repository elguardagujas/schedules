#!/usr/bin/env python3

# Converts a timeschedule database into a XUXU binary format file.
# This file can be easily memory mapped for most operations.

import argparse, sqlite3, hashlib, struct

MAGIC   = b"\x58\x55\x58\x55"
VERSION = (1, 0)

def align4(n: int) -> int:
  return (n + 3) & ~3

def make_structs(e: str) -> dict:
  return {
    "header":    struct.Struct(f"{e}4s HH II HH II IIIIIII Q"),   # 64 bytes
    "trip_day":  struct.Struct(f"{e}IIII"),                       # 16 bytes
    "trip":      struct.Struct(f"{e}III hh 32s H 14s 8s"),        # 72 bytes
    "station":   struct.Struct(f"{e}Iii"),                        # 12 bytes
    "shape_hdr": struct.Struct(f"{e}II"),                         # 8 bytes
    "shape_pt":  struct.Struct(f"{e}ii"),                         # 8 bytes
    "tt_hdr":    struct.Struct(f"{e}I"),                          # 4 bytes
    "tt_stop":   struct.Struct(f"{e}Ihh"),                        # 8 bytes
  }

def encode_date(table_suffix: str) -> int:
  return int(table_suffix)  # already YYYYMMDD


def pad4(s: bytes) -> bytes:
  rem = len(s) % 4
  return s + b"\x00" * (4 - rem) if rem else s


def bstr(s: str | None, n: int) -> bytes:
  return (s or "").encode("utf-8")[:n].ljust(n, b"\x00")


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

def build_timetables(conn: sqlite3.Connection, S: dict) -> tuple[bytes, dict[int, int]]:
  out = bytearray()
  offsets: dict[int, int] = {}
  for tt_id, data in conn.execute("SELECT timetable_id, data FROM timetable_stops ORDER BY timetable_id"):
    offsets[tt_id] = len(out)
    out += S["tt_hdr"].pack(len(data) // TT_STOP_SRC.size)
    out += transcode_blob(data, TT_STOP_SRC, S["tt_stop"])
  return bytes(out), offsets


def build_trip_days(conn: sqlite3.Connection, S: dict,
                    tt_offsets: dict[int, int]) -> tuple[bytes, bytes, list[int]]:
  tables = [r[0] for r in conn.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'trips_%' ORDER BY name"
  )]

  day_table = bytearray()
  trip_blob = bytearray()
  day_dates = []

  for table in tables:
    date_int   = encode_date(table[6:])  # trips_YYYYMMDD -> YYYYMMDD
    trip_start = len(trip_blob)

    trips = conn.execute(f"""
      SELECT trip_id, trip_short_name, route_short_name,
             origin_id, destination_id, start_time, end_time,
             timetable_id, shape_id
      FROM "{table}" ORDER BY trip_id ASC
    """).fetchall()

    for trip_id, trip_sn, route_sn, orig, dest, t0, t1, tt_id, shape_id in trips:
      trip_blob += S["trip"].pack(
        orig, dest,
        tt_offsets.get(tt_id, 0),
        t0 or 0, t1 or 0,
        bstr(trip_id, 32),
        shape_id or 0,
        bstr(route_sn, 14),
        bstr(trip_sn, 8),
      )

    day_table += S["trip_day"].pack(date_int, len(trips), trip_start, 0)
    day_dates.append(date_int)

  return bytes(day_table), bytes(trip_blob), day_dates


def main():
  parser = argparse.ArgumentParser(description="Convert GTFS SQLite DB to binary mmap format.")
  parser.add_argument("db",  help="Input SQLite database")
  parser.add_argument("out", help="Output binary file")
  parser.add_argument("--big-endian", action="store_true", help="Use big-endian byte order")
  args = parser.parse_args()

  e = ">" if args.big_endian else "<"
  S = make_structs(e)

  conn = sqlite3.connect(args.db)

  print("Building stations...")  ;  stations_data        = build_stations(conn, S)
  print("Building shapes...")    ;  shapes_data           = build_shapes(conn, S)
  print("Building timetables...");  tt_data, tt_offsets   = build_timetables(conn, S)
  print("Building trip days...")  ; day_data, trip_data, day_dates = build_trip_days(conn, S, tt_offsets)

  n_timetables = conn.execute("SELECT COUNT(*) FROM timetable_stops").fetchone()[0]
  n_stations   = conn.execute("SELECT COUNT(*) FROM stations").fetchone()[0]
  n_shapes     = conn.execute("SELECT COUNT(*) FROM shapes").fetchone()[0]
  conn.close()

  HEADER_SIZE  = 64
  off_stations = 0
  off_shapes   = off_stations + len(stations_data)
  off_tt       = off_shapes   + len(shapes_data)
  off_days     = off_tt       + len(tt_data)
  off_trips    = off_days     + len(day_data)

  header = S["header"].pack(
    MAGIC, VERSION[0], VERSION[1],
    day_dates[0] if day_dates else 0, day_dates[-1] if day_dates else 0,
    n_stations, n_shapes, n_timetables, len(day_dates),
    off_stations, off_shapes, off_tt, len(tt_data),
    off_days, off_trips, len(trip_data), 0,
  )
  assert len(header) == HEADER_SIZE

  total = HEADER_SIZE + len(stations_data) + len(shapes_data) + len(tt_data) + len(day_data) + len(trip_data)
  pload = header + stations_data + shapes_data + tt_data + day_data + trip_data
  # Calculate CRC and patch the payload
  crc64 = hashlib.sha256(pload).digest()[:8]
  pload = pload[:56] + crc64 + pload[64:]

  with open(args.out, "wb") as f:
    f.write(pload)

  print(f"Written {total:,} bytes to {args.out}")
  print(f"  stations={n_stations}  timetables={n_timetables}  days={len(day_dates)}")


if __name__ == "__main__":
  main()

