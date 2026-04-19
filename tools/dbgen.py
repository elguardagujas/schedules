#!/usr/bin/env python3

import argparse, bisect, hashlib, itertools, re, sqlite3, struct, json, os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
import shapely.geometry

from gtfs_timetable import GTFSReader, TripTimetable

STOP_STRUCT  = struct.Struct("<Ihh")
SHAPE_STRUCT = struct.Struct("<ii")
FILENAME_RE  = re.compile(r"^(.+)_(\d{4}-\d{2}-\d{2})_(\d{2})-(\d{2})\.zip$")

STOP_ID_RULES: list[tuple[re.Pattern, callable]] = [
  (re.compile(r"^(\d+)$"), lambda m: int(m.group(1))),
]

def coords_to_region(regmap, lat, lon):
  pt = shapely.geometry.Point(lon, lat)
  for feature in regmap["features"]:
    if shapely.geometry.shape(feature["geometry"]).contains(pt):
      return feature["properties"]["acom_name"]

def stop_id_to_int(stop_id: str) -> int:
  for pattern, fn in STOP_ID_RULES:
    m = pattern.match(stop_id)
    if m:
      return fn(m)
  raise ValueError(f"Unrecognized stop ID format: {stop_id!r}")

def encode_stops(trip: TripTimetable) -> bytes:
  if trip.start_time is None:
    raise ValueError(f"Trip {trip.trip_id!r} has no start time")
  parts = []
  for s in trip.stops:
    if s.arrival is None or s.departure is None:
      raise ValueError(f"Stop {s.stop_id!r} in trip {trip.trip_id!r} has missing arrival or departure")
    parts.append(STOP_STRUCT.pack(stop_id_to_int(s.stop_id), s.arrival - trip.start_time, s.departure - trip.start_time))
  return b"".join(parts)

def encode_shape(points: list[dict]) -> bytes:
  parts = []
  for p in points:
    parts.append(SHAPE_STRUCT.pack(int(float(p["lat"]) * 1e7), int(float(p["lon"]) * 1e7)))
  return b"".join(parts)

def hash_blob(blob: bytes) -> bytes:
  return hashlib.sha256(blob).digest()


# --- schema ---

def init_schema(conn: sqlite3.Connection):
  conn.executescript("""
    CREATE TABLE IF NOT EXISTS stations (
      stop_id      INTEGER PRIMARY KEY,
      stop_name    TEXT NOT NULL,
      stop_lat     INTEGER,
      stop_lon     INTEGER,
      stop_region  TEXT
    ) STRICT;

    CREATE TABLE IF NOT EXISTS shapes (
      shape_id  INTEGER PRIMARY KEY,
      data      BLOB NOT NULL
    ) STRICT;

    CREATE TABLE IF NOT EXISTS timetable_stops (
      timetable_id INTEGER PRIMARY KEY,
      num_stops    INTEGER NOT NULL,
      data         BLOB NOT NULL
    ) STRICT;

    CREATE TABLE IF NOT EXISTS trips (
      trip_id           TEXT NOT NULL,
      trip_date         INTEGER NOT NULL,
      trip_short_name   TEXT,
      route_id          TEXT,
      route_short_name  TEXT,
      origin_id         INTEGER,
      destination_id    INTEGER,
      start_time        INTEGER,
      end_time          INTEGER,
      timetable_id      INTEGER NOT NULL,
      shape_id          INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (trip_id, trip_date)
    ) STRICT;
  """)


# --- station merging ---

ACCENTED = set("áéíóúàèìòùâêîôûäëïöüãõñýÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÄËÏÖÜÃÕÑÝ")

def has_accents(name: str) -> bool:
  return any(c in ACCENTED for c in name)

def uppercase_count(name: str) -> int:
  return sum(1 for c in name[1:] if c.isupper())

def best_name(a: str, b: str) -> str:
  score_a  = (1 if has_accents(a) else 0)
  score_a += (1 if len(a) > len(b) else 0)
  score_a += (1 if uppercase_count(a) < uppercase_count(b) else 0)
  score_b  = (1 if has_accents(b) else 0)
  score_b += (1 if len(b) > len(a) else 0)
  score_b += (1 if uppercase_count(b) < uppercase_count(a) else 0)
  return b if score_b > score_a else a

def merge_stations(conn: sqlite3.Connection, stops, known, regmap):
  for stop_id, stop_info in stops.items():
    iid = stop_id_to_int(stop_id)
    if iid in known:
      winner = best_name(known[iid], stop_info["name"])
      if winner != known[iid]:
        known[iid] = winner
        conn.execute("UPDATE stations SET stop_name = ? WHERE stop_id = ?", (winner, iid))
    else:
      known[iid] = stop_info["name"]
      if stop_info["pos"] is None:
        conn.execute("INSERT INTO stations (stop_id, stop_name) VALUES (?, ?)", (iid, stop_info["name"]))
      else:
        slat = int(stop_info["pos"][0] * 1e7)
        slon = int(stop_info["pos"][1] * 1e7)
        sreg = coords_to_region(regmap, stop_info["pos"][0], stop_info["pos"][1])
        conn.execute("INSERT INTO stations (stop_id, stop_name, stop_lat, stop_lon, stop_region) "
                     "VALUES (?, ?, ?, ?, ?)", (iid, stop_info["name"], slat, slon, sreg))


def merge_shapes(conn: sqlite3.Connection, reader: GTFSReader,
                 known_shapes: dict[str, int], next_shape_id) -> dict[str, int]:
  shape_id_map: dict[str, int] = {}
  for sid, points in reader.get_shapes().items():
    blob = encode_shape(points)
    if sid in known_shapes:
      db_id = known_shapes[sid]
      conn.execute("UPDATE shapes SET data = ? WHERE shape_id = ?", (blob, db_id))
    else:
      db_id = next(next_shape_id)
      conn.execute("INSERT INTO shapes (shape_id, data) VALUES (?, ?)", (db_id, blob))
      known_shapes[sid] = db_id
    shape_id_map[sid] = db_id
  return shape_id_map


@dataclass
class ProviderState:
  name:         str
  entries:      list[tuple[datetime, Path]]   # sorted by date ascending
  loaded_path:  Path | None               = None
  reader:       GTFSReader | None         = None
  known_shapes: dict[str, int]            = field(default_factory=dict)
  shape_id_map: dict[str, int]            = field(default_factory=dict)

  def pick_path(self, day: date, cutoff: datetime) -> Path | None:
    # keep files whose schedule date <= day AND were published before the cutoff
    valid = [(dt, p) for dt, p in self.entries if dt.date() <= day and dt < cutoff]
    return valid[-1][1] if valid else None  # latest qualifying file

  def ensure_loaded(self, day: date, cutoff: datetime) -> tuple[bool, bool]:
    """Returns (available, reloaded)."""
    path = self.pick_path(day, cutoff)
    if path is None:
      return False, False
    if path != self.loaded_path:
      print(f"  [{self.name}] loading {path.name}")
      self.reader      = GTFSReader(str(path))
      self.loaded_path = path
      return True, True
    return True, False

def scan_providers(directory: Path) -> list[ProviderState]:
  groups: dict[str, list[tuple[datetime, Path]]] = {}
  for path in sorted(directory.glob("*.zip")):
    m = FILENAME_RE.match(path.name)
    if not m:
      continue
    basename, date_str = m.group(1), m.group(2)
    pub_dt = datetime(*map(int, date_str.split("-")), int(m.group(3)), int(m.group(4)), tzinfo=timezone.utc)
    groups.setdefault(basename, []).append((pub_dt, path))
  return [ProviderState(name=name, entries=sorted(entries)) for name, entries in groups.items()]


# --- db inserts ---

def insert_timetable(conn: sqlite3.Connection, timetable_id: int, num_stops: int, blob: bytes):
  conn.execute(
    "INSERT INTO timetable_stops (timetable_id, num_stops, data) VALUES (?, ?, ?)",
    (timetable_id, num_stops, blob)
  )

def insert_trip(conn: sqlite3.Connection, day: date, trip: TripTimetable,
                timetable_id: int, shape_id: int):
  conn.execute("""
    INSERT INTO trips
      (trip_id, trip_date, trip_short_name, route_id, route_short_name,
       origin_id, destination_id, start_time, end_time, timetable_id, shape_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  """, (
    trip.trip_id, int(day.strftime('%Y%m%d')), trip.trip_short_name, trip.route_id, trip.route_short_name,
    stop_id_to_int(trip.origin_id), stop_id_to_int(trip.destination_id),
    trip.start_time, trip.end_time, timetable_id, shape_id
  ))


# --- main ---

def main():
  parser = argparse.ArgumentParser(description="Export GTFS timetable data to SQLite.")
  parser.add_argument("db",        help="Path to output SQLite database")
  parser.add_argument("start",     help="Start date (YYYY-MM-DD, inclusive)")
  parser.add_argument("end",       help="End date (YYYY-MM-DD, exclusive)")
  parser.add_argument("directory", help="Directory containing GTFS zip files")
  parser.add_argument("--regmap",  default=os.path.join(os.path.dirname(__file__), "data/esp_reg_map.geojson"), help="Geojson region map")
  parser.add_argument("--station-dump", type=str, help="Output station list in a JSON file")
  parser.add_argument("--vacuum",  default=False, action="store_true", help="Vacuum the database")
  parser.add_argument("--cutoff", default="04:00 Europe/Madrid", help="Don't use a file for day X if published after HH:MM <tz> on day X (default: '04:00 Europe/Madrid')")
  args = parser.parse_args()

  if os.path.isfile(args.regmap):
    regmap = json.load(open(args.regmap))
  else:
    print("No regmap file could be loaded")

  start = date.fromisoformat(args.start)
  end   = date.fromisoformat(args.end)
  if end <= start:
    raise ValueError(f"End date {end} must be after start date {start}")

  cutoff_time_str, cutoff_tz_str = args.cutoff.rsplit(" ", 1)
  cutoff_hh, cutoff_mm = map(int, cutoff_time_str.split(":"))
  cutoff_tz = ZoneInfo(cutoff_tz_str)

  providers = scan_providers(Path(args.directory))
  if not providers:
    raise ValueError(f"No GTFS files found in {args.directory}")
  print(f"Found {len(providers)} provider(s): {', '.join(p.name for p in providers)}")

  conn = sqlite3.connect(args.db)
  conn.executescript("""
    PRAGMA journal_mode = MEMORY;
    PRAGMA synchronous  = OFF;
    PRAGMA temp_store   = MEMORY;
    PRAGMA locking_mode = EXCLUSIVE;
  """)
  init_schema(conn)

  known_stations:   dict[int, str]   = {}
  seen_timetables:  dict[bytes, int] = {}
  next_timetable_id = itertools.count(1)
  next_shape_id     = itertools.count(1)  # 0 reserved for "no shape"

  day = start
  while day < end:
    trip_count = 0
    local_cutoff = datetime(day.year, day.month, day.day, cutoff_hh, cutoff_mm, tzinfo=cutoff_tz)
    utc_cutoff   = local_cutoff.astimezone(timezone.utc)

    for provider in providers:
      available, reloaded = provider.ensure_loaded(day, utc_cutoff)
      if not available:
        continue
      if reloaded:
        provider.shape_id_map = merge_shapes(conn, provider.reader, provider.known_shapes, next_shape_id)
        merge_stations(conn, provider.reader.get_stops(), known_stations, regmap)
        conn.commit()

      for trip in provider.reader.get_timetable(day):
        blob = encode_stops(trip)
        h    = hash_blob(blob)
        if h not in seen_timetables:
          tid = next(next_timetable_id)
          seen_timetables[h] = tid
          insert_timetable(conn, tid, len(trip.stops), blob)
        shape_id = provider.shape_id_map.get(trip.shape_id, 0) if trip.shape_id else 0
        insert_trip(conn, day, trip, seen_timetables[h], shape_id)
        trip_count += 1

    conn.commit()
    print(f"  {day}  {trip_count:>5} trips")
    day += timedelta(days=1)

  if args.station_dump:
    dump = {}
    r = conn.execute("SELECT stop_id, stop_name, stop_lat, stop_lon, stop_region FROM stations")
    for sid, sname, slat, slon, sreg in r.fetchall():
      dump[sid] = {"name": sname, "latitude": slat, "longitude": slon, "region": sreg}
    with open(args.station_dump, "w") as ofd:
      json.dump(dump, ofd, indent=2)

  if args.vacuum:
    conn.execute("VACUUM")
    conn.commit()
  conn.close()
  print("Done.")

if __name__ == "__main__":
  main()
