
# Loads and parses GTFS files into a python-friendly usable structure.

import zipfile, csv, io
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from collections import defaultdict
from dataclasses import dataclass, field

@dataclass
class StopTime:
    stop_id: str
    arrival: int | None    # minutes from UTC midnight, may be negative
    departure: int | None
    sequence: int


@dataclass
class TripTimetable:
    trip_id: str
    trip_short_name: str
    route_id: str
    route_short_name: str
    shape_id: str
    origin_id: str
    origin_name: str
    destination_id: str
    destination_name: str
    start_time: int | None  # minutes from UTC midnight
    end_time: int | None
    stops: list[StopTime] = field(default_factory=list)


def _parse_gtfs_time(gtfs_time: str) -> timedelta:
    h, m, s = map(int, gtfs_time.split(":"))
    return timedelta(hours=h, minutes=m, seconds=s)


def _to_utc_minutes(gtfs_time: str, day: date, tz_name: str) -> int | None:
    if not gtfs_time:
        return None
    offset = _parse_gtfs_time(gtfs_time)
    midnight = datetime(day.year, day.month, day.day, tzinfo=ZoneInfo(tz_name))
    utc_dt = (midnight + offset).astimezone(timezone.utc)
    utc_midnight = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    delta = utc_dt - utc_midnight
    return int(delta.total_seconds() // 60)


def _build_parent_map(raw_stops: list[dict]) -> dict[str, str]:
    # stop_id -> parent_station (empty string if root)
    parent = {r["stop_id"]: r.get("parent_station", "") for r in raw_stops}

    def root(sid: str) -> str:
        seen = set()
        while parent.get(sid) and sid not in seen:
            seen.add(sid)
            sid = parent[sid]
        return sid

    return {sid: root(sid) for sid in parent}


class GTFSReader:
    def __init__(self, zip_path: str):
        self.zip_path = zip_path
        self.agency_timezones: dict[str, str] = {}
        self.trips = {}
        self.routes = {}
        self.shapes = {}
        self.stops = {}          # only root stations
        self.stop_times = defaultdict(list)
        self.calendar = {}
        self.calendar_dates = defaultdict(dict)
        self._load()

    def _read_csv(self, zf: zipfile.ZipFile, filename: str):
        names = [n for n in zf.namelist() if n.endswith(filename)]
        if not names:
            return []
        with zf.open(names[0]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            reader.fieldnames = [h.strip() for h in reader.fieldnames]
            return [{k.strip(): v.strip() for k, v in row.items()} for row in reader]

    def _load(self):
        with zipfile.ZipFile(self.zip_path) as zf:
            for a in self._read_csv(zf, "agency.txt"):
                agency_id = a.get("agency_id", "default")
                self.agency_timezones[agency_id] = a.get("agency_timezone", "UTC")

            for r in self._read_csv(zf, "routes.txt"):
                agency_id = r.get("agency_id", next(iter(self.agency_timezones), "default"))
                self.routes[r["route_id"]] = {
                    "short_name": r.get("route_short_name", ""),
                    "agency_id": agency_id,
                }

            shapes = defaultdict(dict)
            for r in self._read_csv(zf, "shapes.txt"):
                shapes[r["shape_id"]][int(r["shape_pt_sequence"])] = {
                    "lat": r["shape_pt_lat"],
                    "lon": r["shape_pt_lon"],
                }
            self.shapes = {k: [v for _, v in sorted(d.items())] for k, d in shapes.items()}

            raw_stops = self._read_csv(zf, "stops.txt")
            parent_map = _build_parent_map(raw_stops)
            # only store root stations (those whose root is themselves)
            self.stops = {
                r["stop_id"]: {
                  "name": r.get("stop_name", r["stop_id"]),
                  "pos": (float(r["stop_lat"]), float(r["stop_lon"]))
                          if "stop_lat" in r and "stop_lon" in r else None
                }
                for r in raw_stops
                if parent_map[r["stop_id"]] == r["stop_id"]
            }

            for t in self._read_csv(zf, "trips.txt"):
                self.trips[t["trip_id"]] = {
                    "service_id": t["service_id"],
                    "route_id": t["route_id"],
                    "shape_id": t.get("shape_id", ""),
                    "short_name": t.get("trip_short_name", ""),
                }

            for st in self._read_csv(zf, "stop_times.txt"):
                self.stop_times[st["trip_id"]].append({
                    "stop_id": parent_map.get(st["stop_id"], st["stop_id"]),
                    "arrival": st.get("arrival_time", ""),
                    "departure": st.get("departure_time", ""),
                    "sequence": int(st.get("stop_sequence", 0)),
                })

            for c in self._read_csv(zf, "calendar.txt"):
                self.calendar[c["service_id"]] = {
                    "days": [c.get(d, "0") for d in
                             ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")],
                    "start": datetime.strptime(c["start_date"], "%Y%m%d").date(),
                    "end": datetime.strptime(c["end_date"], "%Y%m%d").date(),
                }

            for cd in self._read_csv(zf, "calendar_dates.txt"):
                d = datetime.strptime(cd["date"], "%Y%m%d").date()
                self.calendar_dates[cd["service_id"]][d] = int(cd["exception_type"])

    def _service_runs_on(self, service_id: str, day: date) -> bool:
        exception = self.calendar_dates.get(service_id, {}).get(day)
        if exception == 1:
            return True
        if exception == 2:
            return False
        cal = self.calendar.get(service_id)
        if not cal:
            return False
        return cal["start"] <= day <= cal["end"] and cal["days"][day.weekday()] == "1"

    def get_stops(self) -> dict[str, str]:
        return dict(self.stops)

    def get_routes(self) -> dict[str, dict]:
        return dict(self.routes)

    def get_shapes(self) -> dict[str, dict]:
        return dict(self.shapes)

    def get_timetable(self, day: date) -> list[TripTimetable]:
        results = []

        for trip_id, trip in self.trips.items():
            if not self._service_runs_on(trip["service_id"], day):
                continue

            raw_stops = sorted(self.stop_times.get(trip_id, []), key=lambda x: x["sequence"])
            if not raw_stops:
                continue

            route = self.routes.get(trip["route_id"], {})
            agency_id = route.get("agency_id", next(iter(self.agency_timezones), "default"))
            tz_name = self.agency_timezones.get(agency_id, "UTC")

            stop_list = [
                StopTime(
                    stop_id=s["stop_id"],
                    arrival=_to_utc_minutes(s["arrival"], day, tz_name),
                    departure=_to_utc_minutes(s["departure"], day, tz_name),
                    sequence=s["sequence"],
                )
                for s in raw_stops
            ]

            first, last = stop_list[0], stop_list[-1]
            results.append(TripTimetable(
                trip_id=trip_id,
                trip_short_name=trip["short_name"],
                route_id=trip["route_id"],
                route_short_name=route.get("short_name", ""),
                shape_id=trip["shape_id"],
                origin_id=first.stop_id,
                origin_name=self.stops.get(first.stop_id, {"name": first.stop_id})["name"],
                destination_id=last.stop_id,
                destination_name=self.stops.get(last.stop_id, {"name": last.stop_id})["name"],
                start_time=first.departure or first.arrival,
                end_time=last.arrival or last.departure,
                stops=stop_list,
            ))

        return results

