#!/usr/bin/env python3
"""
================================================================================
  SPACE2EARTH - NASA POWER decision support for farmers (Urdu / English)
  Independent project (not affiliated with or endorsed by NASA). Developed by the
  Space2Earth team (Peshawar) with AI assistance (Claude, Anthropic) - see README.
================================================================================
  Dependencies: ONLY  flask  and  requests   (everything else is Python standard library)

    pip install flask requests            # (+ gunicorn for production)

  COMMANDS
    python space2earth.py                 run the app            -> http://127.0.0.1:5000
    python space2earth.py --test          offline tests (no internet), writes test_results.json
    python space2earth.py --demo          tests + live data checks (+ validation if station.csv exists)
    python space2earth.py --live          one real call to each data service (run this first on a new network)
    python space2earth.py --validate FILE.csv --lat 34.0151 --lon 71.5249
                                          compare NASA POWER with your own observations
    gunicorn -w 1 --threads 8 -b 0.0.0.0:$PORT space2earth:app      (production)

  ENVIRONMENT (all optional)
    PORT, HOST            server address (default 127.0.0.1:5000)
    TRUST_PROXY=1         ONLY when behind a proxy such as Render/nginx
    DATABASE_PATH         SQLite file for anonymous usage logs (default space2earth.db)
    DISABLE_TELEMETRY=1   do not write usage logs at all
    RATE_PER_MINUTE, RATE_PER_HOUR   per-IP request limits (default 30 / 600)
    ENABLE_NDVI=1         show the EXPERIMENTAL satellite-greenness tab (run --live first)
    LOG_LEVEL             INFO, DEBUG, ...

  TABS: Live (now + 7 days) | Rotation (Field Shift crop-rotation explorer) | Validate (your own data)
        AgriSpace | BioShield | EcoGuard (NASA POWER daily indicators)
  Everything shown is computed from the data services listed in REFERENCES.md; nothing is invented.
  Air quality is not a NASA POWER product, so it is never fabricated.
================================================================================
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import math
import os
import re
import secrets
import sqlite3
import sys
import struct
import threading
import time
import zlib
from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import lru_cache
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import requests
from flask import Flask, jsonify, request
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from werkzeug.middleware.proxy_fix import ProxyFix

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

VERSION = "3.2.1"

INDEPENDENCE = {
    "project": "Space2Earth - independent project (Peshawar, Pakistan)",
    "nasa_affiliation": "None. This is not a NASA product and is not endorsed by NASA. NASA POWER and NASA GIBS are "
                        "public NASA data services used as inputs; the data belongs to NASA, the software does not.",
    "own_contribution": "The software, how indicators are computed and compared, the rotation scoring, the validation "
                        "workflow and the interface.",
    "ai_assistance": "Code was written with substantial AI assistance (Claude, Anthropic); see the README.",
    "status": "Research prototype. Not validated at your location unless you run the Validate tab with your own observations.",
}
CAT_NASA = {"label": "NASA POWER - satellite/reanalysis model estimate, not a field measurement", "provider": "NASA",
            "resolution": "~0.5 deg (~50 km)", "latency": "3-7 days"}
CAT_FORECAST = {"label": "Open-Meteo - weather-model forecast, not NASA", "provider": "Open-Meteo", "resolution": "~11 km (model dependent)"}
CAT_OBS = {"label": "Your own ground observations - measured, not NASA", "provider": "user"}
RELIABILITY = {  # general behaviour of reanalysis products - NOT measured at the user's location
    "T2M": "higher", "T2M_MAX": "higher", "T2M_MIN": "higher", "PS": "higher",
    "RH2M": "medium", "WS2M": "medium", "ALLSKY_SFC_SW_DWN": "medium", "ALLSKY_SFC_UV_INDEX": "medium",
    "PRECTOTCORR": "lower", "GWETTOP": "lower", "GWETROOT": "lower"}


def uncertainty_for(coverage: dict) -> dict:
    out = {}
    for p, c in coverage.items():
        lvl = "lower" if c < 50 else RELIABILITY.get(p, "medium")
        out[p] = {"expected_reliability": lvl, "coverage_pct": c,
                  "basis": "General behaviour of reanalysis/satellite-derived products; NOT validated at your location. "
                           "Use the Validate tab with your own observations."}
    return out

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("space2earth")


# ==============================================================================
# Small infrastructure pieces (stdlib only, thread-safe)
# ==============================================================================
class TTLCache:
    """Tiny thread-safe LRU cache with per-entry expiry."""

    def __init__(self, max_items: int = 512):
        self._d: "OrderedDict[str, tuple[float, object]]" = OrderedDict()
        self._lock = threading.Lock()
        self._max = max_items

    def get(self, key: str):
        with self._lock:
            item = self._d.get(key)
            if item is None:
                return None
            expires, value = item
            if expires < time.monotonic():
                del self._d[key]
                return None
            self._d.move_to_end(key)
            return value

    def set(self, key: str, value, ttl: float):
        with self._lock:
            self._d[key] = (time.monotonic() + ttl, value)
            self._d.move_to_end(key)
            while len(self._d) > self._max:
                self._d.popitem(last=False)

    def clear(self):
        with self._lock:
            self._d.clear()


class RateLimiter:
    """Sliding-window limiter: `limit` hits per `window` seconds per key."""

    def __init__(self, limit: int, window: float):
        self.limit, self.window = limit, window
        self._hits: "dict[str, deque]" = {}
        self._lock = threading.Lock()

    def check(self, key: str):
        now = time.monotonic()
        with self._lock:
            q = self._hits.setdefault(key, deque())
            while q and q[0] <= now - self.window:
                q.popleft()
            if len(q) >= self.limit:
                return False, max(1, int(self.window - (now - q[0])) + 1)
            q.append(now)
            if len(self._hits) > 20000:  # memory guard
                for k in [k for k, v in self._hits.items() if not v]:
                    del self._hits[k]
            return True, 0

    def reset(self):
        with self._lock:
            self._hits.clear()


cache = TTLCache()
def _env_int(name: str, default: int) -> int:
    try:
        v = int(os.getenv(name, default))
        return v if v > 0 else default
    except ValueError:
        return default


# Generous defaults: judges and a whole event hall often share one public IP address.
limit_minute = RateLimiter(_env_int("RATE_PER_MINUTE", 30), 60)
limit_hour = RateLimiter(_env_int("RATE_PER_HOUR", 600), 3600)
limit_geo = RateLimiter(20, 60)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 262144      # /api/validate may carry a CSV; other endpoints are capped at 4 KB below
app.config["RATE_LIMIT_ENABLED"] = True
try:
    app.json.sort_keys = False
except AttributeError:  # very old Flask
    pass
if os.getenv("TRUST_PROXY") == "1":
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)


# ------------------------------------------------------------------------------
# SQLite telemetry (stdlib sqlite3, one short-lived connection per call)
# ------------------------------------------------------------------------------
DB_PATH = os.getenv("DATABASE_PATH", "space2earth.db")
_db_lock = threading.Lock()


def _db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db():
    with _db_lock, _db_connect() as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS telemetry_logs (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   module TEXT NOT NULL,
                   location TEXT NOT NULL,
                   details TEXT,
                   timestamp TEXT NOT NULL)"""
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_ts ON telemetry_logs(timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_mod ON telemetry_logs(module)")


def save_log(module: str, location: str, details: str):
    if os.getenv("DISABLE_TELEMETRY") == "1":
        return
    try:
        with _db_lock, _db_connect() as conn:
            conn.execute(
                "INSERT INTO telemetry_logs(module, location, details, timestamp) VALUES (?,?,?,?)",
                (module[:50], location[:200], details[:500], datetime.now(timezone.utc).isoformat()),
            )
    except sqlite3.Error as exc:
        log.error("DB insert failed: %s", exc)


def db_ok() -> bool:
    try:
        with _db_lock, _db_connect() as conn:
            conn.execute("SELECT 1").fetchone()
        return True
    except sqlite3.Error as exc:
        log.error("DB health check failed: %s", exc)
        return False


try:
    init_db()
except sqlite3.Error as exc:
    log.error("DB init failed: %s", exc)


# ==============================================================================
# Input validation
# ==============================================================================
CROP_ALIASES = {
    "gandum": "wheat", "gandam": "wheat", "makai": "maize", "corn": "maize",
    "chawal": "rice", "dhan": "rice", "kapas": "cotton", "ganna": "sugarcane",
    "tamatar": "tomato", "aloo": "potato", "alu": "potato",
    "گندم": "wheat", "مکئی": "maize", "چاول": "rice", "دھان": "rice", "کپاس": "cotton",
    "گنا": "sugarcane", "ٹماٹر": "tomato", "آلو": "potato",
}


@dataclass(frozen=True)
class Loc:
    latitude: float
    longitude: float
    crop: str
    region: str
    days: int
    kc: Optional[float]
    soil: str = "loam"


_CTRL = re.compile(r"[\x00-\x1f\x7f]")


def _num(payload, name, lo, hi, default=None):
    v = payload.get(name)
    if v is None:                       # missing or an explicit null: use the default
        v = default
    if v is None:
        return None
    if isinstance(v, bool) or not isinstance(v, (int, float, str)):
        raise ValueError(f"{name}: must be a number")
    try:
        f = float(v)
    except ValueError:
        raise ValueError(f"{name}: must be a number") from None
    if not math.isfinite(f):
        raise ValueError(f"{name}: NaN/Infinity not allowed")
    if not (lo <= f <= hi):
        raise ValueError(f"{name}: must be between {lo} and {hi}")
    return f


def _text(payload, name, default, maxlen):
    v = payload.get(name, default)
    if v is None:
        v = default
    if not isinstance(v, str):
        raise ValueError(f"{name}: must be text")
    v = _CTRL.sub("", v).strip()
    if len(v) > maxlen:
        raise ValueError(f"{name}: max {maxlen} characters")
    return v


def parse_location(payload) -> Loc:
    if not isinstance(payload, dict):
        raise ValueError("body: JSON object required")
    lat = _num(payload, "latitude", -90, 90)
    lon = _num(payload, "longitude", -180, 180)
    if lat is None or lon is None:
        raise ValueError("latitude and longitude are required")
    days = _num(payload, "days", 7, 90, 30)
    kc = _num(payload, "kc", 0.1, 1.5, None)
    return Loc(
        latitude=round(lat, 4), longitude=round(lon, 4),
        crop=_text(payload, "crop", "wheat", 50) or "wheat",
        region=_text(payload, "region", "", 100),
        days=int(days), kc=kc, soil=(_text(payload, "soil", "loam", 30).lower() or "loam"),
    )


# ==============================================================================
# NASA POWER client
# ==============================================================================
POWER_DAILY = "https://power.larc.nasa.gov/api/temporal/daily/point"
POWER_CLIM = "https://power.larc.nasa.gov/api/temporal/climatology/point"
CORE_PARAMS = ["T2M", "T2M_MAX", "T2M_MIN", "RH2M", "WS2M", "PS",
               "ALLSKY_SFC_SW_DWN", "PRECTOTCORR", "GWETTOP", "GWETROOT"]
UV_PARAMS = ["ALLSKY_SFC_UV_INDEX"]          # optional: failure degrades gracefully
CLIM_PARAMS = ["T2M", "PRECTOTCORR"]         # optional
MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
          "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
FILL_VALUE = -999.0
POWER_LAG_DAYS = 3        # newest days are often still -999; they are skipped, not averaged
MIN_COVERAGE = 0.5        # need >=50 % valid T2M days or we refuse to answer
CACHE_TTL = 6 * 3600


class PowerError(Exception):
    pass


_http = requests.Session()
_http.headers.update({"User-Agent": f"Space2Earth/{VERSION}"})
_http.mount("https://", HTTPAdapter(max_retries=Retry(
    total=3, backoff_factor=0.6,
    status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET",))))


def power_request(url: str, params: dict) -> dict:
    """Single network choke-point (mocked in tests)."""
    try:
        resp = _http.get(url, params=params, timeout=(5, 30))
        resp.raise_for_status()
        body = resp.json()
    except requests.RequestException as exc:
        log.error("NASA POWER request failed: %s", exc)
        raise PowerError("NASA POWER is unreachable or returned an error") from exc
    except ValueError as exc:
        raise PowerError("NASA POWER returned invalid JSON") from exc
    props = body.get("properties") if isinstance(body, dict) else None
    if not isinstance(props, dict) or not isinstance(props.get("parameter"), dict):
        log.error("Malformed POWER payload: %s", str(body)[:300])
        raise PowerError("NASA POWER returned no parameter data")
    return body


def _as_dict(x) -> dict:
    """Upstream series must be {date: value}; anything else (null, list, text) counts as 'no data'."""
    return x if isinstance(x, dict) else {}


def _clean(v) -> Optional[float]:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    if not math.isfinite(v) or v == FILL_VALUE:
        return None
    return float(v)


def analysis_window(days: int, today: Optional[date] = None):
    today = today or datetime.now(timezone.utc).date()
    end = today - timedelta(days=POWER_LAG_DAYS)
    return end - timedelta(days=days - 1), end


def fetch_daily(lat, lon, params, start: date, end: date):
    body = power_request(POWER_DAILY, {
        "parameters": ",".join(params), "community": "AG",
        "longitude": lon, "latitude": lat,
        "start": start.strftime("%Y%m%d"), "end": end.strftime("%Y%m%d"),
        "format": "JSON",
    })
    raw = body["properties"]["parameter"]
    dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    series = {p: [_clean(_as_dict(raw.get(p)).get(d.strftime("%Y%m%d"))) for d in dates]
              for p in params}
    return dates, series


def fetch_climatology(lat, lon, params=None):
    params = params or CLIM_PARAMS
    body = power_request(POWER_CLIM, {
        "parameters": ",".join(params), "community": "AG",
        "longitude": lon, "latitude": lat, "format": "JSON"})
    raw = body["properties"]["parameter"]
    return {p: [_clean(_as_dict(raw.get(p)).get(m)) for m in MONTHS] for p in params}


def get_power_bundle(lat: float, lon: float, days: int) -> dict:
    """All modules share ONE cached NASA fetch per (cell, window, day)."""
    start, end = analysis_window(days)
    key = f"power:{lat:.2f}:{lon:.2f}:{days}:{end:%Y%m%d}"
    hit = cache.get(key)
    if hit is not None:
        return hit
    # Single-flight: if 100 people ask for the same new place at once, NASA is called only once.
    with _flight_lock:
        lock = _flights.setdefault(key, threading.Lock())
    try:
        with lock:
            hit = cache.get(key)
            if hit is not None:
                return hit
            return _fetch_bundle(lat, lon, start, end, key)
    finally:
        with _flight_lock:
            _flights.pop(key, None)


_flight_lock = threading.Lock()
_flights: "dict[str, threading.Lock]" = {}


def cached_call(key: str, ttl: float, fn):
    """Cache + single-flight for any producer function."""
    hit = cache.get(key)
    if hit is not None:
        return hit
    with _flight_lock:
        lock = _flights.setdefault(key, threading.Lock())
    try:
        with lock:
            hit = cache.get(key)
            if hit is not None:
                return hit
            value = fn()
            cache.set(key, value, ttl)
            return value
    finally:
        with _flight_lock:
            _flights.pop(key, None)


def _fetch_bundle(lat, lon, start, end, key) -> dict:
    # The three NASA requests run in parallel, so a first look-up costs the slowest one, not their sum.
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_core = pool.submit(fetch_daily, lat, lon, CORE_PARAMS, start, end)
        f_uv = pool.submit(fetch_daily, lat, lon, UV_PARAMS, start, end)
        f_clim = pool.submit(fetch_climatology, lat, lon)
        dates, series = f_core.result()          # essential: PowerError propagates
        n = len(dates)
        try:
            _, uv = f_uv.result()
            series.update(uv)
        except PowerError:
            log.warning("UV index unavailable; continuing without it")
            series.update({p: [None] * n for p in UV_PARAMS})
        try:
            clim = f_clim.result()
        except PowerError:
            log.warning("Climatology unavailable; anomalies disabled")
            clim = None

    coverage = {p: round(100 * sum(v is not None for v in s) / n, 1) for p, s in series.items()}
    if coverage["T2M"] < MIN_COVERAGE * 100:
        raise PowerError("NASA POWER returned too little valid data for this location/period")
    bundle = {
        "dates": dates, "series": series, "climatology": clim, "coverage": coverage,
        "missing": [p for p, c in coverage.items() if c == 0.0],
        "start": start, "end": end,
    }
    cache.set(key, bundle, CACHE_TTL)
    return bundle


# ==============================================================================
# Scientific core (pure functions - unit-tested)
# ==============================================================================
def vals(xs):
    return [x for x in xs if x is not None]


def mean(xs):
    v = vals(xs)
    return sum(v) / len(v) if v else None


def rnd(x, nd=2):
    return None if x is None else round(x, nd)


def sat_vp(t_c: float) -> float:
    """Saturation vapour pressure (kPa), FAO-56 eq. 11."""
    return 0.6108 * math.exp(17.27 * t_c / (t_c + 237.3))


def extraterrestrial_radiation(lat_deg: float, doy: int) -> float:
    """Ra, MJ m-2 day-1 (FAO-56 eq. 21)."""
    phi = math.radians(lat_deg)
    dr = 1 + 0.033 * math.cos(2 * math.pi * doy / 365)
    delta = 0.409 * math.sin(2 * math.pi * doy / 365 - 1.39)
    x = max(-1.0, min(1.0, -math.tan(phi) * math.tan(delta)))
    ws = math.acos(x)
    return (24 * 60 / math.pi) * 0.0820 * dr * (
        ws * math.sin(phi) * math.sin(delta) + math.cos(phi) * math.cos(delta) * math.sin(ws))


def et0_penman_monteith(tmean, tmax, tmin, rh, u2, rs, ps, lat, doy) -> float:
    """FAO-56 reference evapotranspiration, mm/day (daily step, G = 0)."""
    rh = max(0.0, min(100.0, rh))
    u2 = max(0.5, u2)                                  # FAO-56 recommendation
    rs = max(0.0, rs)
    es = (sat_vp(tmax) + sat_vp(tmin)) / 2
    ea = rh / 100 * es                                 # FAO-56 eq. 19 (RHmean only)
    delta = 4098 * sat_vp(tmean) / (tmean + 237.3) ** 2
    gamma = 0.000665 * ps
    z = max(0.0, (293 - 293 * (ps / 101.3) ** (1 / 5.26)) / 0.0065)   # elevation from pressure
    ra = extraterrestrial_radiation(lat, doy)
    rso = (0.75 + 2e-5 * z) * ra
    rns = (1 - 0.23) * rs
    ratio = max(0.3, min(1.0, rs / rso)) if rso > 0 else 0.5
    rnl = 4.903e-9 * (((tmax + 273.16) ** 4 + (tmin + 273.16) ** 4) / 2) \
        * (0.34 - 0.14 * math.sqrt(max(ea, 0.0))) * (1.35 * ratio - 0.35)
    rn = rns - rnl
    et0 = (0.408 * delta * rn + gamma * 900 / (tmean + 273) * u2 * (es - ea)) \
        / (delta + gamma * (1 + 0.34 * u2))
    return max(0.0, et0)


def heat_index_c(t_c: float, rh: float) -> float:
    """NWS heat index (Rothfusz regression with NWS adjustments), in deg C."""
    tf = t_c * 9 / 5 + 32
    rh = max(0.0, min(100.0, rh))
    simple = 0.5 * (tf + 61.0 + (tf - 68.0) * 1.2 + rh * 0.094)
    if (simple + tf) / 2 < 80:
        hf = simple
    else:
        hf = (-42.379 + 2.04901523 * tf + 10.14333127 * rh - 0.22475541 * tf * rh
              - 0.00683783 * tf ** 2 - 0.05481717 * rh ** 2 + 0.00122874 * tf ** 2 * rh
              + 0.00085282 * tf * rh ** 2 - 0.00000199 * tf ** 2 * rh ** 2)
        if rh < 13 and 80 <= tf <= 112:
            hf -= ((13 - rh) / 4) * math.sqrt((17 - abs(tf - 95)) / 17)
        elif rh > 85 and 80 <= tf <= 87:
            hf += ((rh - 85) / 10) * ((87 - tf) / 5)
    return (hf - 32) * 5 / 9


def heat_category(hi_c: float) -> str:
    if hi_c >= 51.7:
        return "extreme_danger"
    if hi_c >= 39.4:
        return "danger"
    if hi_c >= 32.2:
        return "extreme_caution"
    if hi_c >= 26.7:
        return "caution"
    return "none"


def uv_category(uvi: Optional[float]) -> Optional[str]:
    if uvi is None:
        return None
    return ("low" if uvi < 3 else "moderate" if uvi < 6 else "high" if uvi < 8
            else "very_high" if uvi < 11 else "extreme")


def longest_run(flags) -> int:
    best = cur = 0
    for f in flags:
        cur = cur + 1 if f else 0
        best = max(best, cur)
    return best


# Indicative literature values: FAO-56 Table 12 (Kc mid-season), base temperatures
# and heat-damage thresholds commonly used in crop modelling. Treat as defaults.
CROPS = {
    "wheat":     {"label": "Wheat",     "kc_mid": 1.15, "tbase": 0.0,  "theat": 32.0},
    "maize":     {"label": "Maize",     "kc_mid": 1.20, "tbase": 10.0, "theat": 35.0},
    "rice":      {"label": "Rice",      "kc_mid": 1.20, "tbase": 10.0, "theat": 35.0},
    "cotton":    {"label": "Cotton",    "kc_mid": 1.15, "tbase": 15.5, "theat": 35.0},
    "sugarcane": {"label": "Sugarcane", "kc_mid": 1.25, "tbase": 12.0, "theat": 38.0},
    "tomato":    {"label": "Tomato",    "kc_mid": 1.15, "tbase": 10.0, "theat": 32.0},
    "potato":    {"label": "Potato",    "kc_mid": 1.15, "tbase": 7.0,  "theat": 30.0},
    "generic":   {"label": "Generic crop", "kc_mid": 1.00, "tbase": 10.0, "theat": 35.0},
}


CROP_UR = {"wheat": "گندم", "maize": "مکئی", "rice": "چاول", "cotton": "کپاس", "sugarcane": "گنا",
           "tomato": "ٹماٹر", "potato": "آلو", "generic": "فصل"}
LEVEL_UR = {"Low": "کم", "Moderate": "درمیانی", "High": "زیادہ", "Insufficient data": "ڈیٹا ناکافی"}
DISCLAIMER = {"en": " This is advisory information from NASA satellite/model data - please confirm in the field.",
              "ur": " یہ ناسا کے سیٹلائٹ/ماڈل ڈیٹا پر مبنی مشورہ ہے، براہِ کرم کھیت میں تصدیق کریں۔"}


def resolve_crop(name: str):
    key = CROP_ALIASES.get(name.strip().lower(), name.strip().lower())
    if key in CROPS:
        return key, CROPS[key], None
    return "generic", CROPS["generic"], (
        f"Crop '{name}' is not in the built-in table; generic parameters were used.")


def daily_et0(b: dict, lat: float):
    s, out = b["series"], []
    for i, d in enumerate(b["dates"]):
        t, tx, tn, rh, u, rs = (s[k][i] for k in
                                ("T2M", "T2M_MAX", "T2M_MIN", "RH2M", "WS2M", "ALLSKY_SFC_SW_DWN"))
        if None in (t, tx, tn, rh, u, rs):
            out.append(None)
            continue
        ps = s["PS"][i] if s["PS"][i] is not None else 101.3
        out.append(et0_penman_monteith(t, tx, tn, rh, u, rs, ps, lat, d.timetuple().tm_yday))
    return out


def anomaly(b: dict, param: str, observed):
    clim = b.get("climatology")
    if not clim or param not in clim:
        return None
    obs, norm = [], []
    for d, v in zip(b["dates"], observed):
        n = clim[param][d.month - 1]
        if v is not None and n is not None:
            obs.append(v)
            norm.append(n)
    if len(obs) < 5:
        return None
    o, n = sum(obs) / len(obs), sum(norm) / len(norm)
    return {"observed_mean": rnd(o), "normal_mean": rnd(n), "difference": rnd(o - n)}


# ==============================================================================
# Module assessments
# ==============================================================================
def assess_agri(b, loc: Loc):
    ckey, crop, warn = resolve_crop(loc.crop)
    warnings = [warn] if warn else []
    kc = loc.kc if loc.kc is not None else crop["kc_mid"]
    s = b["series"]
    et0 = daily_et0(b, loc.latitude)
    pairs = [(p, e) for p, e in zip(s["PRECTOTCORR"], et0) if p is not None and e is not None]
    p_tot = sum(p for p, _ in pairs)
    et0_tot = sum(e for _, e in pairs)
    etc_tot = et0_tot * kc
    ratio = p_tot / etc_tot if etc_tot > 0 else None

    root, top = s["GWETROOT"], s["GWETTOP"]
    recent = mean(root[-7:])
    if recent is None:
        recent = mean(root)
    prev = mean(root[:-7])
    trend = (recent - prev) if recent is not None and prev is not None else None
    et7 = mean(et0[-7:])
    use7 = et7 * kc * 7 if et7 is not None else None

    reasons, advisory = [], []
    if recent is None:
        level = "Insufficient data"
        reasons.append("Root-zone soil wetness (GWETROOT) is unavailable for this period.")
    else:
        low_soil, dry_soil = recent < 0.40, recent < 0.25
        short = ratio is not None and ratio < 0.4
        partial = ratio is not None and ratio < 0.6
        if dry_soil or (low_soil and short):
            level = "High"
        elif low_soil or (partial and recent < 0.50):
            level = "Moderate"
        else:
            level = "Low"
        reasons.append(f"Recent (7-day) root-zone wetness is {recent * 100:.0f}% of saturation.")
        if ratio is not None:
            reasons.append(f"Rainfall covered {min(ratio, 9.99) * 100:.0f}% of estimated crop water demand "
                           f"over the window.")
        if trend is not None and abs(trend) >= 0.03:
            reasons.append("Soil is " + ("drying" if trend < 0 else "wetting") +
                           f" ({trend * 100:+.0f} percentage points vs earlier in the window).")
        if level == "High":
            advisory.append("Prioritise irrigation or moisture conservation now; verify with a soil probe.")
        elif level == "Moderate":
            advisory.append("Plan irrigation in the coming days; check actual soil moisture in the field.")
        else:
            advisory.append("No water-stress signal in the satellite/reanalysis data.")
        if recent > 0.85:
            advisory.append("Profile is very wet: check drainage and waterlogging risk.")
        if use7 is not None and level in ("High", "Moderate"):
            advisory.append(f"If recent weather persists, crop water use is about {use7:.0f} mm over the "
                            f"next 7 days (net requirement before any rain; not an irrigation schedule).")
        advisory.append("Irrigate early morning or evening to cut evaporation losses; mulching reduces demand.")

    metrics = {
        "crop": crop["label"], "kc_used": kc,
        "root_zone_wetness_recent_pct": rnd(None if recent is None else recent * 100, 1),
        "root_zone_wetness_window_mean_pct": rnd(None if mean(root) is None else mean(root) * 100, 1),
        "root_zone_wetness_trend_pct_points": rnd(None if trend is None else trend * 100, 1),
        "surface_wetness_recent_pct": rnd(None if mean(top[-7:]) is None else mean(top[-7:]) * 100, 1),
        "precipitation_total_mm": rnd(p_tot, 1),
        "et0_total_mm": rnd(et0_tot, 1),
        "crop_water_demand_etc_mm": rnd(etc_tot, 1),
        "rain_to_demand_ratio": rnd(ratio),
        "water_balance_mm": rnd(p_tot - etc_tot, 1),
        "crop_water_use_next_7d_mm": rnd(use7, 0),
        "temperature_mean_c": rnd(mean(s["T2M"]), 1),
        "temperature_anomaly": anomaly(b, "T2M", s["T2M"]),
        "precipitation_anomaly_mm_per_day": anomaly(b, "PRECTOTCORR", s["PRECTOTCORR"]),
    }
    series = {"et0_mm": [rnd(x) for x in et0], "precip_mm": [rnd(x) for x in s["PRECTOTCORR"]],
              "root_wetness_pct": [rnd(None if x is None else x * 100, 1) for x in root]}
    method = ("ET0: FAO-56 Penman-Monteith from NASA POWER T2M/T2M_MAX/T2M_MIN/RH2M/WS2M/"
              "ALLSKY_SFC_SW_DWN/PS. ETc = Kc x ET0 (single Kc, mid-season unless 'kc' supplied).")
    limits = ["Kc is a single mid-season value; real Kc changes with growth stage.",
              "Rainfall is not 'effective rainfall'; runoff and deep percolation are ignored.",
              "Risk thresholds (25% / 40% wetness) are indicative rule-of-thumb values, not calibrated per soil."]
    return metrics, {"level": level, "reasons": reasons, "method": method}, advisory, series, limits, warnings


def assess_bio(b, loc: Loc):
    ckey, crop, warn = resolve_crop(loc.crop)
    warnings = [warn] if warn else []
    s = b["series"]
    tx, tn, uv = s["T2M_MAX"], s["T2M_MIN"], s["ALLSKY_SFC_UV_INDEX"]
    pairs = [(a, c) for a, c in zip(tx, tn) if a is not None and c is not None]
    gdd = sum(max(0.0, (a + c) / 2 - crop["tbase"]) for a, c in pairs)
    n_tx = len(vals(tx))
    heat_days = sum(1 for a in vals(tx) if a > crop["theat"])
    frost_days = sum(1 for c in vals(tn) if c <= 0)
    frac = heat_days / n_tx if n_tx else None

    reasons, advisory = [], []
    if frac is None:
        level = "Insufficient data"
        reasons.append("Daily maximum temperature (T2M_MAX) is unavailable.")
    else:
        level = "High" if frac >= 0.25 else "Moderate" if frac >= 0.10 else "Low"
        if frost_days >= 3:
            level = "High"
        elif frost_days >= 1 and level == "Low":
            level = "Moderate"
        reasons.append(f"{heat_days} of {n_tx} days exceeded {crop['theat']:.0f} C maximum "
                       f"({crop['label']} heat-damage threshold).")
        if frost_days:
            reasons.append(f"{frost_days} day(s) with minimum temperature at or below 0 C.")
        if heat_days:
            advisory.append("Heat: irrigate in cooler hours, avoid water stress at flowering/grain fill, "
                            "consider mulch/shade and adjusting sowing dates in future seasons.")
        if frost_days:
            advisory.append("Frost: protect seedlings/flowers (covers, light irrigation before cold nights).")
        if level == "Low":
            advisory.append("Temperatures were within a tolerable range for this crop.")
    uv_v = vals(uv)
    uv_max = max(uv_v) if uv_v else None
    if uv_v:
        advisory.append("UV index is reported for context; no universal crop-damage threshold exists, "
                        "so UV is not used in the stress level.")
    metrics = {
        "crop": crop["label"], "base_temperature_c": crop["tbase"], "heat_threshold_c": crop["theat"],
        "growing_degree_days": rnd(gdd, 0), "heat_stress_days": heat_days, "frost_days": frost_days,
        "heat_stress_fraction_pct": rnd(None if frac is None else frac * 100, 0),
        "tmax_peak_c": rnd(max(vals(tx)) if vals(tx) else None, 1),
        "tmin_lowest_c": rnd(min(vals(tn)) if vals(tn) else None, 1),
        "uv_index_mean": rnd(mean(uv)), "uv_index_max": rnd(uv_max),
        "uv_category_at_max": uv_category(uv_max),
        "temperature_anomaly": anomaly(b, "T2M", s["T2M"]),
    }
    series = {"tmax_c": [rnd(x, 1) for x in tx], "tmin_c": [rnd(x, 1) for x in tn],
              "uv_index": [rnd(x) for x in uv]}
    method = ("GDD = sum(max(0, (Tmax+Tmin)/2 - Tbase)). Stress level from share of days above the crop heat "
              "threshold (>=10% moderate, >=25% high) and frost days. UV index from NASA POWER, WHO categories.")
    limits = ["Thresholds are generic literature values; cultivar and growth stage matter greatly.",
              "No upper temperature cap is applied in the GDD sum.",
              "This module gives no pesticide/biostimulant recommendations - the data cannot support them."]
    return metrics, {"level": level, "reasons": reasons, "method": method}, advisory, series, limits, warnings


def assess_eco(b, loc: Loc):
    s = b["series"]
    his = []
    for tx, rh in zip(s["T2M_MAX"], s["RH2M"]):
        his.append(heat_index_c(tx, rh) if tx is not None and rh is not None else None)
    hv = vals(his)
    counts = {k: 0 for k in ("caution", "extreme_caution", "danger", "extreme_danger")}
    for h in hv:
        c = heat_category(h)
        if c != "none":
            counts[c] += 1
    n = len(hv)
    danger = counts["danger"] + counts["extreme_danger"]
    ext = danger + counts["extreme_caution"]
    precip = s["PRECTOTCORR"]
    pv = vals(precip)
    dry_spell = longest_run([(p is not None and p < 1.0) for p in precip])

    reasons, advisory = [], []
    if n == 0:
        level = "Insufficient data"
        reasons.append("Temperature/humidity data are unavailable.")
    else:
        level = "High" if danger >= 3 else "Moderate" if (danger >= 1 or ext / n >= 0.25) else "Low"
        reasons.append(f"{ext} of {n} days reached heat-index 'extreme caution' (>=32 C) or worse; "
                       f"{danger} reached 'danger' (>=39 C).")
        if dry_spell >= 7:
            reasons.append(f"Longest dry spell (<1 mm/day): {dry_spell} days.")
        if level in ("High", "Moderate"):
            advisory += ["Limit strenuous outdoor work to early morning/evening; ensure drinking water and shade.",
                         "Protect children, elderly and outdoor workers; open public cooling spaces.",
                         "Long-term: tree canopy, cool/reflective roofs and water-sensitive urban design."]
        else:
            advisory.append("No significant heat-stress signal in this window.")
    metrics = {
        "region": loc.region or None,
        "heat_index_max_c": rnd(max(hv) if hv else None, 1), "heat_index_mean_c": rnd(mean(hv), 1),
        "heat_index_days": counts, "tmax_peak_c": rnd(max(vals(s["T2M_MAX"])) if vals(s["T2M_MAX"]) else None, 1),
        "temperature_mean_c": rnd(mean(s["T2M"]), 1),
        "temperature_anomaly": anomaly(b, "T2M", s["T2M"]),
        "precipitation_total_mm": rnd(sum(pv), 1) if pv else None,
        "wet_days_ge_1mm": sum(1 for p in pv if p >= 1.0), "longest_dry_spell_days": dry_spell,
        "air_quality": "Not provided: AQI is not a NASA POWER parameter (use OpenAQ / CAMS for real AQI).",
    }
    series = {"heat_index_c": [rnd(x, 1) for x in his], "tmax_c": [rnd(x, 1) for x in s["T2M_MAX"]],
              "precip_mm": [rnd(x) for x in precip]}
    method = "NWS heat index (Rothfusz) from daily Tmax and daily-mean RH; NWS categories."
    limits = ["Daily-mean RH is used with Tmax; afternoon RH is usually lower, so heat index can be overestimated.",
              "~50 km grid cells cannot resolve urban heat-island hotspots."]
    return metrics, {"level": level, "reasons": reasons, "method": method}, advisory, series, limits, []


# ------------------------------------------------------------------------------
# LIVE layer (NOT NASA): Open-Meteo current conditions + 7-day forecast.
# NASA POWER has a 3-7 day latency and no forecast; this layer closes that gap and is always labelled.
# ------------------------------------------------------------------------------
OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
LIVE_TTL = 600  # current conditions update about every 15 minutes


class LiveError(PowerError):
    pass


def live_request(params: dict) -> dict:
    """Network choke-point for the live layer (mocked in tests)."""
    try:
        resp = _http.get(OPEN_METEO, params=params, timeout=(5, 15))
        resp.raise_for_status()
        body = resp.json()
    except requests.RequestException as exc:
        log.error("Open-Meteo request failed: %s", exc)
        raise LiveError("Live weather service is unreachable or returned an error") from exc
    except ValueError as exc:
        raise LiveError("Live weather service returned invalid JSON") from exc
    if not (isinstance(body, dict) and isinstance(body.get("daily"), dict) and isinstance(body.get("current"), dict)):
        raise LiveError("Live weather service returned no data")
    return body


# Open-Meteo soil layers (volumetric water content, m3/m3) and their thickness in cm.
SOIL_LAYERS = [("soil_moisture_0_to_1cm", 1), ("soil_moisture_1_to_3cm", 2), ("soil_moisture_3_to_9cm", 6),
               ("soil_moisture_9_to_27cm", 18), ("soil_moisture_27_to_81cm", 54)]
# (field capacity, wilting point) m3/m3 - mid-range typical values after FAO-56 Table 19; indicative only.
SOILS = {"sand": (0.12, 0.045), "sandy loam": (0.23, 0.11), "loam": (0.25, 0.12),
         "silt loam": (0.29, 0.15), "clay": (0.36, 0.22)}


def resolve_soil(name: str):
    k = (name or "loam").strip().lower()
    if k in SOILS:
        return k, None
    return "loam", f"Soil type '{name}' is not in the built-in table; loam was used."


def _wmean(pairs):
    pairs = [(v, w) for v, w in pairs if v is not None]
    return sum(v * w for v, w in pairs) / sum(w for _, w in pairs) if pairs else None


def fetch_live(lat: float, lon: float) -> dict:
    body = live_request({
        "latitude": lat, "longitude": lon, "forecast_days": 7, "past_days": 3, "timezone": "auto",
        "wind_speed_unit": "ms",
        "current": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,et0_fao_evapotranspiration,uv_index_max",
        "hourly": ",".join([k for k, _ in SOIL_LAYERS] + ["precipitation"]),
    })
    daily, cur = body["daily"], body["current"]
    cur_time = str(cur.get("time") or "")
    try:
        all_dates = [date.fromisoformat(t) for t in daily.get("time", [])]
    except (TypeError, ValueError) as exc:
        raise LiveError("Live weather service returned bad dates") from exc
    # past_days adds days before today; keep only today onward as the forecast window
    n_past = sum(1 for d in all_dates if cur_time[:10] and d.isoformat() < cur_time[:10])
    dates = all_dates[n_past:]
    if not dates:
        raise LiveError("Live weather service returned no forecast days")

    def arr(k):
        a = daily.get(k)
        if isinstance(a, list) and len(a) == len(all_dates):
            return [_clean(v) for v in a][n_past:]
        return [None] * len(dates)

    H = body.get("hourly") if isinstance(body.get("hourly"), dict) else {}
    ht = [str(x) for x in H["time"]] if isinstance(H.get("time"), list) else []

    def hv(k):
        a = H.get(k)
        return [_clean(v) for v in a] if isinstance(a, list) and len(a) == len(ht) else [None] * len(ht)
    layers = {k: hv(k) for k, _ in SOIL_LAYERS}
    rain_h = hv("precipitation")
    soil = {"root": None, "top": None, "root_3d_ago": None, "rain_last_72h": None, "rain_next_24h": None,
            "hourly": [], "rain_hourly": []}
    idx = None
    for i, t in enumerate(ht):
        if t <= cur_time:
            idx = i
        else:
            break
    if idx is not None:
        def at(i, ls):
            return _wmean([(layers[k][i], w) for k, w in ls]) if 0 <= i < len(ht) else None
        soil["root"], soil["top"] = at(idx, SOIL_LAYERS), at(idx, SOIL_LAYERS[:3])
        soil["root_3d_ago"] = at(idx - 72, SOIL_LAYERS)
        past = vals(rain_h[max(0, idx - 71):idx + 1])
        soil["rain_last_72h"] = sum(past) if len(past) >= 24 else None
        nxt = vals(rain_h[idx + 1:idx + 25])
        soil["rain_next_24h"] = sum(nxt) if nxt else None
        lo, hi = max(0, idx - 72), min(len(ht), idx + 49)
        soil["hourly"] = [at(i, SOIL_LAYERS) for i in range(lo, hi)]
        soil["rain_hourly"] = rain_h[lo:hi]
    return {"fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "current": {"time": cur_time, "temperature_c": _clean(cur.get("temperature_2m")),
                        "humidity_pct": _clean(cur.get("relative_humidity_2m")),
                        "wind_ms": _clean(cur.get("wind_speed_10m")),
                        "precipitation_mm": _clean(cur.get("precipitation"))},
            "dates": dates, "tmax": arr("temperature_2m_max"), "tmin": arr("temperature_2m_min"),
            "precip": arr("precipitation_sum"), "et0": arr("et0_fao_evapotranspiration"),
            "uv": arr("uv_index_max"), "soil": soil}


LIVE_DISCLAIMER = {
    "en": " Forecast data is from Open-Meteo (not NASA) and can change - please confirm in the field.",
    "ur": " یہ پیش گوئی Open-Meteo سے ہے (ناسا کا ڈیٹا نہیں) اور بدل سکتی ہے، براہِ کرم کھیت میں تصدیق کریں۔"}


def assess_live(lv: dict, loc: Loc):
    ckey, crop, warn = resolve_crop(loc.crop)
    kc = loc.kc if loc.kc is not None else crop["kc_mid"]
    cur, dates = lv["current"], lv["dates"]
    tx, tn, pr, et, uv = lv["tmax"], lv["tmin"], lv["precip"], lv["et0"], lv["uv"]
    t_now, rh_now = cur["temperature_c"], cur["humidity_pct"]
    hi_now = heat_index_c(t_now, rh_now) if t_now is not None and rh_now is not None else None
    rain7 = sum(vals(pr))
    etc7 = sum(e * kc for e in vals(et))
    net = max(0.0, etc7 - rain7)
    soil_key, soil_warn = resolve_soil(loc.soil)
    fc, wp = SOILS[soil_key]
    sl = lv.get("soil") or {}
    root, top, root3 = sl.get("root"), sl.get("top"), sl.get("root_3d_ago")
    aw = None if root is None else max(0.0, min(100.0, (root - wp) / (fc - wp) * 100))
    above_fc = root is not None and root > fc
    r72, r24 = sl.get("rain_last_72h"), sl.get("rain_next_24h")

    alerts = []  # (severity 1=moderate 2=high, English text, Urdu text)
    for d, a, b, c in zip(dates, tx, tn, pr):
        if b is not None and b <= 0:
            alerts.append((2 if b <= -2 else 1, f"Frost risk on {d}: minimum {b:.1f} C.",
                           f"{d} کو پالے کا خطرہ: کم از کم {b:.1f} ڈگری۔"))
        if a is not None and a > crop["theat"]:
            alerts.append((2 if a >= crop["theat"] + 5 else 1,
                           f"Heat on {d}: maximum {a:.1f} C (above {crop['theat']:.0f} C for {crop['label']}).",
                           f"{d} کو گرمی: زیادہ سے زیادہ {a:.1f} ڈگری ({CROP_UR.get(ckey, 'فصل')} کی حد {crop['theat']:.0f})۔"))
        if c is not None and c >= 25:
            alerts.append((2 if c >= 50 else 1, f"Heavy rain on {d}: {c:.0f} mm.", f"{d} کو تیز بارش: {c:.0f} ملی میٹر۔"))
    if rain7 < 5 and net >= 25:
        alerts.append((1, f"Dry week ahead: about {net:.0f} mm net crop water need with only {rain7:.0f} mm rain forecast - plan irrigation.",
                       f"خشک ہفتہ: بارش صرف {rain7:.0f} ملی میٹر اور فصل کو تقریباً {net:.0f} ملی میٹر پانی درکار - آبپاشی کا منصوبہ بنائیں۔"))

    if r24 is not None and r24 >= 50 and not any(a[0] == 2 and "eavy rain" in a[1] for a in alerts):
        alerts.append((2, f"Very heavy rain expected in the next 24 hours: about {r24:.0f} mm.",
                       f"اگلے 24 گھنٹوں میں بہت تیز بارش متوقع: تقریباً {r24:.0f} ملی میٹر۔"))
    pr3 = vals(pr[:3])
    rain3 = sum(pr3)
    if len(pr3) == 3 and rain3 >= 50 and not any("eavy rain" in a[1] for a in alerts):
        alerts.append((1, f"Heavy rain accumulation: about {rain3:.0f} mm over the next 3 days (no single day above 25 mm) - "
                          f"soil may saturate and low fields may waterlog.",
                       f"اگلے 3 دن میں مسلسل بارش تقریباً {rain3:.0f} ملی میٹر متوقع ہے (کوئی ایک دن 25 ملی میٹر سے زیادہ نہیں): "
                       f"مٹی بھر سکتی ہے اور نشیبی کھیتوں میں پانی کھڑا ہو سکتا ہے۔"))
    if aw is not None and aw < 30:
        alerts.append((2 if aw < 15 else 1,
                       f"Root-zone soil is dry: about {aw:.0f}% of plant-available water ({soil_key} soil assumed).",
                       f"جڑوں کے علاقے کی مٹی خشک ہے: دستیاب پانی تقریباً {aw:.0f}% ({soil_key} مٹی فرض کی گئی)۔"))
    if above_fc and any(c is not None and c >= 25 for c in pr):
        alerts.append((1, "Soil is already at/above field capacity and heavy rain is forecast: waterlogging and runoff risk.",
                       "مٹی پہلے ہی بھری ہوئی ہے اور تیز بارش متوقع ہے: پانی کھڑا ہونے اور بہاؤ کا خطرہ۔"))

    if not vals(tx) and not vals(tn):
        level, reasons = "Insufficient data", ["No valid forecast values were returned."]
    else:
        level = ["Low", "Moderate", "High"][max([a[0] for a in alerts], default=0)]
        reasons = [a[1] for a in alerts] or ["No frost, heat, heavy-rain or dry-spell alert in the next 7 days."]
    flood_flag = any(a[0] == 2 and "eavy rain" in a[1] for a in alerts)
    advisory = []
    if flood_flag:
        advisory.append("Flood safety: this is NOT an official warning - follow PMD, NDMA and PDMA KP alerts; keep "
                        "people, livestock and stored grain away from streams and low ground. Emergency: Rescue 1122.")
    if any("Frost" in a[1] for a in alerts):
        advisory.append("Frost: cover seedlings/flowers and irrigate lightly before cold nights.")
    if any("Heat on" in a[1] for a in alerts):
        advisory.append("Heat: irrigate in cooler hours and avoid field work at midday.")
    if any("eavy rain" in a[1] for a in alerts):
        advisory.append("Heavy rain: clear drains and postpone spraying and fertiliser.")
    if any("Dry week" in a[1] for a in alerts):
        advisory.append("Dry spell: schedule irrigation; mulching reduces water demand.")
    if any("Root-zone" in a[1] for a in alerts):
        advisory.append("Dry root zone: irrigate soon and confirm with a soil probe or the 'finger test'.")
    if any("waterlogging" in a[1] for a in alerts):
        advisory.append("Wet soil + heavy rain: delay irrigation and clear drainage channels.")
    advisory.append("Forecasts are model predictions: re-check every day, rainfall is the least certain.")

    metrics = {
        "crop": crop["label"], "kc_used": kc, "observation_time_local": cur["time"] or None,
        "temperature_now_c": rnd(t_now, 1), "humidity_now_pct": rnd(rh_now, 0), "wind_now_ms": rnd(cur["wind_ms"], 1),
        "precipitation_now_mm": rnd(cur["precipitation_mm"], 1), "heat_index_now_c": rnd(hi_now, 1),
        "rain_next_7d_mm": rnd(rain7, 1), "crop_water_demand_next_7d_mm": rnd(etc7, 1),
        "net_water_need_next_7d_mm": rnd(net, 1),
        "tmax_peak_next_7d_c": rnd(max(vals(tx)) if vals(tx) else None, 1),
        "tmin_lowest_next_7d_c": rnd(min(vals(tn)) if vals(tn) else None, 1),
        "uv_index_peak_next_7d": rnd(max(vals(uv)) if vals(uv) else None, 1),
        "soil_type_assumed": soil_key,
        "soil_moisture_root_0_81cm_m3m3": rnd(root, 3), "soil_moisture_top_0_9cm_m3m3": rnd(top, 3),
        "root_zone_available_water_pct": rnd(aw, 0), "soil_above_field_capacity": above_fc if root is not None else None,
        "soil_moisture_change_3d_m3m3": rnd(None if root is None or root3 is None else root - root3, 3),
        "rain_last_72h_mm": rnd(r72, 1), "rain_next_24h_mm": rnd(r24, 1),
    }
    series = {"tmax_c": [rnd(x, 1) for x in tx], "tmin_c": [rnd(x, 1) for x in tn],
              "precip_mm": [rnd(x, 1) for x in pr], "et0_mm": [rnd(x) for x in et], "uv_index": [rnd(x, 1) for x in uv],
              "soil_moisture_hourly_m3m3": [rnd(x, 3) for x in sl.get("hourly", [])],
              "rain_hourly_mm": [rnd(x, 1) for x in sl.get("rain_hourly", [])]}
    method = ("Open-Meteo forecast API (weather-service model blend); ET0 as provided (FAO-56 Penman-Monteith). "
              "ETc = Kc x ET0. Soil moisture: Open-Meteo land-surface model (hourly volumetric water content), "
              "depth-weighted over 0-81 cm; plant-available water = (theta - WP)/(FC - WP) with typical FAO-56 "
              "values for the assumed soil type. Hourly series cover 72 h back to 48 h ahead. Alerts: frost Tmin<=0 C; heat Tmax above crop threshold; heavy rain >=25 mm/day; "
              "dry week = <5 mm rain and >=25 mm net need.")
    limits = ["Forecasts are model predictions, not observations; accuracy falls with lead time.",
              "NOT NASA data: Open-Meteo blends national weather-service models. Cite it and check its terms of use "
              "(its free tier is for non-commercial use).",
              "Soil moisture here is model output (not satellite or sensor data) and the soil type is assumed: "
              "calibrate with a field probe.",
              "Alert thresholds are generic rules of thumb."]
    warnings = ([warn] if warn else []) + ([soil_warn] if soil_warn else []) + ["This module uses a non-NASA live source; the other modules use NASA POWER."]

    def f0(x):
        return "n/a" if x is None else f"{x:.0f}"
    first = alerts[0] if alerts else None
    en = (f"Right now: {f0(t_now)} C, humidity {f0(rh_now)}%. Next 7 days: about {f0(rain7)} mm rain, peak {f0(metrics['tmax_peak_next_7d_c'])} C, "
          f"lowest {f0(metrics['tmin_lowest_next_7d_c'])} C. Risk level: {level}.")
    ur = (f"اس وقت: {f0(t_now)} ڈگری، نمی {f0(rh_now)}%۔ اگلے 7 دن: تقریباً {f0(rain7)} ملی میٹر بارش، زیادہ سے زیادہ "
          f"{f0(metrics['tmax_peak_next_7d_c'])} ڈگری، کم از کم {f0(metrics['tmin_lowest_next_7d_c'])} ڈگری۔ خطرے کی سطح: {LEVEL_UR.get(level, level)}۔")
    if aw is not None:
        en += f" Root-zone soil holds about {aw:.0f}% of plant-available water ({soil_key} soil assumed)."
        ur += f" جڑوں کے علاقے کی مٹی میں پودے کے لیے دستیاب پانی تقریباً {aw:.0f}% ہے ({soil_key} مٹی فرض کی گئی)۔"
    if r72 is not None and r24 is not None:
        en += f" Rain: {r72:.0f} mm in the last 3 days, {r24:.0f} mm expected in the next 24 hours."
        ur += f" بارش: پچھلے 3 دن میں {r72:.0f} ملی میٹر، اگلے 24 گھنٹوں میں متوقع {r24:.0f} ملی میٹر۔"
    if first:
        en += " " + first[1]
        ur += " " + first[2]
    if flood_flag:
        en += " This is not an official flood warning - follow PMD, NDMA and PDMA KP alerts."
        ur += " یہ سرکاری سیلاب وارننگ نہیں ہے - پی ایم ڈی، این ڈی ایم اے اور پی ڈی ایم اے کے پی کے انتباہات پر عمل کریں۔"
    if net > 0:
        en += f" Crops may need about {net:.0f} mm more water than rain provides."
        ur += f" فصل کو بارش کے علاوہ تقریباً {net:.0f} ملی میٹر مزید پانی درکار ہو سکتا ہے۔"
    summary = {"en": en + LIVE_DISCLAIMER["en"], "ur": ur + LIVE_DISCLAIMER["ur"]}
    return metrics, {"level": level, "reasons": reasons, "method": method}, advisory, series, limits, warnings, summary


def live_envelope(loc: Loc, lv: dict, parts) -> dict:
    metrics, assessment, advisory, series, limits, warnings, summary = parts
    n = len(lv["dates"])
    cov = {k: round(100 * sum(v is not None for v in lv[k]) / n, 1) for k in ("tmax", "tmin", "precip", "et0", "uv")}
    cov["soil_moisture"] = 100.0 if (lv.get("soil") or {}).get("root") is not None else 0.0
    cov["hourly_rain"] = 100.0 if (lv.get("soil") or {}).get("rain_last_72h") is not None else 0.0
    obs_day = (lv["current"]["time"] or "")[:10] or None
    return {
        "status": "success", "module": "live", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "location": {"latitude": loc.latitude, "longitude": loc.longitude, "label": loc.region or None},
        "window": {"start": lv["dates"][0].isoformat(), "end": lv["dates"][-1].isoformat(), "days": n,
                   "last_valid_date": obs_day, "data_age_days": 0, "mode": "live + forecast"},
        "data_quality": {"coverage_pct": cov, "missing_parameters": [k for k, c in cov.items() if c == 0.0],
                         "climatology_available": False},
        "summary": summary, "data_categories": {"forecast": CAT_FORECAST}, "independence": INDEPENDENCE,
        "metrics": metrics, "assessment": assessment, "advisory": advisory,
        "series": {"dates": [d.isoformat() for d in lv["dates"]], **series},
        "provenance": {"source": "Open-Meteo forecast API (NOT NASA)", "baseline": None,
                       "references": ["Open-Meteo.com (data CC BY 4.0) - verify terms of use",
                                      "FAO Irrigation & Drainage Paper 56 (Allen et al., 1998)"]},
        "limitations": limits, "warnings": warnings,
    }


# ------------------------------------------------------------------------------
# Place search: built-in major places (works offline) + Open-Meteo geocoding (any village, online)
# Coordinates are approximate (~0.01 deg) - fine for ~50 km data grids.
# ------------------------------------------------------------------------------
PLACES = {
    "Peshawar": (34.0151, 71.5249), "Islamabad": (33.6844, 73.0479), "Rawalpindi": (33.5651, 73.0169),
    "Lahore": (31.5497, 74.3436), "Karachi": (24.8607, 67.0011), "Quetta": (30.1798, 66.9750),
    "Multan": (30.1575, 71.4724), "Faisalabad": (31.4504, 73.1350), "Sialkot": (32.4945, 74.5229),
    "Gujranwala": (32.1877, 74.1945), "Sargodha": (32.0836, 72.6711), "Bahawalpur": (29.3956, 71.6836),
    "Sahiwal": (30.6682, 73.1114), "Mardan": (34.1989, 72.0231), "Swat (Mingora)": (34.7717, 72.3600),
    "Dera Ismail Khan": (31.8626, 70.9019), "Sukkur": (27.7052, 68.8574), "Hyderabad (Sindh)": (25.3960, 68.3578),
    "Gilgit": (35.9208, 74.3080), "Muzaffarabad": (34.3700, 73.4711), "Abbottabad": (34.1688, 73.2215),
    "Kohat": (33.5869, 71.4414), "Bannu": (32.9889, 70.6056), "Nowshera": (34.0153, 71.9747),
    "Charsadda": (34.1453, 71.7308), "Swabi": (34.1200, 72.4700), "Chitral": (35.8518, 71.7864),
    "Mithi (Thar)": (24.7371, 69.7978), "Gwadar": (25.1264, 62.3225), "Turbat": (26.0012, 63.0544),
}
PLACES_JSON = json.dumps({k: [v[0], v[1]] for k, v in PLACES.items()})
GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"


def search_places(q: str, limit: int = 8) -> list:
    ql = q.lower()
    hits = [{"name": n, "label": n + ", Pakistan", "latitude": lat, "longitude": lon, "source": "built-in"}
            for n, (lat, lon) in PLACES.items() if ql in n.lower()]
    return hits[:limit]


def geocode_request(params: dict) -> dict:
    """Network choke-point for place search (mocked in tests)."""
    try:
        resp = _http.get(GEOCODE_URL, params=params, timeout=(5, 10))
        resp.raise_for_status()
        body = resp.json()
    except requests.RequestException as exc:
        log.error("Geocoding request failed: %s", exc)
        raise LiveError("Place search service is unreachable") from exc
    except ValueError as exc:
        raise LiveError("Place search service returned invalid JSON") from exc
    if not isinstance(body, dict):
        raise LiveError("Place search service returned no data")
    return body


def geocode_online(q: str) -> list:
    body = geocode_request({"name": q, "count": 5, "language": "en", "format": "json", "countryCode": "PK"})
    out = []
    for r in body.get("results") or []:
        lat, lon = _clean(r.get("latitude")), _clean(r.get("longitude"))
        if lat is None or lon is None or not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue
        name = str(r.get("name") or q)
        label = ", ".join(str(x) for x in (name, r.get("admin1"), r.get("country")) if x)
        out.append({"name": name, "label": label, "latitude": round(lat, 4), "longitude": round(lon, 4),
                    "source": "open-meteo geocoding"})
    return out


# ------------------------------------------------------------------------------
# ROTATION EXPLORER (NASA Space Apps 2026 "Field Shift"): compares >=3 rotation strategies for a place.
# Evidence: NASA POWER monthly climatology (rain, temperature, ET0 by FAO-56) + the farmer's soil type, water
# availability and priorities. Transparent rule-based scoring - no black box, no market data, no invented numbers.
# ------------------------------------------------------------------------------
ROT_CLIM_PARAMS = ["T2M", "T2M_MAX", "T2M_MIN", "RH2M", "WS2M", "PS", "ALLSKY_SFC_SW_DWN", "PRECTOTCORR", "GWETROOT"]
MONTH_DAYS = [31, 28.25, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
ROT_WATER = ("rainfed", "limited", "good")
ROT_DEFAULT_WEIGHTS = {"water": 2.0, "soil": 2.0, "erosion": 2.0, "forage": 1.0, "income": 1.0, "resilience": 2.0, "pest": 1.0}
CRIT_LABEL = {"water": "Water saving", "soil": "Soil health", "erosion": "Erosion protection", "forage": "Fodder value",
              "income": "Market income (neutral unless you supply prices)", "resilience": "Heat/frost resilience",
              "pest": "Pest-cycle break"}
# Indicative generic traits (0-3 scales) for subtropical rabi (winter) / kharif (summer) cropping - REPLACE with local
# expert values. kc = FAO-56 mid-season Kc, theat = heat-damage threshold (C), months = growing months (northern
# hemisphere; shifted by 6 for latitudes below 0).
ROT_CROPS = {
    "wheat":    {"label": "Wheat", "season": "rabi", "months": [11, 12, 1, 2, 3, 4], "kc": 1.15, "theat": 32.0, "frost_tol": True,
                 "fam": "cereal", "n_fix": 0, "cover": 2.5, "residue": 3.0, "forage": 1.0, "root_m": 1.0},
    "mustard":  {"label": "Mustard/canola", "season": "rabi", "months": [10, 11, 12, 1, 2, 3], "kc": 1.0, "theat": 30.0,
                 "frost_tol": True, "fam": "brassica", "n_fix": 0, "cover": 2.0, "residue": 2.0, "forage": 0.5, "root_m": 1.0},
    "chickpea": {"label": "Chickpea", "season": "rabi", "months": [11, 12, 1, 2, 3], "kc": 1.0, "theat": 33.0,
                 "frost_tol": False, "fam": "legume", "n_fix": 1, "cover": 1.5, "residue": 2.0, "forage": 0.5, "root_m": 1.0},
    "berseem":  {"label": "Berseem clover (fodder)", "season": "rabi", "months": [11, 12, 1, 2, 3, 4], "kc": 1.1,
                 "theat": 35.0, "frost_tol": True, "fam": "legume", "n_fix": 1, "cover": 3.0, "residue": 2.5, "forage": 3.0,
                 "root_m": 0.8},
    "maize":    {"label": "Maize", "season": "kharif", "months": [6, 7, 8, 9], "kc": 1.2, "theat": 35.0, "frost_tol": False,
                 "fam": "cereal", "n_fix": 0, "cover": 2.0, "residue": 2.5, "forage": 1.5, "root_m": 1.0},
    "rice":     {"label": "Rice", "season": "kharif", "months": [6, 7, 8, 9, 10], "kc": 1.2, "theat": 35.0, "frost_tol": False,
                 "fam": "cereal", "n_fix": 0, "cover": 2.5, "residue": 2.5, "forage": 0.5, "root_m": 0.5, "perc": 90.0},
    "mungbean": {"label": "Mung bean", "season": "kharif", "months": [6, 7, 8], "kc": 1.05, "theat": 38.0, "frost_tol": False,
                 "fam": "legume", "n_fix": 1, "cover": 1.5, "residue": 1.5, "forage": 0.5, "root_m": 0.7},
    "sorghum":  {"label": "Forage sorghum", "season": "kharif", "months": [6, 7, 8, 9], "kc": 1.0, "theat": 38.0,
                 "frost_tol": False, "fam": "cereal", "n_fix": 0, "cover": 2.5, "residue": 2.0, "forage": 3.0, "root_m": 1.2},
    "fallow":   {"label": "Fallow (bare soil)", "season": None, "months": [], "kc": 0.0, "theat": 99.0, "frost_tol": True,
                 "fam": None, "n_fix": 0, "cover": 0.0, "residue": 0.0, "forage": 0.0, "root_m": 0.0},
}
ROT_LABEL_UR = {"wheat": "گندم", "mustard": "سرسوں", "chickpea": "چنا", "berseem": "برسیم", "maize": "مکئی", "rice": "چاول",
                "mungbean": "مونگ", "sorghum": "جوار (چارہ)", "fallow": "خالی زمین"}
CROP_ALIASES.update({"chana": "chickpea", "چنا": "chickpea", "sarson": "mustard", "سرسوں": "mustard", "برسیم": "berseem",
                     "moong": "mungbean", "مونگ": "mungbean", "jowar": "sorghum", "جوار": "sorghum",
                     "خالی زمین": "fallow", "fallow": "fallow"})


def parse_rotation(payload):
    loc = parse_location(payload)
    weights = dict(ROT_DEFAULT_WEIGHTS)
    pr = payload.get("priorities")
    if pr is not None:
        if not isinstance(pr, dict):
            raise ValueError("priorities: object required")
        for k, v in pr.items():
            if k not in weights:
                raise ValueError(f"priorities: unknown key '{k}'")
            f = _num({k: v}, k, 0, 3, None)
            if f is not None:
                weights[k] = f
    water = payload.get("water", "limited")
    if water not in ROT_WATER:
        raise ValueError("water: use rainfed, limited or good")
    cur = {}
    for key, default, season in (("current_rabi", "wheat", "rabi"), ("current_kharif", "fallow", "kharif")):
        val = _text(payload, key, default, 30).lower() or default
        val = CROP_ALIASES.get(val, val)
        if val not in ROT_CROPS or ROT_CROPS[val]["season"] not in (season, None):
            raise ValueError(f"{key}: choose one of " + ", ".join(k for k, c in ROT_CROPS.items() if c["season"] in (season, None)))
        cur[season] = val
    market = {}
    mk = payload.get("market")
    if mk is not None:
        if not isinstance(mk, dict):
            raise ValueError("market: object required")
        for k, v in mk.items():
            if k not in ROT_CROPS or k == "fallow":
                raise ValueError(f"market: unknown crop '{k}'")
            f = _num({k: v}, k, 0, 5, None)
            if f is not None:
                market[k] = f
    rain_pct = _num(payload, "rain_change_pct", -50, 50, 0) or 0.0
    dT = _num(payload, "temp_change_c", 0, 6, 0) or 0.0
    return loc, weights, water, cur["rabi"], cur["kharif"], market, (rain_pct, dT)


def rot_months(key: str, lat: float) -> list:
    shift = 6 if lat < 0 else 0
    return [((m - 1 + shift) % 12) + 1 for m in ROT_CROPS[key]["months"]]


def rot_climate(clim: dict, lat: float) -> dict:
    """Monthly climate from NASA POWER climatology; ET0 by FAO-56 Penman-Monteith on monthly means (mm/month)."""
    et0 = []
    for m in range(12):
        g = {k: clim[k][m] for k in ROT_CLIM_PARAMS}
        if any(g[k] is None for k in ("T2M", "T2M_MAX", "T2M_MIN", "RH2M", "WS2M", "ALLSKY_SFC_SW_DWN", "PRECTOTCORR")):
            raise PowerError("NASA POWER climatology is incomplete for this location")
        ps = g["PS"] if g["PS"] is not None else 101.3
        doy = date(2001, m + 1, 15).timetuple().tm_yday
        et0.append(et0_penman_monteith(g["T2M"], g["T2M_MAX"], g["T2M_MIN"], g["RH2M"], g["WS2M"],
                                       g["ALLSKY_SFC_SW_DWN"], ps, lat, doy) * MONTH_DAYS[m])
    return {"tmax": clim["T2M_MAX"], "tmin": clim["T2M_MIN"], "tmean": clim["T2M"], "et0": et0,
            "rain": [p * d for p, d in zip(clim["PRECTOTCORR"], MONTH_DAYS)], "gwetroot": clim["GWETROOT"]}


def crop_eval(key: str, cl: dict, lat: float, soil_key: str, water: str) -> dict:
    c = ROT_CROPS[key]
    ms = rot_months(key, lat)
    if not ms:
        return {"key": key, "months": [], "etc": 0.0, "eff_rain": 0.0, "store": 0.0, "need": 0.0,
                "heat_months": [], "frost_months": [], "limited": False}
    fc, wp = SOILS[soil_key]
    store = 0.5 * (fc - wp) * 1000 * c["root_m"]          # assume half of the soil's plant-available water at sowing
    etc = sum(c["kc"] * cl["et0"][m - 1] + c.get("perc", 0.0) for m in ms)
    eff = sum(0.8 * cl["rain"][m - 1] for m in ms)         # assume 80 % of rain is effective
    need = max(0.0, etc - eff - store)
    heat = [m for m in ms if cl["tmax"][m - 1] > c["theat"]]
    frost = [] if c["frost_tol"] else [m for m in ms if cl["tmin"][m - 1] <= 2.0]
    return {"key": key, "months": ms, "etc": etc, "eff_rain": eff, "store": store, "need": need,
            "heat_months": heat, "frost_months": frost, "limited": water == "rainfed" and etc > 0 and need > 0.4 * etc}


def _build_strategy(r: str, k: str, ev, cl: dict, market: dict) -> dict:
    es = [ev(r), ev(k)]
    cs = [ROT_CROPS[r], ROT_CROPS[k]]
    covered = {m for e in es for m in e["months"]}
    bare = [m for m in range(1, 13) if m not in covered]
    fams = [cs[0]["fam"], cs[1]["fam"]]
    same = sum(1 for a, b in ((fams[0], fams[1]), (fams[1], fams[0])) if a is not None and a == b)
    nmonths = sum(len(e["months"]) for e in es)
    bad = sum(len(e["heat_months"]) + len(e["frost_months"]) for e in es)
    income = sum((market.get(x, 3.0) if ROT_CROPS[x]["months"] else 0.0) for x in (r, k)) / 2
    residue, cover = sum(c["residue"] for c in cs) / 2, sum(c["cover"] for c in cs) / 2
    n_fix = any(c["n_fix"] for c in cs)
    return {"id": f"{r}+{k}", "rabi": r, "kharif": k, "evals": es, "bare": bare,
            "bare_rain": sum(cl["rain"][m - 1] for m in bare), "need": sum(e["need"] for e in es),
            "exp_frac": bad / max(1, nmonths), "residue": residue, "cover": cover,
            "forage": sum(c["forage"] for c in cs) / 2, "n_fix": n_fix, "income": income, "pest": 1 - same / 2,
            "soil_raw": 0.4 * residue / 3 + 0.4 * (1 if n_fix else 0) + 0.2 * cover / 3,
            "feasible": not (r == "fallow" and k == "fallow") and all(
                len(e["heat_months"]) <= 1 and not e["frost_months"] and not e["limited"] for e in es)}


def _score_criteria(c: dict, rng: dict) -> None:
    def inv(key):
        lo, hi = rng[key]
        return 1.0 if hi == lo else max(0.0, min(1.0, 1 - (c[key] - lo) / (hi - lo)))
    c["s"] = {"water": inv("need"), "soil": c["soil_raw"], "erosion": 0.7 * inv("bare_rain") + 0.3 * c["cover"] / 3,
              "forage": c["forage"] / 3, "income": c["income"] / 5, "resilience": inv("exp_frac"), "pest": c["pest"]}


def _total(c: dict, w: dict) -> float:
    sw = sum(w.values())
    if sw <= 0:
        return 100 * sum(c["s"].values()) / len(c["s"])
    return 100 * sum(w[k] * c["s"][k] for k in w) / sw


def plan_rotations(cl, lat, soil_key, water, weights, market, cur_r, cur_k) -> dict:
    cache_ev = {}

    def ev(key):
        if key not in cache_ev:
            cache_ev[key] = crop_eval(key, cl, lat, soil_key, water)
        return cache_ev[key]
    rabi = [k for k, c in ROT_CROPS.items() if c["season"] == "rabi"] + ["fallow"]
    kharif = [k for k, c in ROT_CROPS.items() if c["season"] == "kharif"] + ["fallow"]
    cands = [_build_strategy(r, k, ev, cl, market) for r in rabi for k in kharif if not (r == "fallow" and k == "fallow")]
    existing = next((c for c in cands if c["id"] == f"{cur_r}+{cur_k}"), None) or _build_strategy(cur_r, cur_k, ev, cl, market)
    pool = [c for c in cands if c["feasible"] or c is existing]
    if existing not in pool:
        pool.append(existing)
    rng = {k: (min(c[k] for c in pool), max(c[k] for c in pool)) for k in ("need", "bare_rain", "exp_frac")}
    for c in cands + [existing]:
        _score_criteria(c, rng)
        c["score"] = _total(c, weights)
    pool.sort(key=lambda c: (-c["score"], c["id"]))
    alts = [c for c in pool if c is not existing][:2]
    if len(alts) < 2:                                     # climate/water too restrictive: still show 3 options, flagged
        rest = sorted([c for c in cands if c not in pool and c is not existing], key=lambda c: (-c["score"], c["id"]))
        alts += rest[:2 - len(alts)]
    variants = same = 0
    base_top = pool[0]["id"]
    for key in weights:
        for d in (1, -1):
            w2 = dict(weights)
            w2[key] = max(0.0, min(5.0, w2[key] + d))
            if w2 == weights:
                continue
            variants += 1
            top = min(pool, key=lambda c: (-_total(c, w2), c["id"]))
            same += top["id"] == base_top
    shown = sorted([existing] + alts, key=lambda c: (-c["score"], c["id"]))
    return {"existing": existing, "alts": alts, "shown": shown, "pool": pool,
            "stability_pct": round(100 * same / variants, 0) if variants else 100.0, "top_id": base_top}


def _describe(c: dict, existing: dict, cl: dict, weights: dict, water: str) -> dict:
    labels = [ROT_CROPS[c["rabi"]]["label"], ROT_CROPS[c["kharif"]]["label"]]
    contrib = sorted(((weights[k] * c["s"][k], k) for k in weights if weights[k] > 0), reverse=True)
    why = ["This is your current rotation and is the baseline for comparison."] if c is existing else []
    why += [f"{CRIT_LABEL[k]} scores {100 * c['s'][k]:.0f}/100 and you rated it {weights[k]:.0f}/3." for _, k in contrib[:2]]
    evidence, benefits, risks = [], [], []
    for e in c["evals"]:
        if not e["months"]:
            continue
        cr = ROT_CROPS[e["key"]]
        span = f"{MONTH_ABBR[e['months'][0] - 1]}-{MONTH_ABBR[e['months'][-1] - 1]}"
        evidence.append(f"{cr['label']} ({span}): crop water use about {e['etc']:.0f} mm, effective rain {e['eff_rain']:.0f} mm, "
                        f"soil-water allowance {e['store']:.0f} mm -> net irrigation need about {e['need']:.0f} mm.")
        if e["heat_months"]:
            risks.append(f"{cr['label']}: mean daily maximum is above {cr['theat']:.0f} C in "
                         f"{', '.join(MONTH_ABBR[m - 1] for m in e['heat_months'])} (heat stress risk).")
        if e["frost_months"]:
            risks.append(f"{cr['label']} is frost-sensitive and mean daily minimum is 2 C or lower in "
                         f"{', '.join(MONTH_ABBR[m - 1] for m in e['frost_months'])}.")
        if e["limited"]:
            risks.append(f"{cr['label']}: with rain only, water falls short of crop demand (needs irrigation).")
    bare_names = ", ".join(MONTH_ABBR[m - 1] for m in c["bare"]) or "none"
    evidence.append(f"Bare-field months: {bare_names}; about {c['bare_rain']:.0f} mm of rain falls on uncovered soil.")
    if c["bare"] and c["bare_rain"] >= 100:
        risks.append(f"Bare soil in {bare_names} meets about {c['bare_rain']:.0f} mm of rain: erosion and nutrient-loss risk.")
    if c["n_fix"]:
        benefits.append("Includes a legume: biological nitrogen fixation (amount not quantified here).")
    if c["forage"] >= 2:
        benefits.append("Provides good fodder value for livestock.")
    if len(c["bare"]) <= 2:
        benefits.append("Keeps the soil covered for most of the year.")
    if c is not existing and c["need"] < existing["need"] - 10:
        benefits.append(f"Lower irrigation need than your current rotation by about {existing['need'] - c['need']:.0f} mm/year.")
    if c["pest"] >= 1:
        benefits.append("Alternates plant families, which can help interrupt pest and disease cycles.")
    else:
        risks.append("Same plant family in consecutive seasons: pest and disease carry-over is more likely.")
    if not c["feasible"] and c is not existing:
        risks.append("This option does not pass the climate/water suitability checks above; shown only to complete the comparison.")
    gaps = ["Market prices and demand are not in NASA data (the income score is neutral unless you supply prices).",
            "Soil tests (texture, organic matter, salinity, nutrients) and field history are not included.",
            "Monthly climate means hide year-to-year variability and extreme events.",
            "Crop traits are indicative generic values: confirm with local agronomists and extension officers.",
            "SMAP, ECOSTRESS, GPM, Landsat/MODIS and GRACE data are not used in this version; they could refine soil moisture, "
            "canopy stress, extreme rain, vegetation timing and groundwater."]
    return {"id": c["id"], "role": "existing" if c is existing else "alternative", "name": " -> ".join(labels),
            "score": round(c["score"], 1), "criteria": {k: round(100 * v) for k, v in c["s"].items()},
            "rotation": [{"season": s, "crop": k, "label": ROT_CROPS[k]["label"],
                          "months": [MONTH_ABBR[m - 1] for m in ev["months"]]}
                         for s, k, ev in (("rabi (winter)", c["rabi"], c["evals"][0]), ("kharif (summer)", c["kharif"], c["evals"][1]))],
            "why": why, "evidence": evidence, "benefits": benefits or ["No clear benefit identified from the data used."],
            "risks": risks or ["No major climate risk flagged from monthly means; local review still needed."],
            "information_gaps": gaps,
            "metrics": {"irrigation_need_mm_per_year": round(c["need"]), "bare_months": [MONTH_ABBR[m - 1] for m in c["bare"]],
                        "rain_on_bare_soil_mm": round(c["bare_rain"]), "heat_frost_exposure_pct": round(100 * c["exp_frac"])}}


def scenario_clim(clim: dict, rain_pct: float, dT: float) -> dict:
    """What-if: scale rainfall and shift temperatures of the NASA climatology (humidity is NOT adjusted)."""
    if not rain_pct and not dT:
        return clim
    out = {k: list(v) for k, v in clim.items()}
    out["PRECTOTCORR"] = [None if x is None else x * (1 + rain_pct / 100) for x in clim["PRECTOTCORR"]]
    for k in ("T2M", "T2M_MAX", "T2M_MIN"):
        out[k] = [None if x is None else x + dT for x in clim[k]]
    return out


POWER_MONTHLY = "https://power.larc.nasa.gov/api/temporal/monthly/point"


def _ols(xs, ys):
    """Least-squares slope and its t-statistic."""
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx
    icpt = my - slope * mx
    sse = sum((y - (icpt + slope * x)) ** 2 for x, y in zip(xs, ys))
    se = math.sqrt(sse / (n - 2) / sxx) if n > 2 else float("inf")
    t = slope / se if se > 0 else (0.0 if slope == 0 else float("inf"))
    return slope, t


def fetch_trend(lat, lon):
    """25 years of NASA POWER monthly T2M and rain -> change per decade (None if too little data)."""
    y1 = datetime.now(timezone.utc).year - 1
    y0 = y1 - 24
    body = power_request(POWER_MONTHLY, {"parameters": "T2M,PRECTOTCORR", "community": "AG", "longitude": lon,
                                         "latitude": lat, "start": y0, "end": y1, "format": "JSON"})
    raw = body["properties"]["parameter"]
    years, temps, rains = [], [], []
    for y in range(y0, y1 + 1):
        t = [_clean(_as_dict(raw.get("T2M")).get(f"{y}{m:02d}")) for m in range(1, 13)]
        p = [_clean(_as_dict(raw.get("PRECTOTCORR")).get(f"{y}{m:02d}")) for m in range(1, 13)]
        if None in t or None in p:
            continue
        years.append(y)
        temps.append(sum(t) / 12)
        rains.append(sum(pm * d for pm, d in zip(p, MONTH_DAYS)))
    if len(years) < 15:
        return None
    ts, tt = _ols(years, temps)
    rs, rt = _ols(years, rains)
    mean_rain = sum(rains) / len(rains)
    return {"period": f"{years[0]}-{years[-1]}", "n_years": len(years),
            "mean_annual_temp_c": rnd(sum(temps) / len(temps), 1), "mean_annual_rain_mm": rnd(mean_rain, 0),
            "temperature_c_per_decade": rnd(ts * 10, 2), "temperature_significant": bool(abs(tt) > 2.0),
            "rain_mm_per_decade": rnd(rs * 10, 0),
            "rain_pct_per_decade": rnd(rs * 10 / mean_rain * 100, 1) if mean_rain > 0 else None,
            "rain_significant": bool(abs(rt) > 2.0)}


def _safe_trend(lat, lon):
    key = f"trend:{lat:.2f}:{lon:.2f}:{datetime.now(timezone.utc).year}"
    hit = cache.get(key)
    if hit is not None:
        return hit["data"]
    try:
        data = fetch_trend(lat, lon)
    except PowerError:
        log.warning("Long-term trend unavailable; continuing without it")
        return None                                      # failures are not cached
    cache.set(key, {"data": data}, 7 * 86400)
    return data


def rotation_envelope(loc: Loc, clim: dict, cl: dict, weights, water, cur_r, cur_k, market, scen=(0.0, 0.0), trend=None) -> dict:
    soil_key, soil_warn = resolve_soil(loc.soil)
    plan = plan_rotations(cl, loc.latitude, soil_key, water, weights, market, cur_r, cur_k)
    strategies = [_describe(c, plan["existing"], cl, weights, water) for c in plan["shown"]]
    for i, st in enumerate(strategies, 1):
        st["rank"] = i
    cur = next(s for s in strategies if s["role"] == "existing")
    alt = next(s for s in strategies if s["role"] == "alternative")
    best_alt = max((s for s in strategies if s["role"] == "alternative"), key=lambda s: s["score"])
    stab = plan["stability_pct"]
    lab = lambda k: ROT_LABEL_UR[k]
    alt_ur = f"{lab(plan['alts'][0]['rabi'])} → {lab(plan['alts'][0]['kharif'])}" if plan["alts"] else ""
    cur_ur = f"{lab(cur_r)} → {lab(cur_k)}"
    if best_alt["score"] > cur["score"]:
        en = (f"Best-scoring alternative for your priorities: {best_alt['name']} ({best_alt['score']:.0f}/100) versus your current "
              f"{cur['name']} ({cur['score']:.0f}/100). The top-ranked option stayed first in {stab:.0f}% of small changes to your priorities.")
        top_alt = max(plan["alts"], key=lambda c: c["score"])
        ur = (f"آپ کی ترجیحات کے مطابق بہترین متبادل: {lab(top_alt['rabi'])} → {lab(top_alt['kharif'])} (اسکور {best_alt['score']:.0f}/100) "
              f"جبکہ آپ کی موجودہ ترتیب {cur_ur} کا اسکور {cur['score']:.0f}/100 ہے۔ ترجیحات میں معمولی ردوبدل پر پہلا انتخاب {stab:.0f}% صورتوں میں برقرار رہا۔")
    else:
        en = (f"Your current rotation ({cur['name']}) scores {cur['score']:.0f}/100; no alternative scored higher for your priorities "
              f"(best alternative: {best_alt['name']}, {best_alt['score']:.0f}/100).")
        ur = f"آپ کی موجودہ ترتیب ({cur_ur}) کا اسکور {cur['score']:.0f}/100 ہے اور آپ کی ترجیحات کے مطابق کوئی متبادل اس سے بہتر نہیں نکلا۔"
    if trend and trend.get("rain_pct_per_decade") is not None:
        calm = "" if (trend["temperature_significant"] or trend["rain_significant"]) else " (within normal year-to-year variation)"
        en += (f" Local trend {trend['period']}: {trend['temperature_c_per_decade']:+.2f} C and "
               f"{trend['rain_pct_per_decade']:+.0f}% rainfall per decade{calm}.")
        ur += (f" مقامی رجحان {trend['period']}: ہر دہائی میں درجۂ حرارت {trend['temperature_c_per_decade']:+.2f} ڈگری اور "
               f"بارش {trend['rain_pct_per_decade']:+.0f}%۔")
    en += " This compares options using NASA climate data only - confirm soil, markets and local advice before changing your rotation."
    ur += " یہ موازنہ صرف ناسا کے موسمی ڈیٹا پر مبنی ہے؛ فصلی ترتیب بدلنے سے پہلے مٹی، منڈی اور مقامی ماہرین سے تصدیق کریں۔"
    ann_rain, ann_et0 = sum(cl["rain"]), sum(cl["et0"])
    hot = max(range(12), key=lambda i: cl["tmax"][i])
    cold = min(range(12), key=lambda i: cl["tmin"][i])
    cov = {p: round(100 * sum(v is not None for v in clim[p]) / 12, 1) for p in ROT_CLIM_PARAMS}
    limits = ["Monthly climate means (NASA POWER climatology) are used, not year-by-year weather: extremes and variability are hidden.",
              "Assumptions: 80% of rain is effective; half of the soil's plant-available water is stored at sowing; single mid-season Kc.",
              "Seasonal windows assume a northern-subtropical rabi (winter)/kharif (summer) pattern (shifted 6 months south of the equator); "
              "sowing dates must be adjusted locally.",
              "Scores rank options for YOUR stated priorities; a 5-point difference is not meaningful - see the rank-stability figure.",
              "Rotations here span one year (two seasons); multi-year pest and soil-organic-matter dynamics are not modelled.",
              "This tool supports farmer judgement and does not replace local agricultural expertise."]
    warnings = [soil_warn] if soil_warn else []
    if not 15 <= loc.latitude <= 42:
        warnings.append("The crop calendar here (winter crop Nov-Apr, summer crop May-Oct) and the crop traits are for the northern "
                        "subtropics such as Pakistan. For this latitude (southern hemisphere, tropics or far north) the rotation "
                        "results are NOT meaningful.")
    if not any(weights.values()):
        warnings.append("All priorities are 0, so only climate suitability was scored. Raise at least one priority.")
    if scen[0] or scen[1]:
        warnings.append(f"What-if scenario applied: rainfall {scen[0]:+.0f}%, warming {scen[1]:+.1f} C (humidity not adjusted).")
    return {
        "status": "success", "module": "rotation", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "location": {"latitude": loc.latitude, "longitude": loc.longitude, "label": loc.region or None},
        "window": {"start": "climatology", "end": "monthly means", "days": 365, "last_valid_date": None, "data_age_days": None},
        "data_quality": {"coverage_pct": cov, "missing_parameters": [p for p, c in cov.items() if c == 0.0],
                         "climatology_available": True},
        "summary": {"en": en, "ur": ur}, "independence": INDEPENDENCE,
        "data_categories": {"historical_nasa": {**CAT_NASA, "note": "monthly climatology and 25-year monthly series"}},
        "assessment": {"level": "Low", "reasons": [], "method": "Weighted transparent scoring of 7 criteria (see strategies[].criteria)."},
        "inputs": {"soil_type": soil_key, "water_availability": water, "priorities": weights, "current_rotation": f"{cur_r}+{cur_k}",
                   "scenario": {"rain_change_pct": scen[0], "temp_change_c": scen[1]}},
        "climate": {"long_term_trend": trend, "annual_rain_mm": round(ann_rain), "annual_et0_mm": round(ann_et0), "aridity_index_rain_over_et0": round(ann_rain / ann_et0, 2),
                    "hottest_month": f"{MONTH_ABBR[hot]} (mean daily max {cl['tmax'][hot]:.1f} C)",
                    "coldest_month": f"{MONTH_ABBR[cold]} (mean daily min {cl['tmin'][cold]:.1f} C)"},
        "rank_stability_pct": stab,
        "strategies": strategies,
        "series": {"dates": MONTH_ABBR, "rain_mm_month": [rnd(x, 0) for x in cl["rain"]], "et0_mm_month": [rnd(x, 0) for x in cl["et0"]],
                   "tmax_c": [rnd(x, 1) for x in cl["tmax"]], "tmin_c": [rnd(x, 1) for x in cl["tmin"]]},
        "advisory": [], "metrics": {},
        "provenance": {"source": "NASA POWER climatology (monthly means) + 25-year monthly series for the trend, community AG", "baseline": None,
                       "references": ["FAO Irrigation & Drainage Paper 56 (Allen et al., 1998)"]},
        "limitations": limits, "warnings": warnings,
    }


# ------------------------------------------------------------------------------
# EXPERIMENTAL: satellite greenness (MODIS NDVI) from the NASA ORNL DAAC MODIS subset web service (no login).
# OFF unless ENABLE_NDVI=1. The developer could NOT test it against the live service: run `--live` first,
# and only switch it on if that prints "[ndvi] ... OK".
# ------------------------------------------------------------------------------
NDVI_ENABLED = os.getenv("ENABLE_NDVI") == "1"
ORNL_BASE = "https://modis.ornl.gov/rst/api/v1"
NDVI_PRODUCT, NDVI_BAND = "MOD13Q1", "250m_16_days_NDVI"
CAT_NDVI = {"label": "NASA MODIS Terra NDVI (ORNL DAAC subset service) - satellite observation of one ~250 m pixel, "
                     "not a field measurement", "provider": "NASA MODIS via ORNL DAAC",
            "resolution": "250 m, every 16 days", "status": "experimental"}
MON_EN = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MON_UR = ["جنوری", "فروری", "مارچ", "اپریل", "مئی", "جون", "جولائی", "اگست", "ستمبر", "اکتوبر", "نومبر", "دسمبر"]


class NdviError(PowerError):
    pass


def ndvi_request(path: str, params: dict) -> dict:
    """Network choke-point for the NDVI module (mocked in tests)."""
    try:
        resp = _http.get(ORNL_BASE + path, params=params, headers={"Accept": "application/json"}, timeout=(5, 40))
        resp.raise_for_status()
        body = resp.json()
    except requests.RequestException as exc:
        log.error("ORNL DAAC request failed: %s", exc)
        raise NdviError("The NASA ORNL DAAC NDVI service is unreachable or returned an error") from exc
    except ValueError as exc:
        raise NdviError("The NDVI service returned invalid JSON") from exc
    if not isinstance(body, dict):
        raise NdviError("The NDVI service returned unexpected data")
    return body


def fetch_ndvi_series(lat: float, lon: float, years: int = 3) -> dict:
    body = ndvi_request(f"/{NDVI_PRODUCT}/dates", {"latitude": lat, "longitude": lon})
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=int(365.25 * years))
    steps = []
    for it in body.get("dates") or []:
        try:
            steps.append((date.fromisoformat(str(it["calendar_date"])[:10]), str(it["modis_date"])))
        except (KeyError, TypeError, ValueError):
            continue
    steps = sorted(x for x in steps if x[0] >= cutoff)
    if len(steps) < 8:
        raise NdviError("Not enough NDVI observation dates for this location")
    chunks = [steps[i:i + 10] for i in range(0, len(steps), 10)]        # the service allows ~10 dates per request

    def one(ch):
        b = ndvi_request(f"/{NDVI_PRODUCT}/subset", {"latitude": lat, "longitude": lon, "band": NDVI_BAND,
                                                      "startDate": ch[0][1], "endDate": ch[-1][1],
                                                      "kmAboveBelow": 0, "kmLeftRight": 0})
        try:
            scale = float(b.get("scale") or 0.0001)
        except (TypeError, ValueError):
            scale = 0.0001
        if not 0 < scale < 1:
            scale = 0.0001
        out = []
        for row in b.get("subset") or []:
            try:
                d, data = date.fromisoformat(str(row["calendar_date"])[:10]), row.get("data")
            except (KeyError, TypeError, ValueError, AttributeError):
                continue
            raw = _clean(data[0]) if isinstance(data, list) and data else None
            out.append((d, raw * scale if raw is not None and -2000 <= raw <= 10000 else None))
        return out
    got, failed = [], 0
    with ThreadPoolExecutor(max_workers=3) as pool:
        for fut in [pool.submit(one, ch) for ch in chunks]:
            try:
                got.extend(fut.result())
            except PowerError:
                failed += 1
    if not got:
        raise NdviError("The NDVI service returned no data for this location")
    return {"series": sorted(dict(got).items()), "chunks": len(chunks), "chunks_failed": failed}


def greenness_class(v):
    return ("bare or very sparse", "بنجر یا بہت کم سبزہ") if v < 0.2 else \
           ("sparse or young crop", "کم یا نئی فصل") if v < 0.4 else \
           ("moderately green", "درمیانہ سبزہ") if v < 0.6 else ("dense green", "گھنا سبزہ")


def assess_ndvi(res: dict, loc: Loc):
    series = res["series"]
    valid = [(d, v) for d, v in series if v is not None]
    months = {m: [] for m in range(1, 13)}
    for d, v in valid:
        months[d.month].append(v)
    mm = {m: (sum(x) / len(x) if len(x) >= 2 else None) for m, x in months.items()}
    bare = [m for m, v in mm.items() if v is not None and v < 0.2]
    peak = max((m for m in mm if mm[m] is not None), key=lambda m: mm[m], default=None)
    latest = valid[-1] if valid else None
    base = []
    if latest:
        ld = latest[0]
        for d, v in valid:
            if d < ld - timedelta(days=200) and abs(((d.timetuple().tm_yday - ld.timetuple().tm_yday + 183) % 365) - 183) <= 16:
                base.append(v)
    baseline = sum(base) / len(base) if len(base) >= 2 else None
    anomaly = latest[1] - baseline if latest and baseline is not None else None
    change = latest[1] - valid[-2][1] if len(valid) >= 2 else None

    reasons, advisory = [], []
    if latest is None or len(valid) < 8:
        level = "Insufficient data"
        reasons.append("Too few valid satellite observations (clouds and fill values are removed).")
    elif anomaly is None:
        level = "Insufficient data"
        reasons.append("No comparable same-season observation from earlier years, so no anomaly can be computed.")
    else:
        level = "High" if anomaly <= -0.20 else "Moderate" if anomaly <= -0.10 else "Low"
        reasons.append(f"Latest NDVI {latest[1]:.2f} is {anomaly:+.2f} versus the same season in earlier years ({baseline:.2f}).")
        if change is not None:
            reasons.append(f"Change since the previous 16-day observation: {change:+.2f}.")
        advisory.append("Greenness is below the usual level for this season: walk the field to check. A lower value can also "
                        "mean a different crop, a later sowing date or cloud-affected data, not necessarily stress."
                        if level != "Low" else "Greenness is within the usual range for this season.")
    if bare:
        advisory.append("Months that are usually bare or very sparse here: " + ", ".join(MON_EN[m - 1] for m in bare) +
                        ". Cover crops or relay cropping may reduce bare-soil time (see the Rotation tab).")
    advisory.append("The satellite pixel is about 250 m wide: if your field is small or mixed with trees/roads, check on the ground.")

    cls = greenness_class(latest[1]) if latest else None
    metrics = {
        "latest_ndvi": rnd(latest[1] if latest else None, 2), "latest_date": latest[0].isoformat() if latest else None,
        "greenness_class": cls[0] if cls else None,
        "same_season_baseline_ndvi": rnd(baseline, 2), "anomaly_vs_same_season": rnd(anomaly, 2),
        "change_since_previous_observation": rnd(change, 2),
        "peak_month": MON_EN[peak - 1] if peak else None, "peak_month_mean_ndvi": rnd(mm[peak], 2) if peak else None,
        "usually_bare_months": [MON_EN[m - 1] for m in bare],
        "valid_observations": len(valid), "total_observations": len(series),
    }
    series_out = {"ndvi": [rnd(v, 3) for _, v in series], "monthly_mean_ndvi_jan_to_dec": [rnd(mm[m], 3) for m in range(1, 13)]}
    srcen = "NASA MODIS via AppEEARS, from your file" if res.get("source") == "file" else "NASA MODIS via ORNL DAAC, experimental"
    srcur = "ناسا MODIS بذریعہ AppEEARS، آپ کی فائل سے" if res.get("source") == "file" else "ناسا MODIS بذریعہ ORNL DAAC، تجرباتی"
    en = ("Satellite greenness (NDVI) could not be assessed for this place yet."
          if latest is None else
          f"Latest satellite greenness (NDVI) is {latest[1]:.2f} ({cls[0]}) on {latest[0]}"
          + (f", {anomaly:+.2f} versus the same season in earlier years" if anomaly is not None else "") + "."
          + (" Months that are usually bare or very sparse here: " + ", ".join(MON_EN[m - 1] for m in bare) + "." if bare else "")
          + f" This is one ~250 m pixel ({srcen}) and may not match your field.")
    ur = ("اس جگہ کے لیے سیٹلائٹ سبزہ (NDVI) ابھی جانچا نہیں جا سکا۔"
          if latest is None else
          f"تازہ ترین سیٹلائٹ سبزہ (NDVI) {latest[1]:.2f} ({cls[1]}) ہے، تاریخ {latest[0]}"
          + (f"، پچھلے سالوں کے اسی موسم کے مقابلے میں {anomaly:+.2f}" if anomaly is not None else "") + "۔"
          + (" یہاں عموماً بنجر یا بہت کم سبزے والے مہینے: " + "، ".join(MON_UR[m - 1] for m in bare) + "۔" if bare else "")
          + f" یہ تقریباً 250 میٹر کا ایک پکسل ہے ({srcur}) اور آپ کے کھیت سے مختلف ہو سکتا ہے۔")
    first = ("Values come from the file you supplied; the app cannot check that it really is MOD13Q1 or that the place matches."
             if res.get("source") == "file" else
             "Experimental: depends on a third-party NASA ORNL DAAC web service that may change or be unavailable.")
    limits = [first,
              "One ~250 m pixel mixes everything inside it (field, roads, trees); the quality flags are not used to filter values.",
              "A different crop or sowing date than in earlier years can look like stress or recovery.",
              "NDVI shows greenness, not yield, disease or water stress by itself."]
    return metrics, {"level": level, "reasons": reasons,
                     "method": "MOD13Q1 16-day NDVI (scale 0.0001) for the pixel containing the point; fill values removed; "
                               "anomaly = latest minus the mean of observations within +/-16 days of the same day of year in "
                               "earlier years (needs 2+); usually-bare months = monthly mean NDVI below 0.2."}, \
        advisory, series_out, limits, {"en": en, "ur": ur}


CAT_NDVI_FILE = {"label": "NASA MODIS Terra NDVI (MOD13Q1, via NASA AppEEARS) - satellite observation of one ~250 m pixel, "
                         "supplied by you as a file; not a field measurement", "provider": "NASA MODIS via AppEEARS",
                 "resolution": "250 m, every 16 days"}
_DATE_FORMATS = ("%Y-%m-%d", "%m-%d-%Y", "%m/%d/%Y", "%Y%m%d")
_ROW_RX = re.compile(r"(\d{4}-\d{2}-\d{2}|\d{2}[-/]\d{2}[-/]\d{4})[\s,;]+(-?\d+(?:\.\d+)?)")


def _parse_date(txt):
    txt = str(txt).strip().replace("/", "-")
    for f in _DATE_FORMATS:
        try:
            return datetime.strptime(txt, f).date()
        except ValueError:
            pass
    return None


def _ndvi_value(raw):
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v):
        return None
    if abs(v) > 1.5:                      # unscaled MODIS integers (scale factor 0.0001)
        v *= 0.0001
    return v if -0.2 <= v <= 1.0 else None   # outside the valid NDVI range = fill value


def read_ndvi_text(text: str):
    """Parse a NASA AppEEARS point-sample CSV, or the table copied from its Explore page, into ([(date, ndvi|None)], point)."""
    if len(text) > 200_000:
        raise ValueError("NDVI file: at most 200 KB")
    lines = [ln for ln in text.lstrip("\ufeff").splitlines() if ln.strip()]
    if len(lines) > 5000:
        raise ValueError("NDVI file: at most 5000 lines")
    series, point = {}, None
    if lines and "," in lines[0] and re.search(r"\bdate\b", lines[0], re.I):
        rd = csv.DictReader(io.StringIO("\n".join(lines)))
        names = rd.fieldnames or []
        dcol = next((c for c in names if c.strip().lower() == "date"), None)
        vcol = next((c for c in names if "ndvi" in c.lower() and not re.search(r"qual|qa|reliab|state|flag|std", c, re.I)), None)
        if not dcol or not vcol:
            raise ValueError("NDVI file: need a 'Date' column and an NDVI column (AppEEARS CSV export)")
        rcol = next((c for c in names if "reliab" in c.lower()), None)
        pts = set()
        for rec in rd:
            d = _parse_date(rec.get(dcol) or "")
            if d is None:
                continue
            la, lo = (rec.get("Latitude") or "").strip(), (rec.get("Longitude") or "").strip()
            if la and lo:
                pts.add((la, lo))
            v = _ndvi_value(rec.get(vcol))
            if rcol:
                try:
                    if int(float(rec.get(rcol))) in (2, 3, -1):       # snow/ice, cloudy, no data
                        v = None
                except (TypeError, ValueError):
                    pass
            series[d] = v
        if len(pts) > 1:
            raise ValueError("NDVI file: it contains several points; export one point per file")
        if pts:
            try:
                point = (float(next(iter(pts))[0]), float(next(iter(pts))[1]))
            except ValueError:
                point = None
    else:
        for ln in lines:
            m = _ROW_RX.search(ln)
            d = _parse_date(m.group(1)) if m else None
            if d is not None:
                series[d] = _ndvi_value(m.group(2))
    if not any(v is not None for v in series.values()):
        raise ValueError("NDVI file: no rows with a date and a valid NDVI value were found")
    return sorted(series.items()), point


def ndvi_envelope(loc: Loc, res: dict, parts) -> dict:
    metrics, assessment, advisory, series_out, limits, summary = parts
    series = res["series"]
    valid = [d for d, v in series if v is not None]
    cov = round(100 * len(valid) / len(series), 1)
    last = valid[-1] if valid else None
    src_file, point = res.get("source") == "file", res.get("point")
    lat_, lon_ = point if point else (loc.latitude, loc.longitude)
    warnings = [] if src_file else ["Experimental module; see the limitations."]
    if src_file and point and (abs(point[0] - loc.latitude) > 0.05 or abs(point[1] - loc.longitude) > 0.05):
        warnings.append(f"The file is for {point[0]}, {point[1]}, not the coordinates entered ({loc.latitude}, {loc.longitude}); "
                        f"the results describe the file's point.")
    return {
        "status": "success", "module": "ndvi", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "location": {"latitude": lat_, "longitude": lon_, "label": loc.region or None},
        "window": {"start": series[0][0].isoformat(), "end": series[-1][0].isoformat(), "days": (series[-1][0] - series[0][0]).days,
                   "last_valid_date": last.isoformat() if last else None,
                   "data_age_days": (datetime.now(timezone.utc).date() - last).days if last else None,
                   "mode": "satellite file" if src_file else "satellite, experimental"},
        "data_quality": {"coverage_pct": {"NDVI": cov}, "missing_parameters": [] if valid else ["NDVI"],
                         "climatology_available": False, "request_chunks_failed": res.get("chunks_failed", 0)},
        "summary": summary, "data_categories": {"satellite_observation": CAT_NDVI_FILE if src_file else CAT_NDVI},
        "independence": INDEPENDENCE,
        "uncertainty": {"NDVI": {"expected_reliability": "medium", "coverage_pct": cov,
                                 "basis": "One ~250 m pixel; the quality flags are not used to filter values; NOT validated at your location."}},
        "metrics": metrics, "assessment": assessment, "advisory": advisory,
        "series": {"dates": [d.isoformat() for d, _ in series], **series_out},
        "provenance": {"source": ("NASA MODIS Terra MOD13Q1 NDVI via NASA AppEEARS, file supplied by the user" if src_file else
                                  "NASA MODIS Terra MOD13Q1 via the ORNL DAAC MODIS subset web service (experimental)"),
                       "baseline": None,
                       "references": ["MODIS/Terra Vegetation Indices 16-Day L3 Global 250m (MOD13Q1), NASA LP DAAC - verify citation",
                                      "AppEEARS Team, NASA LP DAAC (Application for Extracting and Exploring Analysis Ready Samples) - verify citation"]},
        "limitations": limits, "warnings": warnings,
    }


MODULES = {"agrispace": assess_agri, "bioshield": assess_bio, "ecoguard": assess_eco}
COMMON_LIMITS = [
    "Advisory output from reanalysis/satellite-derived data (~0.5 deg grid, 3-7 day latency); "
    "not a substitute for field measurements or local expert advice.",
]


def make_summary(module: str, loc: Loc, b: dict, metrics: dict, assessment: dict) -> dict:
    """Deterministic plain-language summary in English and Urdu (no AI text generation)."""
    lv = assessment["level"]
    lvu = LEVEL_UR.get(lv, lv)
    n = len(b["dates"])
    ckey, crop, _ = resolve_crop(loc.crop)
    cu = CROP_UR.get(ckey, "فصل")

    def pct(x):
        return "n/a" if x is None else f"{x:.0f}"

    if lv == "Insufficient data":
        en = "There is not enough valid NASA data for this place and period to give an assessment."
        ur = "اس جگہ اور مدت کے لیے ناسا کا کافی درست ڈیٹا موجود نہیں، اس لیے تجزیہ ممکن نہیں۔"
    elif module == "agrispace":
        w = metrics["root_zone_wetness_recent_pct"]
        r = metrics["rain_to_demand_ratio"]
        rp = None if r is None else min(r, 9.99) * 100
        en = (f"{crop['label']}: water-stress risk is {lv}. Root-zone soil moisture is about {pct(w)}% of "
              f"saturation, and rain covered {pct(rp)}% of crop water demand over the last {n} days.")
        ur = (f"{cu}: پانی کی کمی کا خطرہ {lvu} ہے۔ جڑوں کے علاقے میں نمی تقریباً {pct(w)}% ہے اور "
              f"پچھلے {n} دنوں میں بارش نے فصل کی ضرورت کا {pct(rp)}% پورا کیا۔")
        u7 = metrics["crop_water_use_next_7d_mm"]
        if lv in ("High", "Moderate") and u7 is not None:
            en += f" Expect about {u7:.0f} mm of crop water use in the next 7 days; check soil moisture in the field."
            ur += f" اگلے 7 دنوں میں فصل کو تقریباً {u7:.0f} ملی میٹر پانی درکار ہو سکتا ہے؛ کھیت میں نمی ناپ کر دیکھیں۔"
    elif module == "bioshield":
        hd, fd, th = metrics["heat_stress_days"], metrics["frost_days"], metrics["heat_threshold_c"]
        en = (f"{crop['label']}: heat/cold stress risk is {lv}. In the last {n} days, {hd} days were hotter than "
              f"{th:.0f} C and {fd} had frost.")
        ur = (f"{cu}: گرمی/سردی کے دباؤ کا خطرہ {lvu} ہے۔ پچھلے {n} دنوں میں {hd} دن درجۂ حرارت "
              f"{th:.0f} ڈگری سے زیادہ رہا اور {fd} دن پالا پڑا۔")
    else:
        c = metrics["heat_index_days"]
        ext = c["extreme_caution"] + c["danger"] + c["extreme_danger"]
        place_en, place_ur = (loc.region or "This area"), (loc.region or "یہ علاقہ")
        en = (f"{place_en}: heat risk is {lv}. Peak heat index was {pct(metrics['heat_index_max_c'])} C and "
              f"{ext} of the last {n} days reached 'extreme caution' or worse.")
        ur = (f"{place_ur}: گرمی کا خطرہ {lvu} ہے۔ زیادہ سے زیادہ ہیٹ انڈیکس {pct(metrics['heat_index_max_c'])} "
              f"ڈگری رہا اور پچھلے {n} دنوں میں {ext} دن 'انتہائی احتیاط' یا اس سے اوپر کی سطح پر پہنچے۔")
    return {"en": en + DISCLAIMER["en"], "ur": ur + DISCLAIMER["ur"]}


def build_envelope(module: str, loc: Loc, b: dict, parts) -> dict:
    metrics, assessment, advisory, series, limits, warnings = parts
    t = b["series"]["T2M"]
    last_valid = next((d for d, v in zip(reversed(b["dates"]), reversed(t)) if v is not None), None)
    return {
        "status": "success", "module": module, "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "location": {"latitude": loc.latitude, "longitude": loc.longitude, "label": loc.region or None},
        "window": {"start": b["start"].isoformat(), "end": b["end"].isoformat(),
                   "days": len(b["dates"]), "last_valid_date": last_valid.isoformat() if last_valid else None,
                   "data_age_days": (datetime.now(timezone.utc).date() - last_valid).days if last_valid else None},
        "data_quality": {"coverage_pct": b["coverage"], "missing_parameters": b["missing"],
                         "climatology_available": b["climatology"] is not None},
        "summary": make_summary(module, loc, b, metrics, assessment),
        "data_categories": {"historical_nasa": {**CAT_NASA, "window": f"{b['start']} to {b['end']}"}},
        "uncertainty": uncertainty_for(b["coverage"]), "independence": INDEPENDENCE,
        "metrics": metrics, "assessment": assessment, "advisory": advisory,
        "series": {"dates": [d.isoformat() for d in b["dates"]], **series},
        "provenance": {"source": "NASA POWER Daily API, community AG",
                       "baseline": "NASA POWER climatological monthly means" if b["climatology"] else None,
                       "references": ["FAO Irrigation & Drainage Paper 56 (Allen et al., 1998)",
                                      "US NWS heat index", "WHO Global Solar UV Index"]},
        "limitations": COMMON_LIMITS + limits, "warnings": warnings,
    }


# ==============================================================================
# Web layer
# ==============================================================================
@app.before_request
def _size_guard():
    if request.method == "POST" and request.path not in ("/api/validate", "/api/ndvi"):
        cl = request.content_length
        if cl is not None and cl > 4096:
            resp = jsonify({"status": "error", "detail": "Request too large"})
            resp.status_code = 413
            return resp
    return None


@app.before_request
def _rate_limit():
    if not app.config["RATE_LIMIT_ENABLED"] or not request.path.startswith("/api/"):
        return None
    key = request.remote_addr or "unknown"
    for lim in ((limit_geo,) if request.path == "/api/geocode" else (limit_minute, limit_hour)):
        ok, retry = lim.check(key)
        if not ok:
            resp = jsonify({"status": "error", "detail": "Rate limit exceeded"})
            resp.status_code = 429
            resp.headers["Retry-After"] = str(retry)
            return resp
    return None


@app.after_request
def _security_headers(resp):
    nonce = getattr(request, "csp_nonce", None)
    script = f"'nonce-{nonce}'" if nonce else "'none'"
    resp.headers.setdefault(
        "Content-Security-Policy",
        f"default-src 'none'; script-src {script}; style-src {script}; connect-src 'self'; "
        "img-src 'self' https://gibs.earthdata.nasa.gov; manifest-src 'self'; worker-src 'self'; "
        "base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("Referrer-Policy", "no-referrer")
    resp.headers.setdefault("Permissions-Policy", "geolocation=(self)")
    if request.path.startswith("/api/"):
        resp.headers["Cache-Control"] = "no-store"
    return resp


def _make_view(name, fn):
    def view():
        try:
            loc = parse_location(request.get_json(silent=True))
        except ValueError as exc:
            return jsonify({"status": "error", "detail": str(exc)}), 400
        try:
            bundle = get_power_bundle(loc.latitude, loc.longitude, loc.days)
        except PowerError as exc:
            return jsonify({"status": "error", "detail": str(exc)}), 502
        try:
            result = build_envelope(name, loc, bundle, fn(bundle, loc))
        except Exception:  # pragma: no cover - defensive
            log.exception("Assessment failed for %s", name)
            return jsonify({"status": "error", "detail": "Internal analysis error"}), 500
        save_log(name, f"{loc.latitude:.3f},{loc.longitude:.3f}", f"level={result['assessment']['level']}")
        return jsonify(result)

    view.__name__ = f"api_{name}"
    return view


for _name, _fn in MODULES.items():
    app.add_url_rule(f"/api/{_name}", endpoint=f"api_{_name}", view_func=_make_view(_name, _fn),
                     methods=["POST"])


@app.route("/api/live", methods=["POST"])
def api_live():
    try:
        loc = parse_location(request.get_json(silent=True))
    except ValueError as exc:
        return jsonify({"status": "error", "detail": str(exc)}), 400
    try:
        lv = cached_call(f"live:{loc.latitude:.2f}:{loc.longitude:.2f}", LIVE_TTL,
                         lambda: fetch_live(loc.latitude, loc.longitude))
    except PowerError as exc:
        return jsonify({"status": "error", "detail": str(exc)}), 502
    result = live_envelope(loc, lv, assess_live(lv, loc))
    save_log("live", f"{loc.latitude:.3f},{loc.longitude:.3f}", f"level={result['assessment']['level']}")
    return jsonify(result)


@app.route("/api/geocode")
def api_geocode():
    q = _CTRL.sub("", request.args.get("q", "")).strip()
    if not 2 <= len(q) <= 60:
        return jsonify({"status": "error", "detail": "q: 2 to 60 characters required"}), 400
    results, warnings = search_places(q), []
    if len(results) < 5:
        try:
            seen = {(r["latitude"], r["longitude"]) for r in results}
            for r in cached_call(f"geo:{q.lower()}", 86400, lambda: geocode_online(q)):
                if (r["latitude"], r["longitude"]) not in seen:
                    results.append(r)
        except PowerError:
            warnings.append("Online place search is unavailable; showing built-in places only.")
    return jsonify({"status": "success", "query": q, "results": results[:8], "warnings": warnings,
                    "attribution": "Built-in list + Open-Meteo Geocoding (GeoNames data)"})


@app.route("/api/rotation", methods=["POST"])
def api_rotation():
    try:
        loc, weights, water, cur_r, cur_k, market, scen = parse_rotation(request.get_json(silent=True))
    except ValueError as exc:
        return jsonify({"status": "error", "detail": str(exc)}), 400
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:             # climatology and trend are fetched in parallel
            f_trend = pool.submit(_safe_trend, loc.latitude, loc.longitude)
            clim = cached_call(f"rot:{loc.latitude:.2f}:{loc.longitude:.2f}", 7 * 86400,
                               lambda: fetch_climatology(loc.latitude, loc.longitude, ROT_CLIM_PARAMS))
            trend = f_trend.result()
        cl = rot_climate(scenario_clim(clim, *scen), loc.latitude)
    except PowerError as exc:
        return jsonify({"status": "error", "detail": str(exc)}), 502
    result = rotation_envelope(loc, clim, cl, weights, water, cur_r, cur_k, market, scen, trend)
    save_log("rotation", f"{loc.latitude:.3f},{loc.longitude:.3f}", f"top={result['strategies'][0]['id']}")
    return jsonify(result)


@app.route("/api/validate", methods=["POST"])
def api_validate():
    """Compare NASA POWER with the user's own observations (the third data category: measured, not NASA)."""
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return jsonify({"status": "error", "detail": "body: JSON object required"}), 400
    try:
        lat, lon = _num(body, "latitude", -90, 90), _num(body, "longitude", -180, 180)
        if lat is None or lon is None:
            raise ValueError("latitude and longitude are required")
        text, recs = body.get("csv"), body.get("observations")
        if isinstance(text, str) and text.strip():
            obs = read_obs_text(text)
        elif isinstance(recs, list) and recs:
            if len(recs) > 1095:
                raise ValueError("observations: at most 1095 records per call")
            obs = observations_from_records(recs)
        else:
            raise ValueError("send 'csv' (text with a date column) or 'observations' (a list of records)")
        if not any(obs.values()):
            raise ValueError("no usable observations found")
        rep_ = run_validation(obs, round(lat, 4), round(lon, 4))
    except (ValueError, csv.Error) as exc:
        return jsonify({"status": "error", "detail": str(exc)}), 400
    except PowerError as exc:
        return jsonify({"status": "error", "detail": str(exc)}), 502
    p = rep_["period"]
    rep_.update({
        "status": "success", "module": "validation", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "interpretation": interpret_validation(rep_),
        "summary": {"en": f"Your own observations were compared with NASA POWER for {p['start']} to {p['end']}. "
                          f"Bias = NASA minus your data. See the table and the plain statements below.",
                    "ur": f"آپ کے اپنے مشاہدات کا موازنہ ناسا پاور سے کیا گیا ({p['start']} سے {p['end']})۔ "
                          f"فرق = ناسا منفی آپ کا ڈیٹا۔ نیچے جدول اور سادہ بیانات دیکھیں۔"},
        "data_categories": {"historical_nasa": CAT_NASA, "ground_observation": CAT_OBS},
        "independence": INDEPENDENCE,
        "limitations": ["NASA POWER is a ~50 km grid-cell estimate: a single station differs, especially for rainfall and in hilly terrain.",
                        "These numbers describe past agreement at this site only; they do not guarantee future accuracy or transfer to other places.",
                        "At least 5 overlapping days per variable are needed; 30 or more are better.",
                        "No pass/fail accuracy threshold is applied: judge the errors against your own tolerance."]})
    save_log("validate", f"{lat:.3f},{lon:.3f}", f"vars={','.join(rep_['results'])}")
    return jsonify(rep_)


@app.route("/api/ndvi", methods=["POST"])
def api_ndvi():
    body = request.get_json(silent=True)
    text = body.get("csv") if isinstance(body, dict) else None
    use_file = isinstance(text, str) and bool(text.strip())
    if not use_file and not NDVI_ENABLED:
        return jsonify({"status": "error", "detail": "Paste or upload a NASA AppEEARS NDVI file (Extract > Point Samples, layer "
                                                     "_250m_16_days_NDVI). The live NDVI service is experimental and switched off."}), 501
    try:
        loc = parse_location(body)
        if use_file:
            series, point = read_ndvi_text(text)
            res = {"series": series, "chunks": 0, "chunks_failed": 0, "source": "file", "point": point}
        else:
            res = cached_call(f"ndvi:{loc.latitude:.3f}:{loc.longitude:.3f}:{datetime.now(timezone.utc).date()}", 86400,
                              lambda: fetch_ndvi_series(loc.latitude, loc.longitude))
    except (ValueError, csv.Error) as exc:
        return jsonify({"status": "error", "detail": str(exc)}), 400
    except PowerError as exc:
        return jsonify({"status": "error", "detail": str(exc)}), 502
    result = ndvi_envelope(loc, res, assess_ndvi(res, loc))
    save_log("ndvi", f"{loc.latitude:.3f},{loc.longitude:.3f}", f"level={result['assessment']['level']}")
    return jsonify(result)


@app.route("/health")
def health():
    ok = db_ok()
    return jsonify({"status": "ok" if ok else "degraded", "database": ok, "version": VERSION,
                    "time": datetime.now(timezone.utc).isoformat()}), (200 if ok else 503)


@app.route("/")
def home():
    request.csp_nonce = secrets.token_urlsafe(16)
    return INDEX_HTML.replace("__NONCE__", request.csp_nonce).replace("__PLACES__", PLACES_JSON).replace("__NDVI__", "true" if NDVI_ENABLED else "false"), 200, {"Content-Type": "text/html; charset=utf-8"}


# ------------------------------------------------------------------------------
# PWA: installable on Android/desktop (needs HTTPS or localhost)
# ------------------------------------------------------------------------------
SW_JS = """const C="s2e-v3";
self.addEventListener("install",function(e){self.skipWaiting()});
self.addEventListener("activate",function(e){e.waitUntil(caches.keys().then(function(k){
return Promise.all(k.filter(function(x){return x!==C}).map(function(x){return caches.delete(x)}))}).then(function(){return self.clients.claim()}))});
self.addEventListener("fetch",function(e){var u=new URL(e.request.url);
if(e.request.method!=="GET"||u.origin!==location.origin||u.pathname.indexOf("/api/")===0||u.pathname==="/health"||u.pathname==="/sw.js")return;
e.respondWith(caches.open(C).then(function(cache){return cache.match(e.request).then(function(hit){
var net=fetch(e.request).then(function(r){if(r&&r.ok)cache.put(e.request,r.clone());return r}).catch(function(){return hit});
e.waitUntil(net);return hit||net})}))});
"""


@lru_cache(maxsize=4)
def make_icon_png(size: int) -> bytes:
    """App icon (planet on dark background) generated with the stdlib - no image files needed."""
    bg, blue, green = (11, 16, 32), (79, 124, 255), (52, 199, 120)
    cx = cy = size / 2
    r = size * 0.30                       # inside the 'maskable' safe zone
    gx, gy, gr = cx - r * 0.25, cy - r * 0.15, r * 0.45
    rows = []
    for y in range(size):
        row = bytearray([0])
        for x in range(size):
            if (x - cx) ** 2 + (y - cy) ** 2 <= r * r:
                row.extend(green if (x - gx) ** 2 + (y - gy) ** 2 <= gr * gr else blue)
            else:
                row.extend(bg)
        rows.append(bytes(row))

    def chunk(tag: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)

    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(b"".join(rows), 9)) + chunk(b"IEND", b""))


@app.route("/manifest.webmanifest")
def manifest():
    m = {"name": "Space2Earth", "short_name": "Space2Earth",
         "description": "NASA POWER decision support: crop water, crop stress, urban heat",
         "start_url": "/", "scope": "/", "display": "standalone",
         "background_color": "#0b1020", "theme_color": "#0b1020",
         "icons": [{"src": "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any maskable"},
                   {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any maskable"}]}
    return app.response_class(json.dumps(m), mimetype="application/manifest+json")


@app.route("/sw.js")
def service_worker():
    return app.response_class(SW_JS, mimetype="application/javascript",
                              headers={"Service-Worker-Allowed": "/", "Cache-Control": "no-cache"})


@app.route("/icon-<int:size>.png")
def icon_png(size):
    if size not in (192, 512):
        return jsonify({"status": "error", "detail": "Not found"}), 404
    resp = app.response_class(make_icon_png(size), mimetype="image/png")
    resp.headers["Cache-Control"] = "public, max-age=86400"
    return resp


@app.errorhandler(RecursionError)
def _too_deep(_):
    return jsonify({"status": "error", "detail": "JSON is nested too deeply"}), 400


@app.errorhandler(404)
def _nf(_):
    return jsonify({"status": "error", "detail": "Not found"}), 404


@app.errorhandler(405)
def _na(_):
    return jsonify({"status": "error", "detail": "Method not allowed"}), 405


@app.errorhandler(413)
def _big(_):
    return jsonify({"status": "error", "detail": "Request too large"}), 413


@app.errorhandler(500)
def _err(exc):
    log.exception("Unhandled error: %s", exc)
    return jsonify({"status": "error", "detail": "Internal server error"}), 500


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en" dir="ltr"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Space2Earth</title>
<link rel="manifest" href="/manifest.webmanifest"><meta name="theme-color" content="#0b1020">
<link rel="icon" href="/icon-192.png"><link rel="apple-touch-icon" href="/icon-192.png">
<style nonce="__NONCE__">
*{box-sizing:border-box}body{margin:0;font-family:system-ui,-apple-system,"Noto Nastaliq Urdu","Jameel Noori Nastaleeq","Noto Naskh Arabic",sans-serif;background:#0b1020;color:#e7eaf3}
[dir=rtl] body{line-height:1.9}[dir=rtl] ul{padding-right:18px;padding-left:0}
header{padding:20px 24px;border-bottom:1px solid #1f2540;display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap}
header h1{margin:0;font-size:22px}header p{margin:4px 0 0;color:#8b93b0;font-size:13px}
main{max-width:920px;margin:0 auto;padding:20px}.hide{display:none}
.tabs{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px}
.tab{padding:8px 14px;border-radius:8px;background:#141a30;border:1px solid #1f2540;color:#8b93b0;cursor:pointer;font-size:14px}
.tab.active{background:#4f7cff;color:#fff;border-color:#4f7cff}
form{background:#141a30;border:1px solid #1f2540;padding:16px;border-radius:12px;margin-bottom:18px}
.row{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:10px}.row>div{flex:1;min-width:140px}.row>div.fit{flex:0 0 auto;min-width:0;align-self:flex-end}.hits button{margin:0 6px 6px 0}
label{display:block;font-size:12px;color:#8b93b0;margin-bottom:4px}
input,select,textarea{width:100%;padding:10px;border-radius:8px;border:1px solid #2a3155;background:#0b1020;color:#e7eaf3;font-size:14px}
input[type=number]{direction:ltr;text-align:left}
button{padding:10px 16px;border:0;border-radius:8px;background:#4f7cff;color:#fff;font-weight:600;cursor:pointer;font-size:14px}
button.ghost{background:#222a4a}button:disabled{opacity:.5}
.card{background:#141a30;border:1px solid #1f2540;border-radius:12px;padding:16px;margin-bottom:14px}
.card h3{margin:0 0 10px;font-size:15px}.badge{display:inline-block;padding:4px 12px;border-radius:99px;font-weight:700;font-size:13px}
.Low{background:#1f6f43}.Moderate{background:#a8741a}.High{background:#b3342f}.Insufficient{background:#444b66}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:10px}
.m{background:#0b1020;border:1px solid #1f2540;border-radius:8px;padding:10px}.m b{display:block;font-size:11px;color:#8b93b0;text-transform:uppercase;margin-bottom:4px}
.m span{font-size:14px;word-break:break-word}ul{margin:0;padding-left:18px;line-height:1.6;font-size:14px}
.small{font-size:12px;color:#8b93b0}.err{color:#ff8f8f}svg{width:100%;height:70px;background:#0b1020;border-radius:8px}
.card p{line-height:1.7;margin:0 0 10px}
.mosaic{display:grid;grid-template-columns:repeat(3,1fr);position:relative;max-width:480px;margin:8px auto;background:#0b1020;direction:ltr}
.mosaic>img,.mosaic>.tile{width:100%;aspect-ratio:1;display:block;background:#0b1020}
.pin{position:absolute;width:14px;height:14px;border:3px solid #ff3b3b;border-radius:50%;transform:translate(-50%,-50%);box-shadow:0 0 0 2px #fff}
.satsel{width:auto}
.scroll{overflow-x:auto}textarea{font-family:monospace;direction:ltr}#valfile{margin-top:8px}table{border-collapse:collapse;width:100%;font-size:13px}th,td{border:1px solid #1f2540;padding:6px;text-align:start}th{color:#8b93b0;font-weight:600}
.cal{display:grid;grid-template-columns:repeat(12,1fr);gap:2px;margin:6px 0;direction:ltr}.cm{text-align:center;padding:6px 0;font-size:11px;border-radius:4px}.cm.crop{background:#1f6f43}.cm.bare{background:#b3342f}
.card h4{margin:10px 0 4px;font-size:13px;color:#8b93b0}
@media print{body{background:#fff;color:#000}.card,.m,form{background:#fff;border-color:#999}button,form,.tabs,header>div:last-child{display:none}.small,.m b{color:#444}}
</style></head><body>
<header><div><h1 data-k="title">Space2Earth</h1><p data-k="sub"></p></div>
<div><button type="button" class="ghost hide" id="inst" data-k="install"></button> <button type="button" class="ghost" id="lang"></button></div></header>
<main>
<div class="tabs" id="tabs"></div>
<form id="f">
<div class="row"><div><label data-k="place" for="q"></label><input id="q" list="places" maxlength="60" autocomplete="off"></div><div class="fit"><button type="button" id="find" data-k="find"></button></div></div><div id="hits" class="hits"></div><datalist id="places"></datalist>
<div class="row"><div><label data-k="lat" for="lat"></label><input id="lat" type="number" step="0.0001" min="-90" max="90" value="34.0151" required></div>
<div><label data-k="lon" for="lon"></label><input id="lon" type="number" step="0.0001" min="-180" max="180" value="71.5249" required></div>
<div><label data-k="days" for="days"></label><input id="days" type="number" min="7" max="90" value="30"></div></div>
<div class="row"><div id="cropbox"><label data-k="crop" for="crop"></label><input id="crop" type="text" maxlength="50" value="wheat"></div>
<div id="kcbox"><label data-k="kc" for="kc"></label><input id="kc" type="number" step="0.01" min="0.1" max="1.5"></div>
<div id="soilbox"><label data-k="soil" for="soil"></label><select id="soil"><option>sand</option><option>sandy loam</option><option selected>loam</option><option>silt loam</option><option>clay</option></select></div>
<div id="rotbox"><div class="row"><div><label data-k="rot_rabi"></label><select id="rabi"><option value="wheat">Wheat / گندم</option><option value="mustard">Mustard / سرسوں</option><option value="chickpea">Chickpea / چنا</option><option value="berseem">Berseem fodder / برسیم</option><option value="fallow">Fallow (nothing) / خالی زمین</option></select></div>
<div><label data-k="rot_kharif"></label><select id="kharif"><option value="maize">Maize / مکئی</option><option value="rice">Rice / چاول</option><option value="mungbean">Mung bean / مونگ</option><option value="sorghum">Sorghum fodder / جوار</option><option value="fallow">Fallow (nothing) / خالی زمین</option></select></div>
<div><label data-k="rot_water"></label><select id="water"><option value="rainfed">Rainfed only / صرف بارانی</option><option value="limited">Limited irrigation / محدود آبپاشی</option><option value="good">Good irrigation / اچھی آبپاشی</option></select></div></div>
<div class="row"><div><label data-k="pr_water"></label><input type="range" id="pr_water" min="0" max="3" step="1" value="2"></div><div><label data-k="pr_soil"></label><input type="range" id="pr_soil" min="0" max="3" step="1" value="2"></div><div><label data-k="pr_erosion"></label><input type="range" id="pr_erosion" min="0" max="3" step="1" value="2"></div><div><label data-k="pr_forage"></label><input type="range" id="pr_forage" min="0" max="3" step="1" value="1"></div><div><label data-k="pr_income"></label><input type="range" id="pr_income" min="0" max="3" step="1" value="1"></div><div><label data-k="pr_resilience"></label><input type="range" id="pr_resilience" min="0" max="3" step="1" value="2"></div><div><label data-k="pr_pest"></label><input type="range" id="pr_pest" min="0" max="3" step="1" value="1"></div></div>
<div class="row"><div><label data-k="rot_rain"></label><input id="rainchg" type="number" min="-50" max="50" step="5" value="0"></div>
<div><label data-k="rot_temp"></label><input id="tempchg" type="number" min="0" max="6" step="0.5" value="0"></div></div></div>
<div id="regbox"><label data-k="region" for="region"></label><input id="region" type="text" maxlength="100" value="Peshawar"></div></div>
<div id="ndvibox"><label data-k="ndvi_csv" for="ndvicsv"></label><textarea id="ndvicsv" rows="6" placeholder="10-16-2026&#9;0.3321&#9;1&#10;09-30-2026&#9;0.3091&#9;1"></textarea>
<input type="file" id="ndvifile" accept=".csv,.txt,text/csv,text/plain"><p class="small" data-k="ndvi_help"></p></div>
<div id="valbox"><label data-k="val_csv" for="valcsv"></label><textarea id="valcsv" rows="6" placeholder="date,tmax,tmin,precip&#10;2026-10-01,31.2,16.0,0"></textarea>
<input type="file" id="valfile" accept=".csv,text/csv"><p class="small" data-k="val_help"></p></div>
<button type="submit" id="go" data-k="go"></button> <button type="button" class="ghost" id="geo" data-k="geo"></button>
</form><div id="out" aria-live="polite"></div><p class="small" data-k="indep"></p></main>
<script nonce="__NONCE__">
(function(){
var T={en:{title:"Space2Earth",sub:"NASA POWER data + a clearly labelled live forecast layer \u00b7 computed indicators only \u00b7 advisory, not field-level advice",
m_agrispace:"AgriSpace - water balance & drought",m_bioshield:"BioShield - crop heat/cold & UV",m_ecoguard:"EcoGuard - urban heat & climate",
lat:"Latitude",lon:"Longitude",days:"Window (days)",crop:"Crop (wheat, maize, rice, cotton, sugarcane, tomato, potato)",kc:"Kc override (optional)",
region:"Region label",go:"Analyze",geo:"Use my location",install:"Install app",hint:"Pick a module and press Analyze.",
loading:"Querying NASA POWER (first request can take ~10-20 s)...",risk:"Risk level",win:"Window",lastv:"last valid day",why:"Why",metrics:"Metrics",
series:"Daily series",advisory:"Advisory",warnings:"Warnings",method:"Method",dq:"Data quality",limits:"Limitations",download:"Download JSON",
coverage:"Coverage",missing:"Missing parameters",clim:"Climatology baseline",none:"none",yes:"yes",no:"no",error:"Error",failed:"Request failed",nodata:"no data",
lv_Low:"Low",lv_Moderate:"Moderate",lv_High:"High",lv_Insufficient_data:"Insufficient data",note:"",to:"to",summary:"Plain-language summary",share:"Share summary",print:"Print / Save PDF",saved:"Saved result (press Analyze for fresh data). Location:",soil:"Soil type (assumed)",place:"Search a place (village or town)",find:"Search",searching:"Searching...",noplace:"No place found. Try another spelling or enter coordinates.",placeoffline:"Place search needs internet. Choose a built-in city or enter coordinates.",listen:"Listen",novoice:"No voice for this language is installed on this device. Try the English summary or install an Urdu voice in the phone text-to-speech settings.",stale:"Could not refresh (no internet or service problem) - showing your last saved result. Location:",age:"data age (days)",m_live:"Live now & 7-day outlook",m_rotation:"Crop rotation explorer",rot_rabi:"Current winter crop (rabi)",rot_kharif:"Current summer crop (kharif)",rot_water:"Irrigation water",pr_water:"Priority: save water (0-3)",pr_soil:"Priority: soil health (0-3)",pr_erosion:"Priority: less bare soil/erosion (0-3)",pr_forage:"Priority: fodder value (0-3)",pr_income:"Priority: market income (0-3)",pr_resilience:"Priority: low heat/frost risk (0-3)",pr_pest:"Priority: break pest cycles (0-3)",rot_rain:"What-if: rainfall change (%)",rot_temp:"What-if: warming (C)",rot_current:"Current rotation",rot_alt:"Alternative",rot_cmp:"Comparison (0-100, higher is better, star = your current rotation)",rot_name:"Rotation",rot_overall:"Overall",cr_water:"Water",cr_soil:"Soil",cr_erosion:"Erosion",cr_forage:"Fodder",cr_income:"Income*",cr_resilience:"Climate",cr_pest:"Pests",rot_irr:"Irrigation need (mm/yr)",rot_incnote:"*Income is neutral: no market prices are used (the API accepts your own prices).",rot_cal:"Month by month (red = bare soil)",rot_why:"Why it was suggested",rot_ev:"Evidence (NASA POWER climate)",rot_ben:"Possible benefits",rot_risk:"Risks to check locally",rot_gaps:"Information gaps",rot_stab:"Rank stability (share of small priority changes that keep the top choice)",rot_clim:"Local climate (NASA POWER, monthly means)",rot_trend:"Long-term change",decade:"decade",rot_trendnote:"* = statistically clear; otherwise within normal year-to-year variation",m_validate:"Validate with your data",m_ndvi:"Satellite greenness (NDVI) from your NASA AppEEARS file",ndvi_csv:"NASA AppEEARS NDVI file (CSV, or the table copied from the AppEEARS Explore page)",ndvi_help:"Get it free: appeears.earthdatacloud.nasa.gov > Extract > Point Samples > layer _250m_16_days_NDVI (MOD13Q1), then use Explore or download the CSV. One point per file, max 200 KB. Choose a farm field, not a city centre; request Jan-Dec to see bare months.",ndvi_none:"Paste or choose your AppEEARS NDVI file first.",val_csv:"Your daily observations (CSV: date,tmax,tmin,tmean,precip)",val_help:"Paste CSV text or choose a file (max 3 years, 200 KB). Temperatures in C, rain in mm/day. They are compared with NASA POWER at the latitude/longitude above.",val_none:"Paste or choose a CSV first.",val_big:"File too large (max 200 KB).",val_n:"Days",val_bias:"Bias (NASA minus yours)",val_interp:"What the numbers say",datatype:"Data type",indep_h:"Independence and AI use",indep:"Independent project: not affiliated with or endorsed by NASA. Data services: NASA POWER and NASA GIBS (NASA), Open-Meteo (forecast, place names), OpenStreetMap (place search). Built with AI assistance (Claude).",sat:"NASA satellite image (context only)",
satnote:"Imagery: NASA EOSDIS GIBS (yesterday). Clouds may hide the ground. This is a picture, not a crop-health measurement.",
satfail:"Satellite image not available for this place/date right now."}};
T.ur={title:"\u0627\u0633\u067e\u06cc\u0633 \u0679\u0648 \u0627\u0631\u062a\u06be",
sub:"\u0646\u0627\u0633\u0627 \u067e\u0627\u0648\u0631 \u06a9\u0627 \u0688\u06cc\u0679\u0627 \u0627\u0648\u0631 \u0648\u0627\u0636\u062d \u0637\u0648\u0631 \u067e\u0631 \u0646\u0634\u0627\u0646 \u0632\u062f \u0644\u0627\u0626\u06cc\u0648 \u067e\u06cc\u0634 \u06af\u0648\u0626\u06cc \xb7 \u0635\u0631\u0641 \u062d\u0633\u0627\u0628 \u0634\u062f\u06c1 \u0627\u0634\u0627\u0631\u06cc\u06d2 \xb7 \u0645\u0634\u0648\u0631\u06c1\u060c \u06a9\u06be\u06cc\u062a \u06a9\u06cc \u0633\u0637\u062d \u06a9\u06cc \u06c1\u062f\u0627\u06cc\u062a \u0646\u06c1\u06cc\u06ba",
m_agrispace:"\u0627\u06cc\u06af\u0631\u06cc \u0627\u0633\u067e\u06cc\u0633 - \u067e\u0627\u0646\u06cc \u06a9\u0627 \u062a\u0648\u0627\u0632\u0646 \u0627\u0648\u0631 \u062e\u0634\u06a9 \u0633\u0627\u0644\u06cc",
m_bioshield:"\u0628\u0627\u06cc\u0648 \u0634\u06cc\u0644\u0688 - \u0641\u0635\u0644 \u067e\u0631 \u06af\u0631\u0645\u06cc/\u0633\u0631\u062f\u06cc \u0627\u0648\u0631 UV",
m_ecoguard:"\u0627\u06cc\u06a9\u0648 \u06af\u0627\u0631\u0688 - \u0634\u06c1\u0631\u06cc \u06af\u0631\u0645\u06cc \u0627\u0648\u0631 \u0645\u0648\u0633\u0645",
lat:"\u0639\u0631\u0636 \u0628\u0644\u062f (Latitude)",lon:"\u0637\u0648\u0644 \u0628\u0644\u062f (Longitude)",days:"\u062f\u0646\u0648\u06ba \u06a9\u06cc \u0645\u062f\u062a",
crop:"\u0641\u0635\u0644 (\u06af\u0646\u062f\u0645\u060c \u0645\u06a9\u0626\u06cc\u060c \u0686\u0627\u0648\u0644\u060c \u06a9\u067e\u0627\u0633\u060c \u06af\u0646\u0627\u060c \u0679\u0645\u0627\u0679\u0631\u060c \u0622\u0644\u0648)",
kc:"Kc (\u0627\u062e\u062a\u06cc\u0627\u0631\u06cc)",region:"\u0639\u0644\u0627\u0642\u06d2 \u06a9\u0627 \u0646\u0627\u0645",go:"\u062a\u062c\u0632\u06cc\u06c1 \u06a9\u0631\u06cc\u06ba",
geo:"\u0645\u06cc\u0631\u06cc \u0644\u0648\u06a9\u06cc\u0634\u0646",install:"\u0627\u06cc\u067e \u0627\u0646\u0633\u0679\u0627\u0644 \u06a9\u0631\u06cc\u06ba",
hint:"\u0645\u0627\u0688\u06cc\u0648\u0644 \u0686\u0646\u06cc\u06ba \u0627\u0648\u0631 \u062a\u062c\u0632\u06cc\u06c1 \u062f\u0628\u0627\u0626\u06cc\u06ba\u06d4",
loading:"\u0646\u0627\u0633\u0627 \u067e\u0627\u0648\u0631 \u0633\u06d2 \u0688\u06cc\u0679\u0627 \u0644\u06cc\u0627 \u062c\u0627 \u0631\u06c1\u0627 \u06c1\u06d2 (\u067e\u06c1\u0644\u06cc \u0628\u0627\u0631 10-20 \u0633\u06cc\u06a9\u0646\u0688 \u0644\u06af \u0633\u06a9\u062a\u06d2 \u06c1\u06cc\u06ba)...",
risk:"\u062e\u0637\u0631\u06d2 \u06a9\u06cc \u0633\u0637\u062d",win:"\u0645\u062f\u062a",lastv:"\u0622\u062e\u0631\u06cc \u062f\u0631\u0633\u062a \u062f\u0646",
why:"\u0648\u062c\u0648\u06c1\u0627\u062a",metrics:"\u0627\u0639\u062f\u0627\u062f \u0648 \u0634\u0645\u0627\u0631",series:"\u0631\u0648\u0632\u0627\u0646\u06c1 \u0631\u062c\u062d\u0627\u0646",
advisory:"\u0645\u0634\u0648\u0631\u06c1",warnings:"\u0627\u0646\u062a\u0628\u0627\u06c1",method:"\u0637\u0631\u06cc\u0642\u06c1\u0654 \u06a9\u0627\u0631",
dq:"\u0688\u06cc\u0679\u0627 \u06a9\u0627 \u0645\u0639\u06cc\u0627\u0631",limits:"\u062d\u062f\u0648\u062f",download:"JSON \u0688\u0627\u0624\u0646 \u0644\u0648\u0688 \u06a9\u0631\u06cc\u06ba",
coverage:"\u06a9\u0648\u0631\u06cc\u062c",missing:"\u063a\u0627\u0626\u0628 \u067e\u06cc\u0631\u0627\u0645\u06cc\u0679\u0631",clim:"\u0645\u0648\u0633\u0645\u06cc\u0627\u062a\u06cc \u0628\u06cc\u0633 \u0644\u0627\u0626\u0646",
none:"\u06a9\u0648\u0626\u06cc \u0646\u06c1\u06cc\u06ba",yes:"\u06c1\u0627\u06ba",no:"\u0646\u06c1\u06cc\u06ba",error:"\u062e\u0631\u0627\u0628\u06cc",
failed:"\u062f\u0631\u062e\u0648\u0627\u0633\u062a \u0646\u0627\u06a9\u0627\u0645",nodata:"\u0688\u06cc\u0679\u0627 \u0646\u06c1\u06cc\u06ba",
lv_Low:"\u06a9\u0645",lv_Moderate:"\u062f\u0631\u0645\u06cc\u0627\u0646\u06cc",lv_High:"\u0632\u06cc\u0627\u062f\u06c1",lv_Insufficient_data:"\u0688\u06cc\u0679\u0627 \u0646\u0627\u06a9\u0627\u0641\u06cc",
note:"\u0646\u0648\u0679: \u062a\u0641\u0635\u06cc\u0644\u06cc \u0648\u062c\u0648\u06c1\u0627\u062a\u060c \u0645\u0634\u0648\u0631\u06d2 \u0627\u0648\u0631 \u062d\u062f\u0648\u062f \u0627\u0628\u06be\u06cc \u0627\u0646\u06af\u0631\u06cc\u0632\u06cc \u0645\u06cc\u06ba \u06c1\u06cc\u06ba\u06d4",to:"\u0633\u06d2",summary:"\u0622\u0633\u0627\u0646 \u0632\u0628\u0627\u0646 \u0645\u06cc\u06ba \u062e\u0644\u0627\u0635\u06c1",share:"\u062e\u0644\u0627\u0635\u06c1 \u0634\u06cc\u0626\u0631 \u06a9\u0631\u06cc\u06ba",print:"\u067e\u0631\u0646\u0679 / PDF \u0645\u062d\u0641\u0648\u0638 \u06a9\u0631\u06cc\u06ba",saved:"\u0645\u062d\u0641\u0648\u0638 \u0634\u062f\u06c1 \u0646\u062a\u06cc\u062c\u06c1 (\u062a\u0627\u0632\u06c1 \u0688\u06cc\u0679\u0627 \u06a9\u06d2 \u0644\u06cc\u06d2 \u062a\u062c\u0632\u06cc\u06c1 \u062f\u0628\u0627\u0626\u06cc\u06ba)\u06d4 \u0645\u0642\u0627\u0645:",age:"\u0688\u06cc\u0679\u0627 \u06a9\u06cc \u0639\u0645\u0631 (\u062f\u0646)",m_live:"\u0644\u0627\u0626\u06cc\u0648 \u062d\u0627\u0644\u0627\u062a \u0627\u0648\u0631 7 \u062f\u0646 \u06a9\u06cc \u067e\u06cc\u0634 \u06af\u0648\u0626\u06cc",soil:"\u0645\u0679\u06cc \u06a9\u06cc \u0642\u0633\u0645 (\u0641\u0631\u0636\u06cc)",place:"\u062c\u06af\u06c1 \u062a\u0644\u0627\u0634 \u06a9\u0631\u06cc\u06ba (\u06af\u0627\u0624\u06ba/\u0634\u06c1\u0631)",find:"\u062a\u0644\u0627\u0634",searching:"\u062a\u0644\u0627\u0634 \u062c\u0627\u0631\u06cc \u06c1\u06d2...",noplace:"\u06a9\u0648\u0626\u06cc \u062c\u06af\u06c1 \u0646\u06c1\u06cc\u06ba \u0645\u0644\u06cc\u06d4 \u06c1\u062c\u06d2 \u0628\u062f\u0644 \u06a9\u0631 \u062f\u06cc\u06a9\u06be\u06cc\u06ba \u06cc\u0627 \u062e\u0648\u062f \u0639\u0631\u0636 \u0628\u0644\u062f/\u0637\u0648\u0644 \u0628\u0644\u062f \u0644\u06a9\u06be\u06cc\u06ba\u06d4",placeoffline:"\u062c\u06af\u06c1 \u06a9\u06cc \u062a\u0644\u0627\u0634 \u06a9\u06d2 \u0644\u06cc\u06d2 \u0627\u0646\u0679\u0631\u0646\u06cc\u0679 \u0686\u0627\u06c1\u06cc\u06d2\u06d4 \u0641\u06c1\u0631\u0633\u062a \u0645\u06cc\u06ba \u0633\u06d2 \u0634\u06c1\u0631 \u0686\u0646\u06cc\u06ba \u06cc\u0627 \u062e\u0648\u062f \u0639\u0631\u0636 \u0628\u0644\u062f/\u0637\u0648\u0644 \u0628\u0644\u062f \u0644\u06a9\u06be\u06cc\u06ba\u06d4",listen:"\u0633\u0646\u06cc\u06ba",novoice:"\u0627\u0633 \u0641\u0648\u0646 \u0645\u06cc\u06ba \u0627\u0633 \u0632\u0628\u0627\u0646 \u06a9\u06cc \u0622\u0648\u0627\u0632 \u0645\u0648\u062c\u0648\u062f \u0646\u06c1\u06cc\u06ba\u06d4 \u0627\u0646\u06af\u0631\u06cc\u0632\u06cc \u062e\u0644\u0627\u0635\u06c1 \u0622\u0632\u0645\u0627\u0626\u06cc\u06ba \u06cc\u0627 \u0641\u0648\u0646 \u06a9\u06cc \u0679\u06cc\u06a9\u0633\u0679 \u0679\u0648 \u0627\u0633\u067e\u06cc\u0686 \u0633\u06cc\u0679\u0646\u06af\u0632 \u0645\u06cc\u06ba \u0627\u0631\u062f\u0648 \u0622\u0648\u0627\u0632 \u0627\u0646\u0633\u0679\u0627\u0644 \u06a9\u0631\u06cc\u06ba\u06d4",stale:"\u062a\u0627\u0632\u06c1 \u0688\u06cc\u0679\u0627 \u0646\u06c1\u06cc\u06ba \u0645\u0644 \u0633\u06a9\u0627 (\u0627\u0646\u0679\u0631\u0646\u06cc\u0679 \u06cc\u0627 \u0633\u0631\u0648\u0633 \u06a9\u0627 \u0645\u0633\u0626\u0644\u06c1) - \u0622\u067e \u06a9\u0627 \u0622\u062e\u0631\u06cc \u0645\u062d\u0641\u0648\u0638 \u0646\u062a\u06cc\u062c\u06c1 \u062f\u06a9\u06be\u0627\u06cc\u0627 \u062c\u0627 \u0631\u06c1\u0627 \u06c1\u06d2\u06d4 \u0645\u0642\u0627\u0645:",sat:"\u0646\u0627\u0633\u0627 \u06a9\u06cc \u0633\u06cc\u0679\u0644\u0627\u0626\u0679 \u062a\u0635\u0648\u06cc\u0631 (\u0635\u0631\u0641 \u0633\u0645\u062c\u06be\u0646\u06d2 \u06a9\u06d2 \u0644\u06cc\u06d2)",satnote:"\u062a\u0635\u0648\u06cc\u0631: \u0646\u0627\u0633\u0627 GIBS (\u06a9\u0644 \u06a9\u06cc)\u06d4 \u0628\u0627\u062f\u0644 \u0632\u0645\u06cc\u0646 \u0686\u06be\u067e\u0627 \u0633\u06a9\u062a\u06d2 \u06c1\u06cc\u06ba\u06d4 \u06cc\u06c1 \u0635\u0631\u0641 \u062a\u0635\u0648\u06cc\u0631 \u06c1\u06d2\u060c \u0641\u0635\u0644 \u06a9\u06cc \u0635\u062d\u062a \u06a9\u06cc \u067e\u06cc\u0645\u0627\u0626\u0634 \u0646\u06c1\u06cc\u06ba\u06d4",satfail:"\u0627\u0633 \u062c\u06af\u06c1/\u062a\u0627\u0631\u06cc\u062e \u06a9\u06d2 \u0644\u06cc\u06d2 \u0633\u06cc\u0679\u0644\u0627\u0626\u0679 \u062a\u0635\u0648\u06cc\u0631 \u0627\u0628\u06be\u06cc \u062f\u0633\u062a\u06cc\u0627\u0628 \u0646\u06c1\u06cc\u06ba\u06d4",m_rotation:"\u0641\u0635\u0644\u0648\u06ba \u06a9\u06cc \u06af\u0631\u062f\u0634 (\u0631\u0648\u0679\u06cc\u0634\u0646) \u06a9\u0627 \u062c\u0627\u0626\u0632\u06c1",rot_rabi:"\u0645\u0648\u062c\u0648\u062f\u06c1 \u0633\u0631\u062f\u06cc\u0648\u06ba \u06a9\u06cc \u0641\u0635\u0644 (\u0631\u0628\u06cc\u0639)",rot_kharif:"\u0645\u0648\u062c\u0648\u062f\u06c1 \u06af\u0631\u0645\u06cc\u0648\u06ba \u06a9\u06cc \u0641\u0635\u0644 (\u062e\u0631\u06cc\u0641)",rot_water:"\u0622\u0628\u067e\u0627\u0634\u06cc \u06a9\u0627 \u067e\u0627\u0646\u06cc",pr_water:"\u062a\u0631\u062c\u06cc\u062d: \u067e\u0627\u0646\u06cc \u0628\u0686\u0627\u0646\u0627 (0-3)",pr_soil:"\u062a\u0631\u062c\u06cc\u062d: \u0645\u0679\u06cc \u06a9\u06cc \u0635\u062d\u062a (0-3)",pr_erosion:"\u062a\u0631\u062c\u06cc\u062d: \u06a9\u0645 \u06a9\u06be\u0644\u06cc \u0632\u0645\u06cc\u0646/\u06a9\u0679\u0627\u0624 (0-3)",pr_forage:"\u062a\u0631\u062c\u06cc\u062d: \u0686\u0627\u0631\u06d2 \u06a9\u06cc \u0642\u062f\u0631 (0-3)",pr_income:"\u062a\u0631\u062c\u06cc\u062d: \u0645\u0646\u0688\u06cc \u0633\u06d2 \u0622\u0645\u062f\u0646 (0-3)",pr_resilience:"\u062a\u0631\u062c\u06cc\u062d: \u06af\u0631\u0645\u06cc/\u067e\u0627\u0644\u06d2 \u06a9\u0627 \u06a9\u0645 \u062e\u0637\u0631\u06c1 (0-3)",pr_pest:"\u062a\u0631\u062c\u06cc\u062d: \u06a9\u06cc\u0691\u0648\u06ba \u06a9\u0627 \u0686\u06a9\u0631 \u062a\u0648\u0691\u0646\u0627 (0-3)",rot_rain:"\u0627\u06af\u0631 \u0627\u06cc\u0633\u0627 \u06c1\u0648: \u0628\u0627\u0631\u0634 \u0645\u06cc\u06ba \u062a\u0628\u062f\u06cc\u0644\u06cc (%)",rot_temp:"\u0627\u06af\u0631 \u0627\u06cc\u0633\u0627 \u06c1\u0648: \u062f\u0631\u062c\u06c2 \u062d\u0631\u0627\u0631\u062a \u0645\u06cc\u06ba \u0627\u0636\u0627\u0641\u06c1",rot_current:"\u0645\u0648\u062c\u0648\u062f\u06c1 \u06af\u0631\u062f\u0634",rot_alt:"\u0645\u062a\u0628\u0627\u062f\u0644",rot_cmp:"\u0645\u0648\u0627\u0632\u0646\u06c1 (0-100\u060c \u0632\u06cc\u0627\u062f\u06c1 \u0628\u06c1\u062a\u0631\u061b \u0633\u062a\u0627\u0631\u06c1 = \u0622\u067e \u06a9\u06cc \u0645\u0648\u062c\u0648\u062f\u06c1 \u06af\u0631\u062f\u0634)",rot_name:"\u06af\u0631\u062f\u0634",rot_overall:"\u0645\u062c\u0645\u0648\u0639\u06cc",cr_water:"\u067e\u0627\u0646\u06cc",cr_soil:"\u0645\u0679\u06cc",cr_erosion:"\u06a9\u0679\u0627\u0624",cr_forage:"\u0686\u0627\u0631\u06c1",cr_income:"\u0622\u0645\u062f\u0646*",cr_resilience:"\u0645\u0648\u0633\u0645",cr_pest:"\u06a9\u06cc\u0691\u06d2",rot_irr:"\u0622\u0628\u067e\u0627\u0634\u06cc \u06a9\u06cc \u0636\u0631\u0648\u0631\u062a (\u0645\u0644\u06cc \u0645\u06cc\u0679\u0631/\u0633\u0627\u0644)",rot_incnote:"*\u0622\u0645\u062f\u0646 \u063a\u06cc\u0631 \u062c\u0627\u0646\u0628\u062f\u0627\u0631 \u06c1\u06d2: \u0645\u0646\u0688\u06cc \u06a9\u06d2 \u0631\u06cc\u0679 \u0627\u0633\u062a\u0639\u0645\u0627\u0644 \u0646\u06c1\u06cc\u06ba \u06c1\u0648\u0626\u06d2\u06d4",rot_cal:"\u0645\u06c1\u06cc\u0646\u06c1 \u0628\u06c1 \u0645\u06c1\u06cc\u0646\u06c1 (\u0633\u0631\u062e = \u062e\u0627\u0644\u06cc \u0632\u0645\u06cc\u0646)",rot_why:"\u06cc\u06c1 \u06a9\u06cc\u0648\u06ba \u062a\u062c\u0648\u06cc\u0632 \u06c1\u0648\u0626\u06cc",rot_ev:"\u062b\u0628\u0648\u062a (\u0646\u0627\u0633\u0627 \u067e\u0627\u0648\u0631 \u06a9\u0627 \u0645\u0648\u0633\u0645)",rot_ben:"\u0645\u0645\u06a9\u0646\u06c1 \u0641\u0648\u0627\u0626\u062f",rot_risk:"\u0645\u0642\u0627\u0645\u06cc \u0637\u0648\u0631 \u067e\u0631 \u062c\u0627\u0646\u0686\u0646\u06d2 \u06a9\u06d2 \u062e\u0637\u0631\u0627\u062a",rot_gaps:"\u0645\u0639\u0644\u0648\u0645\u0627\u062a \u06a9\u06cc \u06a9\u0645\u06cc",rot_stab:"\u062f\u0631\u062c\u06c1 \u0628\u0646\u062f\u06cc \u06a9\u06cc \u0627\u0633\u062a\u0642\u0627\u0645\u062a (\u062a\u0631\u062c\u06cc\u062d\u0627\u062a \u0645\u06cc\u06ba \u0645\u0639\u0645\u0648\u0644\u06cc \u0631\u062f\u0648\u0628\u062f\u0644 \u067e\u0631 \u067e\u06c1\u0644\u0627 \u0627\u0646\u062a\u062e\u0627\u0628 \u0628\u0631\u0642\u0631\u0627\u0631 \u0631\u06c1\u0646\u06d2 \u06a9\u0627 \u062a\u0646\u0627\u0633\u0628)",rot_clim:"\u0645\u0642\u0627\u0645\u06cc \u0645\u0648\u0633\u0645 (\u0646\u0627\u0633\u0627 \u067e\u0627\u0648\u0631\u060c \u0645\u0627\u06c1\u0627\u0646\u06c1 \u0627\u0648\u0633\u0637)",rot_trend:"\u0637\u0648\u06cc\u0644 \u0645\u062f\u062a\u06cc \u062a\u0628\u062f\u06cc\u0644\u06cc",decade:"\u062f\u06c1\u0627\u0626\u06cc",rot_trendnote:"* = \u0627\u0639\u062f\u0627\u062f\u06cc \u0644\u062d\u0627\u0638 \u0633\u06d2 \u0648\u0627\u0636\u062d\u061b \u0648\u0631\u0646\u06c1 \u0633\u0627\u0644 \u0628\u06c1 \u0633\u0627\u0644 \u0645\u0639\u0645\u0648\u0644 \u06a9\u06d2 \u0627\u062a\u0627\u0631 \u0686\u0691\u06be\u0627\u0624 \u06a9\u06d2 \u0627\u0646\u062f\u0631",m_validate:"\u0627\u067e\u0646\u06d2 \u0688\u06cc\u0679\u0627 \u0633\u06d2 \u062a\u0635\u062f\u06cc\u0642",val_csv:"\u0622\u067e \u06a9\u06d2 \u0631\u0648\u0632\u0627\u0646\u06c1 \u0645\u0634\u0627\u06c1\u062f\u0627\u062a (CSV: date,tmax,tmin,tmean,precip)",val_help:"CSV \u0645\u062a\u0646 \u067e\u06cc\u0633\u0679 \u06a9\u0631\u06cc\u06ba \u06cc\u0627 \u0641\u0627\u0626\u0644 \u0686\u0646\u06cc\u06ba (\u0632\u06cc\u0627\u062f\u06c1 \u0633\u06d2 \u0632\u06cc\u0627\u062f\u06c1 3 \u0633\u0627\u0644\u060c 200 KB)\u06d4 \u062f\u0631\u062c\u06c2 \u062d\u0631\u0627\u0631\u062a \u0688\u06af\u0631\u06cc \u0633\u06cc\u0644\u0633\u06cc\u0633 \u0645\u06cc\u06ba \u0627\u0648\u0631 \u0628\u0627\u0631\u0634 \u0645\u0644\u06cc \u0645\u06cc\u0679\u0631/\u062f\u0646 \u0645\u06cc\u06ba\u06d4 \u0627\u0646 \u06a9\u0627 \u0645\u0648\u0627\u0632\u0646\u06c1 \u0627\u0648\u067e\u0631 \u062f\u06cc\u06d2 \u06af\u0626\u06d2 \u0639\u0631\u0636 \u0628\u0644\u062f/\u0637\u0648\u0644 \u0628\u0644\u062f \u067e\u0631 \u0646\u0627\u0633\u0627 \u067e\u0627\u0648\u0631 \u0633\u06d2 \u06c1\u0648\u06af\u0627\u06d4",val_none:"\u067e\u06c1\u0644\u06d2 CSV \u067e\u06cc\u0633\u0679 \u06a9\u0631\u06cc\u06ba \u06cc\u0627 \u0641\u0627\u0626\u0644 \u0686\u0646\u06cc\u06ba\u06d4",val_big:"\u0641\u0627\u0626\u0644 \u0628\u06c1\u062a \u0628\u0691\u06cc \u06c1\u06d2 (\u0632\u06cc\u0627\u062f\u06c1 \u0633\u06d2 \u0632\u06cc\u0627\u062f\u06c1 200 KB)\u06d4",val_n:"\u062f\u0646",val_bias:"\u0641\u0631\u0642 (\u0646\u0627\u0633\u0627 \u0645\u0646\u0641\u06cc \u0622\u067e \u06a9\u0627)",val_interp:"\u0627\u0639\u062f\u0627\u062f \u06a9\u06cc\u0627 \u0628\u062a\u0627\u062a\u06d2 \u06c1\u06cc\u06ba",datatype:"\u0688\u06cc\u0679\u0627 \u06a9\u06cc \u0642\u0633\u0645",indep_h:"\u0622\u0632\u0627\u062f\u06cc \u0627\u0648\u0631 \u0645\u0635\u0646\u0648\u0639\u06cc \u0630\u06c1\u0627\u0646\u062a \u06a9\u0627 \u0627\u0633\u062a\u0639\u0645\u0627\u0644",indep:"\u0622\u0632\u0627\u062f \u0645\u0646\u0635\u0648\u0628\u06c1: \u0646\u0627\u0633\u0627 \u0633\u06d2 \u0648\u0627\u0628\u0633\u062a\u06c1 \u0646\u06c1\u06cc\u06ba \u0627\u0648\u0631 \u0646\u06c1 \u0646\u0627\u0633\u0627 \u06a9\u06cc \u062a\u0635\u062f\u06cc\u0642 \u0634\u062f\u06c1\u06d4 \u0688\u06cc\u0679\u0627 \u0633\u0631\u0648\u0633\u0632: \u0646\u0627\u0633\u0627 \u067e\u0627\u0648\u0631 \u0627\u0648\u0631 \u0646\u0627\u0633\u0627 GIBS (\u0646\u0627\u0633\u0627)\u060c Open-Meteo (\u067e\u06cc\u0634 \u06af\u0648\u0626\u06cc\u060c \u062c\u06af\u06c1\u0648\u06ba \u06a9\u06d2 \u0646\u0627\u0645)\u060c OpenStreetMap (\u062c\u06af\u06c1 \u06a9\u06cc \u062a\u0644\u0627\u0634)\u06d4 \u0645\u0635\u0646\u0648\u0639\u06cc \u0630\u06c1\u0627\u0646\u062a (Claude) \u06a9\u06cc \u0645\u062f\u062f \u0633\u06d2 \u0628\u0646\u0627\u06cc\u0627 \u06af\u06cc\u0627\u06d4",m_ndvi:"\u0633\u06cc\u0679\u0644\u0627\u0626\u0679 \u0633\u0628\u0632\u06c1 (NDVI): \u0622\u067e \u06a9\u06cc \u0646\u0627\u0633\u0627 AppEEARS \u0641\u0627\u0626\u0644 \u0633\u06d2",ndvi_csv:"\u0646\u0627\u0633\u0627 AppEEARS \u06a9\u06cc NDVI \u0641\u0627\u0626\u0644 (CSV\u060c \u06cc\u0627 AppEEARS \u06a9\u06d2 Explore \u0635\u0641\u062d\u06d2 \u0633\u06d2 \u06a9\u0627\u067e\u06cc \u06a9\u06cc \u06c1\u0648\u0626\u06cc \u062c\u062f\u0648\u0644)",ndvi_help:"\u0645\u0641\u062a \u062d\u0627\u0635\u0644 \u06a9\u0631\u06cc\u06ba: appeears.earthdatacloud.nasa.gov \u2190 Extract \u2190 Point Samples \u2190 \u0644\u06cc\u0626\u0631 _250m_16_days_NDVI (MOD13Q1)\u060c \u067e\u06be\u0631 Explore \u06cc\u0627 CSV \u0688\u0627\u0624\u0646 \u0644\u0648\u0688\u06d4 \u06c1\u0631 \u0641\u0627\u0626\u0644 \u0645\u06cc\u06ba \u0627\u06cc\u06a9 \u0646\u0642\u0637\u06c1\u060c \u0632\u06cc\u0627\u062f\u06c1 \u0633\u06d2 \u0632\u06cc\u0627\u062f\u06c1 200 KB\u06d4 \u0634\u06c1\u0631 \u06a9\u06d2 \u0645\u0631\u06a9\u0632 \u06a9\u06cc \u0628\u062c\u0627\u0626\u06d2 \u06a9\u06be\u06cc\u062a \u0686\u0646\u06cc\u06ba\u060c \u0627\u0648\u0631 \u062e\u0627\u0644\u06cc \u0645\u06c1\u06cc\u0646\u06d2 \u062f\u06cc\u06a9\u06be\u0646\u06d2 \u06a9\u06d2 \u0644\u06cc\u06d2 \u062c\u0646\u0648\u0631\u06cc \u062a\u0627 \u062f\u0633\u0645\u0628\u0631 \u0645\u0627\u0646\u06af\u06cc\u06ba\u06d4",ndvi_none:"\u067e\u06c1\u0644\u06d2 \u0627\u067e\u0646\u06cc AppEEARS NDVI \u0641\u0627\u0626\u0644 \u067e\u06cc\u0633\u0679 \u06a9\u0631\u06cc\u06ba \u06cc\u0627 \u0686\u0646\u06cc\u06ba\u06d4"};
var lang="en",mod="live",last=null,busy=false;var PLACES=__PLACES__,FEATURES={ndvi:__NDVI__};
try{var sv=localStorage.getItem("s2e_lang");if(sv==="ur"||sv==="en")lang=sv;else if((navigator.language||"").slice(0,2)==="ur")lang="ur"}catch(e){}
function $(i){return document.getElementById(i)}
function t(k){return (T[lang]&&T[lang][k])||T.en[k]||k}
function el(tag,c,x){var e=document.createElement(tag);if(c)e.className=c;if(x!==undefined)e.textContent=x;return e}
function fmt(v){if(v===null||v===undefined)return "n/a";
if(typeof v==="object")return Object.keys(v).map(function(k){return k.replace(/_/g," ")+": "+fmt(v[k])}).join(" | ");return String(v)}
var MODS={live:["cropbox","kcbox","soilbox"],rotation:["rotbox","soilbox"],validate:["valbox"],agrispace:["cropbox","kcbox"],bioshield:["cropbox"],ecoguard:["regbox"],ndvi:["ndvibox"]};
function buildTabs(){var tb=$("tabs");tb.textContent="";Object.keys(MODS).forEach(function(m){
var d=el("div","tab"+(m===mod?" active":""),t("m_"+m));d.onclick=function(){setMod(m)};tb.appendChild(d)})}
function setMod(m){mod=m;["cropbox","kcbox","soilbox","regbox","rotbox","valbox","ndvibox"].forEach(function(b){$(b).style.display=MODS[m].indexOf(b)>=0?"":"none"});buildTabs();if(!busy){showSaved();maybeAuto()}}
var lastAuto=0;
function maybeAuto(){if(mod==="live"&&navigator.onLine&&!busy&&Date.now()-lastAuto>300000){lastAuto=Date.now();setTimeout(function(){$("go").click()},60)}}
function showSaved(key,keepHint){var o=$("out");try{var sv=JSON.parse(localStorage.getItem("s2e_last_"+mod)||"null");
if(sv&&Date.now()-sv.t>6048e5){localStorage.removeItem("s2e_last_"+mod);sv=null}
if(sv&&sv.d&&sv.d.module===mod){last=sv.d;render(sv.d);var n=el("p","small",t(key||"saved")+" "+sv.d.location.latitude+", "+sv.d.location.longitude+" \u00b7 "+new Date(sv.t).toLocaleString());o.insertBefore(n,o.firstChild);return true}}catch(e){}
if(!keepHint){last=null;o.textContent="";o.appendChild(el("p","small",t("hint")))}return false}
function fail(msg){var o=$("out"),er=el("p","err",msg);if(showSaved("stale",true)){o.insertBefore(er,o.firstChild)}else{o.textContent="";o.appendChild(er)}}
function applyLang(){document.documentElement.lang=lang;document.documentElement.dir=lang==="ur"?"rtl":"ltr";document.title=t("title");
Array.prototype.forEach.call(document.querySelectorAll("[data-k]"),function(e){e.textContent=t(e.getAttribute("data-k"))});
$("lang").textContent=lang==="ur"?"English":"\u0627\u0631\u062f\u0648";buildTabs();
if(last)render(last);else if(!busy)showSaved()}
$("lang").onclick=function(){lang=lang==="ur"?"en":"ur";try{localStorage.setItem("s2e_lang",lang)}catch(e){}applyLang()};
function spark(name,vals){var NS="http://www.w3.org/2000/svg",w=300,h=60,wrap=el("div");
wrap.appendChild(el("div","small",name));var v=vals.filter(function(x){return x!==null});
if(!v.length){wrap.appendChild(el("div","small",t("nodata")));return wrap}
var mn=Math.min.apply(null,v),mx=Math.max.apply(null,v);if(mx===mn){mx+=1}
var s=document.createElementNS(NS,"svg");s.setAttribute("viewBox","0 0 "+w+" "+h);s.setAttribute("preserveAspectRatio","none");
var seg=[],cur=[];vals.forEach(function(x,i){if(x===null){if(cur.length)seg.push(cur);cur=[];return}
cur.push((i/(vals.length-1||1)*(w-4)+2).toFixed(1)+","+(h-4-(x-mn)/(mx-mn)*(h-8)).toFixed(1))});if(cur.length)seg.push(cur);
seg.forEach(function(p){var l=document.createElementNS(NS,p.length>1?"polyline":"circle");
if(p.length>1){l.setAttribute("points",p.join(" "));l.setAttribute("fill","none")}else{var c=p[0].split(",");l.setAttribute("cx",c[0]);l.setAttribute("cy",c[1]);l.setAttribute("r","2")}
l.setAttribute("stroke","#6ea0ff");l.setAttribute("stroke-width","2");s.appendChild(l)});wrap.appendChild(s);
wrap.appendChild(el("div","small","min "+mn.toFixed(1)+" / max "+mx.toFixed(1)));return wrap}
var SAT={z:9,layer:"MODIS_Terra_CorrectedReflectance_TrueColor"};
function satCard(d){var lat=Math.max(-85,Math.min(85,d.location.latitude)),lon=d.location.longitude,c=el("div","card");
c.appendChild(el("h3",null,t("sat")));var ctl=el("div"),sel=document.createElement("select");sel.className="satsel";
[["MODIS_Terra_CorrectedReflectance_TrueColor","MODIS Terra"],["VIIRS_SNPP_CorrectedReflectance_TrueColor","VIIRS SNPP"]].forEach(function(o){
var op=document.createElement("option");op.value=o[0];op.textContent=o[1];if(o[0]===SAT.layer)op.selected=true;sel.appendChild(op)});
var mi=el("button","ghost","-"),pl=el("button","ghost","+");mi.type="button";pl.type="button";
ctl.appendChild(sel);ctl.appendChild(document.createTextNode(" "));ctl.appendChild(mi);ctl.appendChild(document.createTextNode(" "));ctl.appendChild(pl);c.appendChild(ctl);
var box=el("div","mosaic"),cap=el("p","small");c.appendChild(box);c.appendChild(cap);c.appendChild(el("p","small",t("satnote")));
function draw(){box.textContent="";var z=SAT.z,n=Math.pow(2,z),day=new Date(Date.now()-86400000).toISOString().slice(0,10),
fx=(lon+180)/360*n,r=lat*Math.PI/180,fy=(1-Math.log(Math.tan(r)+1/Math.cos(r))/Math.PI)/2*n,x0=Math.floor(fx),y0=Math.floor(fy),bad=0,tot=0;
cap.textContent=day+" \u00b7 "+SAT.layer.split("_")[0]+" \u00b7 zoom "+z;
for(var dy=-1;dy<=1;dy++)for(var dx=-1;dx<=1;dx++){var tx=((x0+dx)%n+n)%n,ty=y0+dy;
if(ty<0||ty>=n){box.appendChild(el("div","tile"));continue}
var im=document.createElement("img");im.alt="";tot++;im.onerror=function(){bad++;if(bad===tot)cap.textContent=t("satfail")};
im.src="https://gibs.earthdata.nasa.gov/wmts/epsg3857/best/"+SAT.layer+"/default/"+day+"/GoogleMapsCompatible_Level9/"+z+"/"+ty+"/"+tx+".jpg";box.appendChild(im)}
var pin=el("div","pin");pin.style.left=((1+fx-x0)/3*100)+"%";pin.style.top=((1+fy-y0)/3*100)+"%";box.appendChild(pin)}
sel.onchange=function(){SAT.layer=sel.value;draw()};mi.onclick=function(){SAT.z=Math.max(5,SAT.z-1);draw()};pl.onclick=function(){SAT.z=Math.min(9,SAT.z+1);draw()};
draw();return c}
var MO=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],CRIT=["water","soil","erosion","forage","income","resilience","pest"];
function lst(title,items){var w=el("div");w.appendChild(el("h4",null,title));var u=el("ul");items.forEach(function(i){u.appendChild(el("li",null,i))});w.appendChild(u);return w}
function scls(x){return x>=65?"Low":x>=45?"Moderate":"High"}
function rotTable(d){var w=el("div","scroll"),tb=document.createElement("table"),tr=document.createElement("tr");
["#","rot_name","rot_overall"].concat(CRIT.map(function(k){return "cr_"+k})).concat(["rot_irr"]).forEach(function(h){tr.appendChild(el("th",null,h==="#"?"#":t(h)))});tb.appendChild(tr);
d.strategies.forEach(function(s){var r=document.createElement("tr");r.appendChild(el("td",null,String(s.rank)));
r.appendChild(el("td",null,(s.role==="existing"?"\u2605 ":"")+s.name));r.appendChild(el("td",null,fmt(s.score)));
CRIT.forEach(function(k){r.appendChild(el("td",null,fmt(s.criteria[k])))});r.appendChild(el("td",null,fmt(s.metrics.irrigation_need_mm_per_year)));tb.appendChild(r)});
w.appendChild(tb);return w}
function rotCal(s){var map={};s.rotation.forEach(function(r){r.months.forEach(function(m){if(!map[m])map[m]=r.label})});var g=el("div","cal");
MO.forEach(function(m){var c=map[m],x=el("span","cm "+(c?"crop":"bare"),m.charAt(0));x.title=m+": "+(c||"bare");g.appendChild(x)});return g}
function catLine(d){var cats=d.data_categories||{};return el("p","small",t("datatype")+": "+Object.keys(cats).map(function(k){return cats[k].label}).join(" | "))}
function renderValidation(d){var o=$("out");o.textContent="";var tx=(d.summary&&(d.summary[lang]||d.summary.en))||"",c=el("div","card");
c.appendChild(el("h3",null,t("m_validate")));c.appendChild(el("p",null,tx));c.appendChild(el("p","small",d.period.start+" "+t("to")+" "+d.period.end));
if(lang==="ur")c.appendChild(el("p","small",t("note")));c.appendChild(catLine(d));o.appendChild(c);
var tc=el("div","card"),tw=el("div","scroll"),tb=document.createElement("table"),hr=document.createElement("tr");
["",t("val_n"),t("val_bias"),"MAE","RMSE","r"].forEach(function(h){hr.appendChild(el("th",null,h))});tb.appendChild(hr);
Object.keys(d.results).forEach(function(k){var st=d.results[k],r=document.createElement("tr");
[k+" ("+d.model_variable[k]+")",st.n,st.bias===undefined?"-":st.bias,st.mae===undefined?"-":st.mae,st.rmse===undefined?"-":st.rmse,(st.r===undefined||st.r===null)?"-":st.r].forEach(function(v){r.appendChild(el("td",null,String(v)))});tb.appendChild(r)});
tw.appendChild(tb);tc.appendChild(el("h3",null,t("metrics")));tc.appendChild(tw);o.appendChild(tc);
o.appendChild(section(t("val_interp"),Object.keys(d.interpretation).map(function(k){return k+": "+d.interpretation[k]})));
o.appendChild(section(t("limits"),d.limitations));o.appendChild(section(t("indep_h"),[d.independence.nasa_affiliation,d.independence.ai_assistance]));
var b=el("button","ghost",t("download"));b.onclick=function(){var u=URL.createObjectURL(new Blob([JSON.stringify(d,null,2)],{type:"application/json"}));
var x=document.createElement("a");x.href=u;x.download="space2earth_validation.json";x.click();URL.revokeObjectURL(u)};o.appendChild(b)}
function renderRotation(d){var o=$("out");o.textContent="";var tx=(d.summary&&(d.summary[lang]||d.summary.en))||"",c=el("div","card");
c.appendChild(el("h3",null,t("m_rotation")));c.appendChild(el("p",null,tx));var vn=el("p","small"),lb=el("button","ghost",t("listen")),pb=el("button","ghost",t("print"));
lb.onclick=function(){speak(tx,vn)};pb.onclick=function(){window.print()};c.appendChild(lb);c.appendChild(document.createTextNode(" "));c.appendChild(pb);c.appendChild(vn);
c.appendChild(el("p","small",t("rot_stab")+": "+d.rank_stability_pct+"%"));if(lang==="ur")c.appendChild(el("p","small",t("note")));c.appendChild(catLine(d));o.appendChild(c);
var cl=d.climate,cc=el("div","card");cc.appendChild(el("h3",null,t("rot_clim")));
cc.appendChild(el("p","small",["Rain "+cl.annual_rain_mm+" mm/yr","ET0 "+cl.annual_et0_mm+" mm/yr","Rain/ET0 "+cl.aridity_index_rain_over_et0,cl.hottest_month,cl.coldest_month].join(" \u00b7 ")));
cc.appendChild(spark("rain mm/month",d.series.rain_mm_month));cc.appendChild(spark("ET0 mm/month",d.series.et0_mm_month));
var tr=cl.long_term_trend;if(tr){cc.appendChild(el("p","small",t("rot_trend")+" "+tr.period+": "+tr.temperature_c_per_decade+" C/"+t("decade")+(tr.temperature_significant?" *":"")+" \u00b7 "+(tr.rain_pct_per_decade===null?"n/a":tr.rain_pct_per_decade)+"%/"+t("decade")+(tr.rain_significant?" *":"")+"  ("+t("rot_trendnote")+")"))}
o.appendChild(cc);
var tc=el("div","card");tc.appendChild(el("h3",null,t("rot_cmp")));tc.appendChild(rotTable(d));tc.appendChild(el("p","small",t("rot_incnote")));o.appendChild(tc);
d.strategies.forEach(function(s){var k=el("div","card");k.appendChild(el("h3",null,"#"+s.rank+" "+(s.role==="existing"?t("rot_current"):t("rot_alt"))+": "+s.name));
k.appendChild(el("span","badge "+scls(s.score),t("rot_overall")+": "+s.score));k.appendChild(el("p","small",t("rot_cal")));k.appendChild(rotCal(s));
k.appendChild(lst(t("rot_why"),s.why));k.appendChild(lst(t("rot_ev"),s.evidence));k.appendChild(lst(t("rot_ben"),s.benefits));k.appendChild(lst(t("rot_risk"),s.risks));o.appendChild(k)});
o.appendChild(section(t("rot_gaps"),d.strategies[0].information_gaps));o.appendChild(section(t("limits"),d.limitations));if(d.warnings.length)o.appendChild(section(t("warnings"),d.warnings));
var b=el("button","ghost",t("download"));b.onclick=function(){var u=URL.createObjectURL(new Blob([JSON.stringify(d,null,2)],{type:"application/json"}));
var x=document.createElement("a");x.href=u;x.download="space2earth_rotation.json";x.click();URL.revokeObjectURL(u)};o.appendChild(b)}
function section(title,items){var c=el("div","card");c.appendChild(el("h3",null,title));var u=el("ul");
items.forEach(function(i){u.appendChild(el("li",null,i))});c.appendChild(u);return c}
function render(d){if(d.module==="rotation"){renderRotation(d);return}if(d.module==="validation"){renderValidation(d);return}var o=$("out");o.textContent="";var a=d.assessment,c=el("div","card");
c.appendChild(el("h3",null,t("m_"+d.module)));var lv=a.level.split(" ")[0];
c.appendChild(el("span","badge "+lv,t("risk")+": "+t("lv_"+a.level.replace(/ /g,"_"))));
c.appendChild(el("p","small",t("win")+" "+d.window.start+" "+t("to")+" "+d.window.end+" ("+t("lastv")+": "+(d.window.last_valid_date||"n/a")+(d.window.data_age_days!=null?" \u00b7 "+t("age")+": "+d.window.data_age_days:"")+")"));
if(lang==="ur")c.appendChild(el("p","small",t("note")));c.appendChild(catLine(d));o.appendChild(c);
var sm=el("div","card");sm.appendChild(el("h3",null,t("summary")));var stx=(d.summary&&(d.summary[lang]||d.summary.en))||"";sm.appendChild(el("p",null,stx));
var sb=el("button","ghost",t("share"));sb.onclick=function(){var tx="Space2Earth: "+stx;if(navigator.share){navigator.share({title:"Space2Earth",text:tx}).catch(function(){})}else{window.open("https://wa.me/?text="+encodeURIComponent(tx),"_blank","noopener")}};
var pb=el("button","ghost",t("print"));pb.onclick=function(){window.print()};var lb=el("button","ghost",t("listen")),vm=el("p","small");lb.onclick=function(){speak(stx,vm)};sm.appendChild(lb);sm.appendChild(document.createTextNode(" "));sm.appendChild(sb);sm.appendChild(document.createTextNode(" "));sm.appendChild(pb);sm.appendChild(vm);o.appendChild(sm);o.appendChild(satCard(d));
if(a.reasons.length)o.appendChild(section(t("why"),a.reasons));
var mc=el("div","card");mc.appendChild(el("h3",null,t("metrics")));var g=el("div","grid");
Object.keys(d.metrics).forEach(function(k){var m=el("div","m");m.appendChild(el("b",null,k.replace(/_/g," ")));m.appendChild(el("span",null,fmt(d.metrics[k])));g.appendChild(m)});
mc.appendChild(g);o.appendChild(mc);
var sc=el("div","card");sc.appendChild(el("h3",null,t("series")));Object.keys(d.series).forEach(function(k){if(k!=="dates")sc.appendChild(spark(k.replace(/_/g," "),d.series[k]))});o.appendChild(sc);
if(d.advisory.length)o.appendChild(section(t("advisory"),d.advisory));
if(d.warnings.length)o.appendChild(section(t("warnings"),d.warnings));
o.appendChild(section(t("method"),[a.method]));
var dq=d.data_quality,cov=Object.keys(dq.coverage_pct).map(function(k){return k+" "+dq.coverage_pct[k]+"%"}).join(", ");
o.appendChild(section(t("dq"),[t("coverage")+": "+cov,t("missing")+": "+(dq.missing_parameters.join(", ")||t("none")),t("clim")+": "+(dq.climatology_available?t("yes"):t("no"))]));
o.appendChild(section(t("limits"),d.limitations));
var b=el("button","ghost",t("download"));b.onclick=function(){var u=URL.createObjectURL(new Blob([JSON.stringify(d,null,2)],{type:"application/json"}));
var x=document.createElement("a");x.href=u;x.download="space2earth_"+d.module+".json";x.click();URL.revokeObjectURL(u)};o.appendChild(b)}
function speak(text,vm){vm.textContent="";if(!("speechSynthesis" in window)){vm.textContent=t("novoice");return}
var vs=speechSynthesis.getVoices()||[],want=lang==="ur"?"ur":"en";
if(vs.length&&!vs.some(function(v){return (v.lang||"").toLowerCase().indexOf(want)===0})){vm.textContent=t("novoice");return}
var u=new SpeechSynthesisUtterance(text);u.lang=lang==="ur"?"ur-PK":"en-US";u.rate=.9;speechSynthesis.cancel();speechSynthesis.speak(u)}
function fillPlaces(){var dl=$("places");Object.keys(PLACES).forEach(function(n){var o=document.createElement("option");o.value=n;dl.appendChild(o)})}
function pick(name,lat,lon){$("lat").value=(+lat).toFixed(4);$("lon").value=(+lon).toFixed(4);$("region").value=name;$("q").value=name;$("hits").textContent="";if(mod==="live")$("go").click()}
function search(){var q=$("q").value.trim(),h=$("hits");if(q.length<2)return;
var key=Object.keys(PLACES).filter(function(n){return n.toLowerCase()===q.toLowerCase()})[0];
if(key){pick(key,PLACES[key][0],PLACES[key][1]);return}
h.textContent="";h.appendChild(el("p","small",t("searching")));
fetch("/api/geocode?q="+encodeURIComponent(q)).then(function(r){return r.json().then(function(j){return{ok:r.ok,j:j}})})
.then(function(x){h.textContent="";if(!x.ok){h.appendChild(el("p","err",x.j.detail||t("failed")));return}
if(!x.j.results.length){h.appendChild(el("p","small",t("noplace")));return}
x.j.results.forEach(function(r){var b=el("button","ghost",r.label);b.type="button";b.onclick=function(){pick(r.name,r.latitude,r.longitude)};h.appendChild(b)});
if(x.j.warnings&&x.j.warnings.length)h.appendChild(el("p","small",x.j.warnings[0]))})
.catch(function(){h.textContent="";h.appendChild(el("p","err",t("placeoffline")))})}
$("geo").onclick=function(){if(!navigator.geolocation){return}navigator.geolocation.getCurrentPosition(function(p){
$("lat").value=p.coords.latitude.toFixed(4);$("lon").value=p.coords.longitude.toFixed(4);if(mod==="live")$("go").click()})};
$("f").onsubmit=function(e){e.preventDefault();var body={latitude:parseFloat($("lat").value),longitude:parseFloat($("lon").value),days:parseInt($("days").value||"30",10)};
if(mod!=="ecoguard")body.crop=$("crop").value||"wheat";if((mod==="agrispace"||mod==="live")&&$("kc").value)body.kc=parseFloat($("kc").value);
if(mod==="ecoguard")body.region=$("region").value;if(mod==="live"||mod==="rotation")body.soil=$("soil").value;
if(mod==="rotation"){body.current_rabi=$("rabi").value;body.current_kharif=$("kharif").value;body.water=$("water").value;
body.priorities={water:+$("pr_water").value,soil:+$("pr_soil").value,erosion:+$("pr_erosion").value,forage:+$("pr_forage").value,income:+$("pr_income").value,resilience:+$("pr_resilience").value,pest:+$("pr_pest").value};
body.rain_change_pct=parseFloat($("rainchg").value||"0");body.temp_change_c=parseFloat($("tempchg").value||"0")}
if(mod==="validate"){var vt=$("valcsv").value;if(!vt.trim()){fail(t("val_none"));return}body.csv=vt}
if(mod==="ndvi"){var nt=$("ndvicsv").value;if(nt.trim()){body.csv=nt}else if(!FEATURES.ndvi){fail(t("ndvi_none"));return}}
var o=$("out"),btn=$("go");o.textContent="";o.appendChild(el("p","small",t("loading")));btn.disabled=true;busy=true;last=null;
fetch("/api/"+mod,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)})
.then(function(r){return r.json().then(function(j){return{ok:r.ok,j:j}})})
.then(function(x){if(!x.ok){fail(t("error")+": "+(x.j.detail||t("failed")));return}last=x.j;try{localStorage.setItem("s2e_last_"+mod,JSON.stringify({t:Date.now(),d:x.j}))}catch(e){}render(x.j)})
.catch(function(err){fail(t("failed")+": "+err.message)})
.then(function(){btn.disabled=false;busy=false})};
var dip=null;window.addEventListener("beforeinstallprompt",function(e){e.preventDefault();dip=e;$("inst").className="ghost"});
$("inst").onclick=function(){if(dip){dip.prompt();dip=null;$("inst").className="ghost hide"}};
if("serviceWorker" in navigator){window.addEventListener("load",function(){navigator.serviceWorker.register("/sw.js").catch(function(){})})}
fillPlaces();$("find").onclick=search;$("q").onkeydown=function(e){if(e.key==="Enter"){e.preventDefault();search()}};
$("ndvifile").onchange=function(){var f=this.files&&this.files[0];if(!f)return;if(f.size>200000){$("ndvicsv").value="";fail(t("val_big"));return}
var rd=new FileReader();rd.onload=function(){$("ndvicsv").value=String(rd.result||"")};rd.readAsText(f)};
$("valfile").onchange=function(){var f=this.files&&this.files[0];if(!f)return;if(f.size>200000){$("valcsv").value="";fail(t("val_big"));return}
var rd=new FileReader();rd.onload=function(){$("valcsv").value=String(rd.result||"")};rd.readAsText(f)};
setMod("live");applyLang();
})();
</script></body></html>
"""


# ==============================================================================
# Validation against local observations  (python space2earth.py --validate FILE.csv --lat X --lon Y)
# ==============================================================================
VAL_MAP = {"tmax": "T2M_MAX", "tmin": "T2M_MIN", "tmean": "T2M", "precip": "PRECTOTCORR"}


def read_obs_csv(path: str) -> dict:
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return read_obs_text(fh.read())


def read_obs_text(text: str) -> dict:
    """CSV text with a 'date' column plus any of: tmax, tmin, tmean (deg C), precip (mm/day)."""
    obs = {k: {} for k in VAL_MAP}
    with io.StringIO(text.lstrip("\ufeff"), newline="") as fh:
        rd = csv.DictReader(fh)
        if not rd.fieldnames:
            raise ValueError("empty CSV")
        cols = {c.strip().lower(): c for c in rd.fieldnames}
        if "date" not in cols:
            raise ValueError("CSV needs a 'date' column")
        known = [k for k in VAL_MAP if k in cols]
        if not known:
            raise ValueError("CSV needs at least one of: tmax, tmin, tmean, precip")
        for i, row in enumerate(rd, start=2):
            if i > 1100:
                raise ValueError("CSV: at most 1095 data rows (3 years) per run")
            raw = (row.get(cols["date"]) or "").strip()
            d = None
            for fmt in ("%Y-%m-%d", "%Y%m%d", "%d/%m/%Y"):
                try:
                    d = datetime.strptime(raw, fmt).date()
                    break
                except ValueError:
                    pass
            if d is None:
                raise ValueError(f"line {i}: bad date '{raw}' (use YYYY-MM-DD)")
            for k in known:
                v = (row.get(cols[k]) or "").strip()
                if v == "":
                    continue
                try:
                    f = float(v)
                except ValueError:
                    raise ValueError(f"line {i}: '{v}' is not a number in column {k}") from None
                if not math.isfinite(f) or (k == "precip" and f < 0) or (k != "precip" and not -60 <= f <= 60):
                    raise ValueError(f"line {i}: value {v} out of range for {k}")
                obs[k][d] = f
    return obs


def compare_series(obs: dict, model: dict) -> dict:
    """bias = model - observed. r = Pearson correlation."""
    pairs = [(o, model[d]) for d, o in sorted(obs.items()) if model.get(d) is not None]
    n = len(pairs)
    if n < 5:
        return {"n": n, "note": "need at least 5 overlapping days"}
    o, m = [p[0] for p in pairs], [p[1] for p in pairs]
    mo, mm = sum(o) / n, sum(m) / n
    err = [b - a for a, b in pairs]
    so, sm = sum((x - mo) ** 2 for x in o), sum((x - mm) ** 2 for x in m)
    r = sum((a - mo) * (b - mm) for a, b in pairs) / math.sqrt(so * sm) if so > 0 and sm > 0 else None
    return {"n": n, "bias": rnd(sum(err) / n), "mae": rnd(sum(abs(e) for e in err) / n),
            "rmse": rnd(math.sqrt(sum(e * e for e in err) / n)), "r": rnd(r, 3),
            "observed_mean": rnd(mo), "model_mean": rnd(mm)}


def run_validation(obs: dict, lat: float, lon: float, fetch=None) -> dict:
    fetch = fetch or fetch_daily
    all_dates = [d for v in obs.values() for d in v]
    if not all_dates:
        raise ValueError("no observations found")
    start, end = min(all_dates), max(all_dates)
    if (end - start).days > 1095:
        raise ValueError("validation period is limited to 3 years per run")
    params = [VAL_MAP[k] for k, v in obs.items() if v]
    dates, series = fetch(lat, lon, params, start, end)
    out = {}
    for k, p in VAL_MAP.items():
        if not obs[k]:
            continue
        model = dict(zip(dates, series[p]))
        st = compare_series(obs[k], model)
        if k == "precip" and st.get("n", 0) >= 5:
            pr = [(o, model[d]) for d, o in obs[k].items() if model.get(d) is not None]
            st["wet_day_agreement_pct"] = round(100 * sum((a >= 1) == (b >= 1) for a, b in pr) / len(pr), 1)
        out[k] = st
    return {"location": {"latitude": lat, "longitude": lon},
            "period": {"start": start.isoformat(), "end": end.isoformat()},
            "model_variable": {k: VAL_MAP[k] for k in out}, "results": out,
            "notes": ["bias = NASA POWER minus your observation (positive = NASA higher).",
                      "POWER is a ~50 km grid-cell estimate; a single station will differ, especially for "
                      "rainfall and in hilly terrain. Report these numbers honestly as the app's uncertainty."]}


def observations_from_records(recs: list) -> dict:
    """JSON records -> the same validated parser as the CSV path (one code path, one set of checks)."""
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["date"] + list(VAL_MAP))
    for i, rec in enumerate(recs):
        if not isinstance(rec, dict) or not isinstance(rec.get("date"), str):
            raise ValueError(f"observation {i}: object with a 'date' string (YYYY-MM-DD) required")
        row = [rec["date"]]
        for k in VAL_MAP:
            v = rec.get(k)
            if v is not None and (isinstance(v, bool) or not isinstance(v, (int, float, str))):
                raise ValueError(f"observation {i}: {k} must be a number")
            row.append("" if v is None else str(v))
        w.writerow(row)
    return read_obs_text(buf.getvalue())


def interpret_validation(rep_: dict) -> dict:
    """Plain statements of what the error metrics say. No accuracy grades are invented."""
    out = {}
    for var, st in rep_["results"].items():
        if "bias" not in st:
            out[var] = st.get("note", "not enough overlapping days")
            continue
        unit = "mm/day" if var == "precip" else "C"
        if st["bias"] == 0:
            txt = "NASA POWER matches your observations on average (bias 0.0)"
        else:
            txt = (f"NASA POWER is on average {abs(st['bias']):.1f} {unit} "
                   f"{'higher' if st['bias'] > 0 else 'lower'} than your observations")
        txt += f" (MAE {st['mae']:.1f}, RMSE {st['rmse']:.1f} {unit}, correlation {st['r']}) over {st['n']} days."
        if "wet_day_agreement_pct" in st:
            txt += f" Wet/dry-day agreement {st['wet_day_agreement_pct']}%."
        if st["n"] < 30:
            txt += " Fewer than 30 days: indicative only."
        out[var] = txt
    return out


def validate_cli(path: str, lat: float, lon: float) -> int:
    try:
        rep_ = run_validation(read_obs_csv(path), lat, lon)
    except (ValueError, OSError, PowerError, csv.Error) as exc:
        print("VALIDATION FAILED:", exc)
        return 1
    print(f"\nValidation {rep_['period']['start']} -> {rep_['period']['end']} at ({lat}, {lon})")
    print(f"{'var':8}{'n':>5}{'bias':>8}{'MAE':>8}{'RMSE':>8}{'r':>8}")
    for k, st in rep_["results"].items():
        if "bias" not in st:
            print(f"{k:8}{st['n']:>5}  {st['note']}")
            continue
        print(f"{k:8}{st['n']:>5}{st['bias']:>8}{st['mae']:>8}{st['rmse']:>8}{str(st['r']):>8}"
              + (f"   wet-day agreement {st['wet_day_agreement_pct']}%" if "wet_day_agreement_pct" in st else ""))
    with open("validation_report.json", "w", encoding="utf-8") as fh:
        json.dump(rep_, fh, indent=2, ensure_ascii=False)
    print("\nSaved validation_report.json (put these numbers in your submission).")
    return 0


# ==============================================================================
# Offline test-suite  (python space2earth.py --test)
# ==============================================================================
def write_test_summary(res, path: str = "test_results.json") -> dict:
    bad = len(res.failures) + len(res.errors)
    summary = {"version": VERSION, "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "tests_run": res.testsRun, "failures": len(res.failures), "errors": len(res.errors),
               "skipped": len(res.skipped), "passed": res.testsRun - bad - len(res.skipped),
               "success": bool(res.wasSuccessful()),
               "failed_tests": [str(t) for t, _ in list(res.failures) + list(res.errors)]}
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    return summary


def run_tests() -> int:
    import tempfile
    import unittest
    from unittest import mock

    this = sys.modules[__name__]
    tmpdir = tempfile.mkdtemp(prefix="s2e_")

    SCEN_NORMAL = {"T2M": 20, "T2M_MAX": 28, "T2M_MIN": 12, "RH2M": 50, "WS2M": 2.0, "PS": 95.0,
                   "ALLSKY_SFC_SW_DWN": 18.0, "PRECTOTCORR": 0.5, "GWETTOP": 0.4, "GWETROOT": 0.5,
                   "ALLSKY_SFC_UV_INDEX": 6.0}
    calls = []
    live_calls = []
    ndvi_calls = []

    def make_fake(scen, fail_uv=False, fail_all=False, hole_from=None):
        def fake(url, params):
            calls.append(url)
            if fail_all:
                raise PowerError("down")
            names = params["parameters"].split(",")
            if "climatology" in url:
                return {"properties": {"parameter": {n: {m: scen[n] for m in MONTHS + ["ANN"]} for n in names}}}
            if fail_uv and "ALLSKY_SFC_UV_INDEX" in names:
                raise PowerError("uv down")
            start = datetime.strptime(params["start"], "%Y%m%d")
            end = datetime.strptime(params["end"], "%Y%m%d")
            nd = (end - start).days + 1
            out = {}
            for n in names:
                ser = {}
                for i in range(nd):
                    k = (start + timedelta(days=i)).strftime("%Y%m%d")
                    ser[k] = -999.0 if (hole_from is not None and i >= hole_from) else scen[n]
                out[n] = ser
            return {"properties": {"parameter": out}}
        return fake

    class T(unittest.TestCase):
        def setUp(self):
            app.config["TESTING"] = True
            app.config["RATE_LIMIT_ENABLED"] = False
            cache.clear()
            calls.clear()
            this.DB_PATH = os.path.join(tmpdir, f"t_{time.time_ns()}.db")
            init_db()
            self.c = app.test_client()

        def post(self, mod, body=None, **kw):
            return self.c.post(f"/api/{mod}", json=body if body is not None else {"latitude": 34.0, "longitude": 71.5}, **kw)

        def patched(self, **kw):
            return mock.patch.object(this, "power_request", side_effect=make_fake(**kw))

        # ---- science ----
        def test_fao56_example18(self):  # Brussels, 6 July: published ET0 = 3.9 mm/day
            et = et0_penman_monteith(16.9, 21.5, 12.3, 73.5, 2.078, 22.07, 100.1, 50.80, 187)
            self.assertAlmostEqual(et, 3.9, delta=0.35)

        def test_ra_equator_equinox(self):
            self.assertTrue(34.5 < extraterrestrial_radiation(0, 80) < 38.5)

        def test_heat_index_nws(self):  # 95 F @ 50 % RH -> Rothfusz regression = ~105 F = ~40.6 C
            self.assertAlmostEqual(heat_index_c(35.0, 50), 40.6, delta=0.6)
            self.assertAlmostEqual(heat_index_c(20.0, 50), 20.0, delta=2.5)

        def test_fill_value_not_averaged(self):
            self.assertEqual(_clean(-999.0), None)
            self.assertEqual(mean([25.0, None, 27.0]), 26.0)
            self.assertIsNone(mean([None, None]))

        def test_uv_and_heat_categories(self):
            self.assertEqual(uv_category(9), "very_high")
            self.assertEqual(heat_category(40), "danger")
            self.assertEqual(longest_run([1, 1, 0, 1, 1, 1]), 3)

        # ---- validation ----
        def test_invalid_inputs_400(self):
            for body in ({"latitude": 105, "longitude": 71.5}, {"latitude": 34, "longitude": -999},
                         {"latitude": "abc", "longitude": 71.5}, {}, {"latitude": float("nan"), "longitude": 1},
                         {"latitude": 34, "longitude": 71, "days": 500}, {"latitude": True, "longitude": 1}):
                self.assertEqual(self.post("agrispace", body).status_code, 400, body)

        def test_non_object_json_400(self):
            r = self.c.post("/api/agrispace", data="[1,2]", content_type="application/json")
            self.assertEqual(r.status_code, 400)
            r = self.c.post("/api/agrispace", data="not json", content_type="application/json")
            self.assertEqual(r.status_code, 400)

        def test_oversized_body_413(self):
            r = self.c.post("/api/agrispace", data="x" * 10000, content_type="application/json")
            self.assertEqual(r.status_code, 413)

        # ---- modules ----
        def test_agri_normal_and_envelope(self):
            with self.patched(scen=SCEN_NORMAL):
                r = self.post("agrispace", {"latitude": 34.0, "longitude": 71.5, "crop": "gandum"})
            d = r.get_json()
            self.assertEqual(r.status_code, 200)
            self.assertEqual(d["metrics"]["crop"], "Wheat")
            self.assertEqual(d["assessment"]["level"], "Low")
            self.assertTrue(0.5 < d["metrics"]["et0_total_mm"] / 30 < 12)
            for k in ("window", "data_quality", "provenance", "limitations", "series"):
                self.assertIn(k, d)
            self.assertEqual(len(d["series"]["dates"]), 30)

        def test_agri_drought_high(self):
            scen = {**SCEN_NORMAL, "GWETROOT": 0.12, "PRECTOTCORR": 0.0}
            with self.patched(scen=scen):
                d = self.post("agrispace").get_json()
            self.assertEqual(d["assessment"]["level"], "High")
            self.assertTrue(any("irrigat" in a.lower() for a in d["advisory"]))

        def test_unknown_crop_warns(self):
            with self.patched(scen=SCEN_NORMAL):
                d = self.post("bioshield", {"latitude": 34, "longitude": 71, "crop": "dragonfruit"}).get_json()
            self.assertTrue(d["warnings"])
            self.assertEqual(d["metrics"]["crop"], "Generic crop")

        def test_bioshield_heat(self):
            scen = {**SCEN_NORMAL, "T2M_MAX": 40.0}
            with self.patched(scen=scen):
                d = self.post("bioshield").get_json()
            self.assertEqual(d["assessment"]["level"], "High")
            self.assertEqual(d["metrics"]["heat_stress_days"], 30)

        def test_bioshield_graceful_without_uv(self):
            with self.patched(scen=SCEN_NORMAL, fail_uv=True):
                r = self.post("bioshield")
            d = r.get_json()
            self.assertEqual(r.status_code, 200)
            self.assertIn("ALLSKY_SFC_UV_INDEX", d["data_quality"]["missing_parameters"])
            self.assertIsNone(d["metrics"]["uv_index_max"])

        def test_ecoguard_no_fake_aqi(self):
            scen = {**SCEN_NORMAL, "T2M_MAX": 42.0, "RH2M": 40}
            with self.patched(scen=scen):
                d = self.post("ecoguard", {"latitude": 34, "longitude": 71, "region": "Peshawar"}).get_json()
            self.assertIn("Not provided", d["metrics"]["air_quality"])
            self.assertNotIn("air_quality_index", d["metrics"])
            self.assertIn(d["assessment"]["level"], ("Moderate", "High"))

        def test_trailing_fill_days_skipped_not_averaged(self):
            with self.patched(scen=SCEN_NORMAL, hole_from=25):
                d = self.post("agrispace").get_json()
            self.assertEqual(d["metrics"]["temperature_mean_c"], 20.0)
            self.assertEqual(d["window"]["last_valid_date"],
                             (date.fromisoformat(d["window"]["end"]) - timedelta(days=5)).isoformat())

        def test_too_little_data_502(self):
            with self.patched(scen=SCEN_NORMAL, hole_from=5):
                r = self.post("agrispace")
            self.assertEqual(r.status_code, 502)

        def test_nasa_outage_502(self):
            with self.patched(scen=SCEN_NORMAL, fail_all=True):
                r = self.post("agrispace")
            self.assertEqual(r.status_code, 502)

        # ---- infra ----
        def test_cache_shared_across_modules(self):
            with self.patched(scen=SCEN_NORMAL):
                self.post("agrispace")
                n = len(calls)
                self.post("bioshield")
                self.post("ecoguard")
            self.assertEqual(len(calls), n)  # no extra NASA calls

        def test_cache_separate_locations(self):
            with self.patched(scen=SCEN_NORMAL):
                self.post("agrispace")
                n = len(calls)
                self.post("agrispace", {"latitude": 50, "longitude": 10})
            self.assertGreater(len(calls), n)

        def test_rate_limit(self):
            app.config["RATE_LIMIT_ENABLED"] = True
            limit_minute.reset(); limit_hour.reset()
            codes = [self.post("agrispace", {}).status_code for _ in range(limit_minute.limit + 2)]
            self.assertEqual(codes.count(429), 2)
            limit_minute.reset(); limit_hour.reset()

        def test_telemetry_can_be_disabled(self):
            with self.patched(scen=SCEN_NORMAL), mock.patch.dict(os.environ, {"DISABLE_TELEMETRY": "1"}):
                self.post("agrispace")
            with _db_connect() as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM telemetry_logs").fetchone()[0], 0)

        def test_db_logging_and_health(self):
            with self.patched(scen=SCEN_NORMAL):
                self.post("agrispace")
            with _db_connect() as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM telemetry_logs").fetchone()[0], 1)
            r = self.c.get("/health")
            self.assertEqual(r.status_code, 200)
            self.assertTrue(r.get_json()["database"])

        def test_security_headers_and_nonce(self):
            r = self.c.get("/")
            html = r.get_data(as_text=True)
            self.assertNotIn("__NONCE__", html)
            csp = r.headers["Content-Security-Policy"]
            nonce = re.search(r"'nonce-([^']+)'", csp).group(1)
            self.assertIn(f'nonce="{nonce}"', html)
            self.assertEqual(r.headers["X-Content-Type-Options"], "nosniff")

        def test_json_errors(self):
            self.assertEqual(self.c.get("/nope").get_json()["status"], "error")
            self.assertEqual(self.c.get("/api/agrispace").status_code, 405)

        def test_pwa_assets(self):
            m = self.c.get("/manifest.webmanifest")
            self.assertEqual(m.status_code, 200)
            self.assertIn("manifest+json", m.content_type)
            j = m.get_json(force=True)
            self.assertEqual(j["display"], "standalone")
            self.assertEqual({i["sizes"] for i in j["icons"]}, {"192x192", "512x512"})
            sw = self.c.get("/sw.js")
            self.assertEqual(sw.status_code, 200)
            self.assertIn("javascript", sw.content_type)
            self.assertEqual(sw.headers["Service-Worker-Allowed"], "/")
            for size in (192, 512):
                png = self.c.get(f"/icon-{size}.png")
                self.assertEqual(png.status_code, 200)
                d = png.data
                self.assertEqual(d[:8], b"\x89PNG\r\n\x1a\n")
                self.assertEqual(struct.unpack(">II", d[16:24]), (size, size))
            self.assertEqual(self.c.get("/icon-999.png").status_code, 404)
            csp = self.c.get("/").headers["Content-Security-Policy"]
            for part in ("manifest-src 'self'", "worker-src 'self'", "img-src 'self'"):
                self.assertIn(part, csp)

        def test_urdu_crop_names(self):
            for name in ("گندم", "ٹماٹر", "آلو"):
                self.assertNotEqual(resolve_crop(name)[0], "generic", name)
            with self.patched(scen=SCEN_NORMAL):
                d = self.post("agrispace", {"latitude": 34, "longitude": 71, "crop": "گندم"}).get_json()
            self.assertEqual(d["metrics"]["crop"], "Wheat")

        def test_html_is_bilingual(self):
            html = self.c.get("/").get_data(as_text=True)
            self.assertIn('rel="manifest"', html)
            self.assertIn("serviceWorker", html)
            self.assertIn("lv_Insufficient_data", html)

        def test_compare_series_known(self):
            d0 = date(2026, 10, 1)
            obs = {d0 + timedelta(days=i): float(i + 1) for i in range(5)}
            mod = {d: v + 1 for d, v in obs.items()}
            st = compare_series(obs, mod)
            self.assertEqual((st["bias"], st["mae"], st["rmse"], st["r"]), (1.0, 1.0, 1.0, 1.0))
            self.assertIn("note", compare_series({d0: 1.0}, {d0: 2.0}))

        def test_validation_pipeline(self):
            import csv as _csv
            path = os.path.join(tmpdir, "obs.csv")
            d0 = date(2026, 10, 1)
            with open(path, "w", newline="", encoding="utf-8") as fh:
                w = _csv.writer(fh)
                w.writerow(["Date", "TMAX", "precip"])
                for i in range(10):
                    w.writerow([(d0 + timedelta(days=i)).isoformat(), 30 + i, 0 if i % 2 else 3])
            obs = read_obs_csv(path)

            def fake_fetch(lat, lon, params, start, end):
                dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
                return dates, {"T2M_MAX": [31.0 + i for i in range(len(dates))],
                               "PRECTOTCORR": [0.0 if i % 2 else 3.0 for i in range(len(dates))]}
            rep_ = run_validation(obs, 34.0, 71.5, fetch=fake_fetch)
            self.assertEqual(rep_["results"]["tmax"]["bias"], 1.0)
            self.assertEqual(rep_["results"]["tmax"]["r"], 1.0)
            self.assertEqual(rep_["results"]["precip"]["wet_day_agreement_pct"], 100.0)

        def test_csv_errors(self):
            bad = os.path.join(tmpdir, "bad.csv")
            for content, msg in (("when,tmax\n1,2\n", "date"), ("date,foo\n2026-10-01,1\n", "at least one"),
                                 ("date,tmax\nyesterday,20\n", "bad date"), ("date,tmax\n2026-10-01,abc\n", "not a number"),
                                 ("date,precip\n2026-10-01,-5\n", "out of range")):
                with open(bad, "w", encoding="utf-8") as fh:
                    fh.write(content)
                with self.assertRaises(ValueError) as cm:
                    read_obs_csv(bad)
                self.assertIn(msg, str(cm.exception))

        def test_bilingual_summary_all_modules(self):
            with self.patched(scen=SCEN_NORMAL):
                for mod in ("agrispace", "bioshield", "ecoguard"):
                    d = self.post(mod, {"latitude": 34, "longitude": 71, "crop": "wheat"}).get_json()
                    self.assertIn("NASA", d["summary"]["en"])
                    self.assertIn("ناسا", d["summary"]["ur"])
            with self.patched(scen={**SCEN_NORMAL, "GWETROOT": 0.12, "PRECTOTCORR": 0.0}):
                d = self.post("agrispace", {"latitude": 35, "longitude": 72, "crop": "گندم"}).get_json()
            self.assertIn("گندم", d["summary"]["ur"])
            self.assertIn("High", d["summary"]["en"])
            self.assertIn("mm of crop water use", d["summary"]["en"])

        def test_parallel_and_single_flight(self):
            import threading as th
            base = make_fake(scen=SCEN_NORMAL)

            def slow(url, params):
                time.sleep(0.2)
                return base(url, params)
            with mock.patch.object(this, "power_request", side_effect=slow):
                t0 = time.perf_counter()
                get_power_bundle(10.0, 20.0, 30)
                one = time.perf_counter() - t0
                self.assertLess(one, 0.5, one)            # 3 calls x 0.2 s sequential would be 0.6 s
                calls.clear()
                res = []
                ts = [th.Thread(target=lambda: res.append(get_power_bundle(34.0, 71.5, 30))) for _ in range(6)]
                [t.start() for t in ts]
                [t.join() for t in ts]
            self.assertEqual(len(res), 6)
            self.assertEqual(len(calls), 3)               # 6 simultaneous users -> one set of NASA calls

        def _live_fake(self, **ov):
            base = {"t": 22.0, "rh": 40.0, "ws": 2.0, "p": 0.0, "tmax": [28.0] * 7, "tmin": [15.0] * 7,
                    "pr": [0.0] * 7, "et": [2.0] * 7, "uv": [7.0] * 7, "soil": 0.20, "hr": 0.5, "hourly": True}
            base.update(ov)

            def fake(params):
                live_calls.append(params)
                if base.get("fail"):
                    raise LiveError("down")
                today = datetime.now(timezone.utc).date()
                out = {"current": {"time": today.isoformat() + "T10:00", "temperature_2m": base["t"],
                                   "relative_humidity_2m": base["rh"], "precipitation": base["p"],
                                   "wind_speed_10m": base["ws"]},
                       # 3 past days with absurd values: they must be sliced away, never used as forecast
                       "daily": {"time": [(today + timedelta(days=i)).isoformat() for i in range(-3, 7)],
                                 "temperature_2m_max": [99.0] * 3 + base["tmax"],
                                 "temperature_2m_min": [-50.0] * 3 + base["tmin"],
                                 "precipitation_sum": [500.0] * 3 + base["pr"],
                                 "et0_fao_evapotranspiration": [50.0] * 3 + base["et"],
                                 "uv_index_max": [99.0] * 3 + base["uv"]}}
                if base["hourly"]:
                    start = datetime(today.year, today.month, today.day) - timedelta(days=3)
                    times = [(start + timedelta(hours=i)).strftime("%Y-%m-%dT%H:00") for i in range(24 * 10)]
                    hourly = {"time": times, "precipitation": [base["hr"]] * len(times)}
                    for k, _ in SOIL_LAYERS:
                        hourly[k] = [base["soil"]] * len(times)
                    out["hourly"] = hourly
                return out
            return mock.patch.object(this, "live_request", side_effect=fake)

        def test_live_soil_and_rain_metrics(self):
            with self._live_fake(soil=0.20, hr=0.5):
                d = self.post("live", {"latitude": 34.0, "longitude": 71.5, "crop": "wheat"}).get_json()
            m = d["metrics"]
            self.assertEqual(m["soil_moisture_root_0_81cm_m3m3"], 0.2)
            self.assertAlmostEqual(m["root_zone_available_water_pct"], (0.20 - 0.12) / 0.13 * 100, delta=1)
            self.assertEqual(m["rain_last_72h_mm"], 36.0)
            self.assertEqual(m["rain_next_24h_mm"], 12.0)
            self.assertEqual(m["soil_moisture_change_3d_m3m3"], 0.0)
            self.assertLessEqual(m["tmax_peak_next_7d_c"], 28.0)        # past-day values were sliced away
            self.assertEqual(len(d["series"]["dates"]), 7)
            self.assertEqual(len(d["series"]["soil_moisture_hourly_m3m3"]), 121)
            self.assertIn("plant-available", d["summary"]["en"])
            self.assertIn("بارش", d["summary"]["ur"])

        def test_live_soil_alerts_and_soil_type(self):
            with self._live_fake(soil=0.13):
                d = self.post("live", {"latitude": 34.0, "longitude": 71.5}).get_json()
            self.assertEqual(d["assessment"]["level"], "High")
            self.assertTrue(any("Root-zone" in x for x in d["assessment"]["reasons"]))
            cache.clear()
            with self._live_fake(soil=0.30, pr=[0, 0, 60, 0, 0, 0, 0]):
                d = self.post("live", {"latitude": 34.0, "longitude": 71.5}).get_json()
            self.assertTrue(any("waterlogging" in x for x in d["assessment"]["reasons"]))
            cache.clear()
            with self._live_fake(soil=0.30):
                d = self.post("live", {"latitude": 34.0, "longitude": 71.5, "soil": "clay"}).get_json()
            self.assertEqual(d["metrics"]["soil_type_assumed"], "clay")
            self.assertAlmostEqual(d["metrics"]["root_zone_available_water_pct"], (0.30 - 0.22) / 0.14 * 100, delta=1)
            cache.clear()
            with self._live_fake():
                d = self.post("live", {"latitude": 34.0, "longitude": 71.5, "soil": "moon dust"}).get_json()
            self.assertEqual(d["metrics"]["soil_type_assumed"], "loam")
            self.assertTrue(any("moon dust" in w for w in d["warnings"]))

        def test_live_without_hourly_data_degrades(self):
            with self._live_fake(hourly=False):
                r = self.post("live", {"latitude": 34.0, "longitude": 71.5})
            d = r.get_json()
            self.assertEqual(r.status_code, 200)
            self.assertIsNone(d["metrics"]["root_zone_available_water_pct"])
            self.assertIn("soil_moisture", d["data_quality"]["missing_parameters"])

        def test_live_normal_is_labelled_non_nasa(self):
            live_calls.clear()
            with self._live_fake():
                r = self.post("live", {"latitude": 34.0, "longitude": 71.5, "crop": "wheat"})
            d = r.get_json()
            self.assertEqual(r.status_code, 200)
            self.assertEqual(d["assessment"]["level"], "Low")
            self.assertIn("NOT NASA", d["provenance"]["source"])
            self.assertTrue(any("non-NASA" in w for w in d["warnings"]))
            self.assertIn("Open-Meteo", d["summary"]["en"])
            self.assertIn("Open-Meteo", d["summary"]["ur"])
            self.assertEqual(len(d["series"]["dates"]), 7)
            self.assertEqual(d["window"]["mode"], "live + forecast")
            self.assertAlmostEqual(d["metrics"]["heat_index_now_c"], 22.0, delta=2.5)

        def test_live_alerts(self):
            cases = (({"tmin": [-3.0] + [10.0] * 6}, "High", "Frost"), ({"tmax": [41.0] * 7}, "High", "Heat"),
                     ({"pr": [0, 0, 60, 0, 0, 0, 0]}, "High", "Heavy rain"), ({"et": [4.0] * 7}, "Moderate", "Dry week"))
            for ov, level, word in cases:
                cache.clear()
                with self._live_fake(**ov):
                    d = self.post("live", {"latitude": 34.0, "longitude": 71.5, "crop": "wheat"}).get_json()
                self.assertEqual(d["assessment"]["level"], level, ov)
                self.assertTrue(any(word in x for x in d["assessment"]["reasons"]), ov)

        def test_live_cache_errors_and_validation(self):
            live_calls.clear()
            with self._live_fake():
                self.post("live")
                self.post("live")
            self.assertEqual(len(live_calls), 1)
            cache.clear()
            with self._live_fake(fail=True):
                self.assertEqual(self.post("live").status_code, 502)
            self.assertEqual(self.post("live", {"latitude": 999, "longitude": 1}).status_code, 400)

        def test_nasa_data_age_reported(self):
            with self.patched(scen=SCEN_NORMAL):
                d = self.post("agrispace").get_json()
            self.assertEqual(d["window"]["data_age_days"], POWER_LAG_DAYS)

        def test_places_builtin_are_sane(self):
            for name, (lat, lon) in PLACES.items():
                self.assertTrue(23.0 <= lat <= 37.5 and 60.5 <= lon <= 77.9, name)
            self.assertEqual(PLACES["Multan"][1], 71.4724)
            self.assertEqual([h["name"] for h in search_places("pesh")], ["Peshawar"])

        def test_geocode_endpoint(self):
            body = {"results": [{"name": "Bara", "admin1": "Khyber Pakhtunkhwa", "country": "Pakistan",
                                 "latitude": 33.91, "longitude": 71.49}, {"name": "bad", "latitude": None}]}
            geo_calls = []

            def fake(params):
                geo_calls.append(params)
                return body
            with mock.patch.object(this, "geocode_request", side_effect=fake):
                r = self.c.get("/api/geocode?q=Bara")
                self.c.get("/api/geocode?q=Bara")
            d = r.get_json()
            self.assertEqual(r.status_code, 200)
            self.assertEqual([x["name"] for x in d["results"]], ["Bara"])
            self.assertEqual(d["results"][0]["label"], "Bara, Khyber Pakhtunkhwa, Pakistan")
            self.assertEqual(len(geo_calls), 1)                       # second look-up came from cache
            self.assertEqual(geo_calls[0]["countryCode"], "PK")
            with mock.patch.object(this, "geocode_request", side_effect=LiveError("down")):
                d = self.c.get("/api/geocode?q=Peshawar").get_json()
            self.assertEqual(d["results"][0]["name"], "Peshawar")      # built-in still works offline
            self.assertTrue(d["warnings"])
            self.assertEqual(self.c.get("/api/geocode?q=a").status_code, 400)
            self.assertEqual(self.c.get("/api/geocode").status_code, 400)

        def test_flood_safety_wording(self):
            with self._live_fake(hr=3.0):                              # 72 mm in the next 24 h
                d = self.post("live", {"latitude": 34.0, "longitude": 71.5}).get_json()
            self.assertEqual(d["assessment"]["level"], "High")
            self.assertTrue(any("Very heavy rain" in x for x in d["assessment"]["reasons"]))
            self.assertTrue(any("NOT an official warning" in x and "1122" in x for x in d["advisory"]))
            self.assertIn("not an official flood warning", d["summary"]["en"])

        def test_ui_has_search_voice_and_offline_fallback(self):
            html = self.c.get("/").get_data(as_text=True)
            self.assertNotIn("__PLACES__", html)
            self.assertIn('"Peshawar":[34.0151,71.5249]', html.replace(" ", ""))
            for needle in ("speechSynthesis", "/api/geocode", 'id="places"', "function fail(", "stale"):
                self.assertIn(needle, html)

        def test_live_rain_accumulation_alert(self):
            with self._live_fake(pr=[20.0, 20.0, 20.0, 0, 0, 0, 0]):
                d = self.post("live", {"latitude": 34.0, "longitude": 71.5}).get_json()
            self.assertEqual(d["assessment"]["level"], "Moderate")
            self.assertTrue(any("accumulation" in x for x in d["assessment"]["reasons"]))
            cache.clear()
            with self._live_fake(pr=[40.0, 40.0, 40.0, 0, 0, 0, 0]):          # daily alerts already cover this
                d = self.post("live", {"latitude": 34.0, "longitude": 71.5}).get_json()
            self.assertFalse(any("accumulation" in x for x in d["assessment"]["reasons"]))

        def test_ui_has_nasa_satellite_card(self):
            r = self.c.get("/")
            html = r.get_data(as_text=True)
            for needle in ("satCard", "gibs.earthdata.nasa.gov/wmts", "GoogleMapsCompatible_Level9", "satnote"):
                self.assertIn(needle, html)
            self.assertIn("https://gibs.earthdata.nasa.gov", r.headers["Content-Security-Policy"])
            self.assertTrue("FEATURES={ndvi:false}" in html)               # experimental NDVI is off by default
            self.assertFalse("showField" in html or "fallback" in html.lower())   # and no hard-coded fake NDVI values

        def _rot_fake(self):
            def fake(url, params):
                calls.append(url)
                out = {}
                for n in params["parameters"].split(","):
                    ser = {}
                    for i, m in enumerate(MONTHS):
                        c = math.cos((i + 1 - 7) * math.pi / 6)
                        ser[m] = {"T2M_MAX": 26 + 12 * c, "T2M_MIN": 12 + 12 * c, "T2M": 19 + 12 * c, "RH2M": 45.0,
                                  "WS2M": 2.0, "PS": 98.0, "ALLSKY_SFC_SW_DWN": 16 + 6 * c,
                                  "PRECTOTCORR": 0.6 + 2.0 * max(0.0, c), "GWETROOT": 0.45}.get(n, 1.0)
                    ser["ANN"] = 0.0
                    out[n] = ser
                return {"properties": {"parameter": out}}
            return mock.patch.object(this, "power_request", side_effect=fake)

        def test_rotation_compares_three_strategies_with_evidence(self):
            with self._rot_fake():
                r = self.post("rotation", {"latitude": 34.0, "longitude": 71.5, "current_rabi": "wheat", "current_kharif": "maize"})
            d = r.get_json()
            self.assertEqual(r.status_code, 200)
            st = d["strategies"]
            self.assertGreaterEqual(len(st), 3)
            self.assertEqual(sum(s["role"] == "existing" for s in st), 1)
            for s in st:
                for k in ("why", "evidence", "benefits", "risks", "information_gaps"):
                    self.assertTrue(s[k], k)
                self.assertTrue(0 <= s["score"] <= 100)
                self.assertTrue(all(0 <= v <= 100 for v in s["criteria"].values()))
            self.assertEqual([s["rank"] for s in st], list(range(1, len(st) + 1)))
            self.assertEqual([s["score"] for s in st], sorted((s["score"] for s in st), reverse=True))
            self.assertIn("ناسا", d["summary"]["ur"])
            self.assertIn("NASA", d["summary"]["en"])
            self.assertTrue(0 <= d["rank_stability_pct"] <= 100)
            self.assertIn("SMAP", " ".join(st[0]["information_gaps"]))

        def test_rotation_priorities_scenario_soil_and_cache(self):
            body = {"latitude": 34.0, "longitude": 71.5, "current_rabi": "wheat", "current_kharif": "maize", "water": "limited"}
            with self._rot_fake():
                base = self.post("rotation", body).get_json()
                n = len(calls)
                dry = self.post("rotation", {**body, "rain_change_pct": -30, "temp_change_c": 3}).get_json()
                self.assertEqual(len(calls), n)                                  # climatology cached, scenario applied locally
                need = lambda d: next(s for s in d["strategies"] if s["role"] == "existing")["metrics"]["irrigation_need_mm_per_year"]
                self.assertGreater(need(dry), need(base))
                self.assertTrue(any("What-if" in w for w in dry["warnings"]))
                sand = need(self.post("rotation", {**body, "soil": "sand"}).get_json())
                clay = need(self.post("rotation", {**body, "soil": "clay"}).get_json())
                self.assertGreaterEqual(sand, clay)
                water_only = self.post("rotation", {**body, "priorities": {k: 0 for k in ROT_DEFAULT_WEIGHTS} | {"water": 3}}).get_json()
                crit = [s["criteria"]["water"] for s in water_only["strategies"]]
                self.assertEqual(crit, sorted(crit, reverse=True))               # water-only priority ranks by water saving

        def test_rotation_validation_and_outage(self):
            base = {"latitude": 34.0, "longitude": 71.5}
            for extra in ({"current_rabi": "dragonfruit"}, {"current_kharif": "wheat"}, {"water": "ocean"},
                          {"priorities": {"water": 9}}, {"priorities": {"profit": 1}}, {"priorities": [1]},
                          {"market": {"wheat": 99}}, {"rain_change_pct": 500}, {"temp_change_c": -5}):
                self.assertEqual(self.post("rotation", {**base, **extra}).status_code, 400, extra)
            with mock.patch.object(this, "power_request", side_effect=PowerError("down")):
                self.assertEqual(self.post("rotation", base).status_code, 502)

        def test_rotation_southern_hemisphere_and_ui(self):
            self.assertEqual(rot_months("wheat", -30.0), [5, 6, 7, 8, 9, 10])
            html = self.c.get("/").get_data(as_text=True)
            for needle in ("renderRotation", 'rotation:["rotbox"', "rot_stab", "pr_resilience"):
                self.assertIn(needle, html)

        def test_ols_slope(self):
            slope, t = _ols([1, 2, 3, 4], [2, 4, 6, 8])
            self.assertAlmostEqual(slope, 2.0)
            self.assertTrue(math.isinf(t))

        def test_rotation_long_term_trend(self):
            def fake(url, params):
                calls.append(url)
                if "monthly" in url:
                    if getattr(self, "_fail_monthly", False):
                        raise PowerError("down")
                    out = {"T2M": {}, "PRECTOTCORR": {}}
                    for y in range(int(params["start"]), int(params["end"]) + 1):
                        for m in range(1, 13):
                            out["T2M"][f"{y}{m:02d}"] = 20 + 0.03 * (y - 2001) + (m - 6) * 0.1
                            out["PRECTOTCORR"][f"{y}{m:02d}"] = 2.0 - 0.01 * (y - 2001)
                        out["T2M"][f"{y}13"] = 0.0                      # annual key must be ignored
                    return {"properties": {"parameter": out}}
                out = {}
                for n in params["parameters"].split(","):
                    ser = {}
                    for i, m in enumerate(MONTHS):
                        c = math.cos((i + 1 - 7) * math.pi / 6)
                        ser[m] = {"T2M_MAX": 26 + 12 * c, "T2M_MIN": 12 + 12 * c, "T2M": 19 + 12 * c, "RH2M": 45.0,
                                  "WS2M": 2.0, "PS": 98.0, "ALLSKY_SFC_SW_DWN": 16 + 6 * c,
                                  "PRECTOTCORR": 0.6 + 2.0 * max(0.0, c), "GWETROOT": 0.45}.get(n, 1.0)
                    out[n] = ser
                return {"properties": {"parameter": out}}
            body = {"latitude": 34.0, "longitude": 71.5}
            with mock.patch.object(this, "power_request", side_effect=fake):
                d = self.post("rotation", body).get_json()
                tr = d["climate"]["long_term_trend"]
                self.assertEqual(tr["n_years"], 25)
                self.assertAlmostEqual(tr["temperature_c_per_decade"], 0.30, delta=0.02)
                self.assertAlmostEqual(tr["rain_pct_per_decade"], -5.3, delta=0.6)
                self.assertTrue(tr["temperature_significant"] and tr["rain_significant"])
                self.assertIn("Local trend", d["summary"]["en"])
                self.assertIn("مقامی رجحان", d["summary"]["ur"])
                n = len(calls)
                self.post("rotation", body)
                self.assertEqual(len(calls), n)                          # trend and climatology both cached
                cache.clear()
                self._fail_monthly = True
                r = self.post("rotation", body)
                self.assertEqual(r.status_code, 200)                     # trend is optional
                self.assertIsNone(r.get_json()["climate"]["long_term_trend"])

        def test_ui_shows_long_term_trend(self):
            html = self.c.get("/").get_data(as_text=True)
            for needle in ("long_term_trend", "rot_trend", "rot_trendnote"):
                self.assertIn(needle, html)

        def test_independence_categories_uncertainty_in_all_modules(self):
            with self.patched(scen=SCEN_NORMAL):
                d = self.post("agrispace").get_json()
            self.assertIn("None.", d["independence"]["nasa_affiliation"])
            self.assertIn("AI assistance", d["independence"]["ai_assistance"])
            self.assertIn("not a field measurement", d["data_categories"]["historical_nasa"]["label"])
            self.assertEqual(d["uncertainty"]["PRECTOTCORR"]["expected_reliability"], "lower")
            self.assertIn("NOT validated", d["uncertainty"]["T2M"]["basis"])
            with self._live_fake():
                d = self.post("live").get_json()
            self.assertIn("not NASA", d["data_categories"]["forecast"]["label"])
            self.assertIn("independence", d)
            with self._rot_fake():
                d = self.post("rotation").get_json()
            self.assertIn("historical_nasa", d["data_categories"])
            self.assertIn("independence", d)

        def test_validate_endpoint_json_and_csv(self):
            d0 = date(2026, 10, 1)
            recs = [{"date": (d0 + timedelta(days=i)).isoformat(), "tmax": 30 + i, "precip": 0 if i % 2 else 3} for i in range(10)]
            with self.patched(scen=SCEN_NORMAL):
                r = self.c.post("/api/validate", json={"latitude": 34.0, "longitude": 71.5, "observations": recs})
                d = r.get_json()
                self.assertEqual(r.status_code, 200)
                self.assertEqual(d["module"], "validation")
                st = d["results"]["tmax"]
                self.assertEqual(st["n"], 10)
                self.assertAlmostEqual(st["bias"], 28 - 34.5, places=1)
                self.assertIn("lower", d["interpretation"]["tmax"])
                self.assertIn("wet_day_agreement_pct", d["results"]["precip"])
                self.assertIn("ground_observation", d["data_categories"])
                self.assertIn("historical_nasa", d["data_categories"])
                self.assertIn("None.", d["independence"]["nasa_affiliation"])
                csv_text = "date,tmax\n" + "\n".join(f"{x['date']},{x['tmax']}" for x in recs)
                r2 = self.c.post("/api/validate", json={"latitude": 34.0, "longitude": 71.5, "csv": csv_text})
                self.assertEqual(r2.status_code, 200)
                self.assertEqual(r2.get_json()["results"]["tmax"]["n"], 10)

        def test_validate_endpoint_errors_and_limits(self):
            ok = {"latitude": 34.0, "longitude": 71.5}
            for body in ({}, {**ok}, {**ok, "csv": ""}, {**ok, "csv": "when,tmax\n1,2"}, {**ok, "observations": []},
                         {**ok, "observations": [{"date": "yesterday", "tmax": 1}]}, {**ok, "observations": ["x"]},
                         {**ok, "observations": [{"date": "2026-10-01", "tmax": 500}]},
                         {"latitude": 99, "longitude": 1, "csv": "date,tmax\n2026-10-01,3"}):
                self.assertEqual(self.c.post("/api/validate", json=body).status_code, 400, body)
            good = {**ok, "csv": "date,tmax\n" + "\n".join(f"2026-10-{i:02d},30" for i in range(1, 10))}
            with self.patched(scen=SCEN_NORMAL, fail_all=True):
                self.assertEqual(self.c.post("/api/validate", json=good).status_code, 502)
            big = {**ok, "csv": "date,tmax\n" + "\n".join(["2026-10-01,30"] * 5000)}   # > 4 KB: allowed here, rejected on rows
            self.assertEqual(self.c.post("/api/validate", json=big).status_code, 400)
            self.assertEqual(self.c.post("/api/agrispace", data="x" * 5000, content_type="application/json").status_code, 413)

        def test_test_summary_file(self):
            from types import SimpleNamespace
            res = SimpleNamespace(testsRun=5, failures=[("t1", "x")], errors=[], skipped=[], wasSuccessful=lambda: False)
            path = os.path.join(tmpdir, "tr.json")
            sm = write_test_summary(res, path)
            self.assertEqual((sm["passed"], sm["failures"], sm["success"]), (4, 1, False))
            with open(path, encoding="utf-8") as fh:
                self.assertEqual(json.load(fh)["tests_run"], 5)

        def test_ui_has_validate_tab_and_independence_notice(self):
            html = self.c.get("/").get_data(as_text=True)
            for needle in ("renderValidation", "valcsv", "m_validate", "data-k=\"indep\"", "catLine"):
                self.assertIn(needle, html)

        def test_null_and_wrongly_typed_inputs_regressions(self):
            with self.patched(scen=SCEN_NORMAL):
                r = self.post("agrispace", {"latitude": 34.0, "longitude": 71.5, "days": None, "kc": None})
            self.assertEqual(r.status_code, 200)
            self.assertEqual(len(r.get_json()["series"]["dates"]), 30)       # explicit null -> default window

            cache.clear()                                                    # the first call cached this place
            def weird(url, params):
                names = params["parameters"].split(",")
                return {"properties": {"parameter": {n: ([1, 2] if n == "PRECTOTCORR" else None) for n in names}}}
            with mock.patch.object(this, "power_request", side_effect=weird):
                self.assertEqual(self.post("agrispace").status_code, 502)    # clean error, never a 500

        def test_translations_complete_and_all_keys_defined(self):
            html = self.c.get("/").get_data(as_text=True)
            js = re.search(r'<script nonce="[^"]+">(.*?)</script>', html, re.S).group(1)
            i, j = js.index("var T={en:{"), js.index("T.ur={")
            pat = re.compile(r'(?:^|[{,\n])([A-Za-z0-9_]+):"')
            ken, kur = set(pat.findall(js[i:j])), set(pat.findall(js[j:js.index("var lang=", j)]))
            self.assertGreater(len(ken), 90)
            self.assertEqual(ken, kur, sorted(ken ^ kur))                    # every English text has an Urdu text
            used = set(re.findall(r'(?<![A-Za-z0-9_])t\("([A-Za-z0-9_]+)"\)', js)) | set(re.findall(r'data-k="([A-Za-z0-9_]+)"', html))
            used |= {"m_live", "m_agrispace", "m_bioshield", "m_ecoguard", "m_rotation", "m_validate", "m_ndvi",
                     "lv_Low", "lv_Moderate", "lv_High", "lv_Insufficient_data"}
            self.assertFalse(used - ken, sorted(used - ken))                 # no t("missing_key") anywhere

        def test_fuzz_no_server_errors(self):
            import random
            rng = random.Random(7)
            vals = [None, True, 0, 1, -1, 1e308, float("nan"), "", "abc", "😀" * 40, [], {}, [1, 2], 34.0151, "34.0151",
                    91, -181, "x" * 300, "گندم", "<b>x</b>", 10 ** 30]
            keys = ["latitude", "longitude", "crop", "region", "days", "kc", "soil", "csv", "observations", "water",
                    "current_rabi", "current_kharif", "priorities", "rain_change_pct", "temp_change_c", "market"]
            from contextlib import ExitStack

            def ndvi_ctx():
                st = ExitStack()
                st.enter_context(self._ndvi_fake())
                st.enter_context(mock.patch.object(this, "NDVI_ENABLED", True))
                return st
            contexts = {"live": self._live_fake, "rotation": self._rot_fake, "ndvi": ndvi_ctx}
            for ep in ("live", "agrispace", "bioshield", "ecoguard", "rotation", "validate", "ndvi"):
                ctx = contexts.get(ep, lambda: self.patched(scen=SCEN_NORMAL))
                with ctx():
                    for n in range(40):
                        payload = {"latitude": 34.0, "longitude": 71.5} if n % 3 == 0 else {}
                        for k in rng.sample(keys, rng.randint(0, 6)):
                            payload[k] = rng.choice(vals)
                        if n % 5 == 0:
                            payload["priorities"] = {k: rng.choice(vals) for k in ("water", "soil", "erosion", "forage", "income")}
                        r = self.c.post(f"/api/{ep}", data=json.dumps(payload), content_type="application/json")
                        self.assertIn(r.status_code, (200, 400, 502), (ep, payload))
                        self.assertIsNotNone(r.get_json(silent=True), (ep, payload))

        def _ndvi_fake(self, low_recent=False, fail_first=False, fail_all=False):
            today = datetime.now(timezone.utc).date()
            steps, d = [], today - timedelta(days=3 * 365 + 20)
            while d <= today - timedelta(days=5):
                steps.append(d)
                d += timedelta(days=16)
            mod = {f"A{x.year}{x.timetuple().tm_yday:03d}": x for x in steps}
            keys = list(mod)
            first_kept = next(k for k, x in mod.items() if x >= today - timedelta(days=int(365.25 * 3)))

            def val(x):
                v = 0.35 + 0.2 * math.cos(2 * math.pi * (x.timetuple().tm_yday - 100) / 365)   # peak ~Apr, trough ~Oct
                return v - 0.25 if low_recent and x in steps[-3:] else v

            def fake(path, params):
                ndvi_calls.append(path)
                if fail_all:
                    raise NdviError("down")
                if path.endswith("/dates"):
                    return {"dates": [{"modis_date": k, "calendar_date": x.isoformat()} for k, x in mod.items()]}
                a, b = keys.index(params["startDate"]), keys.index(params["endDate"])
                if fail_first and params["startDate"] == first_kept:
                    raise NdviError("chunk down")
                rows = [{"modis_date": k, "calendar_date": mod[k].isoformat(), "band": params["band"],
                         "data": [-3000 if keys.index(k) % 7 == 3 else round(val(mod[k]) * 10000)]} for k in keys[a:b + 1]]
                return {"scale": "0.0001", "subset": rows}
            return mock.patch.object(this, "ndvi_request", side_effect=fake)

        def test_ndvi_is_off_by_default(self):
            r = self.post("ndvi")
            self.assertEqual(r.status_code, 501)
            self.assertIn("experimental", r.get_json()["detail"])

        def test_ndvi_module(self):
            ndvi_calls.clear()
            with self._ndvi_fake(), mock.patch.object(this, "NDVI_ENABLED", True):
                r = self.post("ndvi")
                d = r.get_json()
                self.assertEqual(r.status_code, 200)
                self.assertEqual(d["module"], "ndvi")
                self.assertEqual(d["assessment"]["level"], "Low")
                self.assertIn("not a field measurement", d["data_categories"]["satellite_observation"]["label"])
                self.assertIn("independence", d)
                m = d["metrics"]
                self.assertIn("Oct", m["usually_bare_months"])
                self.assertNotIn("Apr", m["usually_bare_months"])
                self.assertLess(m["valid_observations"], m["total_observations"])      # fill values were removed
                self.assertLess(d["data_quality"]["coverage_pct"]["NDVI"], 100)
                self.assertGreater(len(d["series"]["ndvi"]), 55)
                self.assertIn("NDVI", d["summary"]["en"])
                self.assertIn("سبزہ", d["summary"]["ur"])
                n = len(ndvi_calls)
                self.post("ndvi")
                self.assertEqual(len(ndvi_calls), n)                                    # cached
            cache.clear()
            with self._ndvi_fake(low_recent=True), mock.patch.object(this, "NDVI_ENABLED", True):
                d = self.post("ndvi").get_json()
            self.assertEqual(d["assessment"]["level"], "High")
            self.assertLessEqual(d["metrics"]["anomaly_vs_same_season"], -0.2)

        def test_ndvi_failures(self):
            with mock.patch.object(this, "NDVI_ENABLED", True):
                with self._ndvi_fake(fail_all=True):
                    self.assertEqual(self.post("ndvi").status_code, 502)
                cache.clear()
                with self._ndvi_fake(fail_first=True):
                    r = self.post("ndvi")
                    self.assertEqual(r.status_code, 200)
                    self.assertEqual(r.get_json()["data_quality"]["request_chunks_failed"], 1)

        def test_ndvi_tab_only_when_enabled(self):
            off = self.c.get("/").get_data(as_text=True)
            self.assertIn("FEATURES={ndvi:false}", off)
            self.assertNotIn("__NDVI__", off)
            with mock.patch.object(this, "NDVI_ENABLED", True):
                on = self.c.get("/").get_data(as_text=True)
            self.assertIn("FEATURES={ndvi:true}", on)

        def test_ndvi_from_real_appeears_values(self):
            rows = [("10-16-2026", 0.3321, 1), ("09-30-2026", 0.3091, 1), ("09-14-2026", 0.2827, 1), ("08-29-2026", 0.3129, 1),
                    ("08-13-2026", 0.3562, 1), ("07-28-2026", 0.2717, 1), ("07-12-2026", 0.2938, 1), ("06-26-2026", 0.2679, 0),
                    ("06-10-2026", 0.3236, 1), ("05-25-2026", 0.3381, 1), ("11-01-2025", 0.2201, 1), ("10-16-2025", 0.2794, 1),
                    ("09-30-2025", 0.2045, 1), ("09-14-2025", 0.2427, 1), ("08-29-2025", 0.313, 0), ("08-13-2025", 0.266, 1),
                    ("07-28-2025", 0.2219, 1), ("07-12-2025", 0.2259, 1), ("06-26-2025", 0.2542, 1), ("06-10-2025", 0.2134, 1)]
            text = "\n".join(f"{d}\t{v}\t{q}\tVI produced" for d, v, q in rows)      # as copied from the AppEEARS Explore page
            ndvi_calls.clear()
            r = self.post("ndvi", {"latitude": 34.0151, "longitude": 71.5249, "csv": text})
            d = r.get_json()
            self.assertEqual(r.status_code, 200)                       # works without the experimental live service
            self.assertEqual(len(ndvi_calls), 0)
            m = d["metrics"]
            self.assertEqual((m["latest_date"], m["latest_ndvi"], m["valid_observations"]), ("2026-10-16", 0.33, 20))
            self.assertEqual(m["greenness_class"], "sparse or young crop")
            self.assertAlmostEqual(m["anomaly_vs_same_season"], 0.097, delta=0.012)     # vs 2025-09-30/10-16/11-01
            self.assertEqual(m["peak_month"], "Aug")
            self.assertEqual(m["usually_bare_months"], [])
            self.assertEqual(d["assessment"]["level"], "Low")
            self.assertIn("AppEEARS", d["data_categories"]["satellite_observation"]["label"])
            self.assertIn("supplied by you", d["data_categories"]["satellite_observation"]["label"])
            self.assertEqual(d["warnings"], [])
            self.assertIn("your file", d["summary"]["en"])

        def test_ndvi_csv_export_formats_and_errors(self):
            head = "ID,Category,Latitude,Longitude,Date,MOD13Q1_061__250m_16_days_NDVI,MOD13Q1_061__250m_16_days_pixel_reliability"
            body = ["Peshawar,,34.0151,71.5249,2026-%02d-%02d,%s,%s" % (m, 10, v, rel) for m, v, rel in
                    [(1, 0.15, 0), (2, 0.2, 0), (3, 0.5, 0), (4, 0.62, 0), (5, 0.3, 0), (6, 0.18, 0), (7, 0.2, 0), (8, 3321, 0),
                     (9, -0.3, 0), (10, 0.4, 3), (11, 0.25, 1), (12, 0.1, 0)]]
            text = head + "\n" + "\n".join(body)
            r = self.post("ndvi", {"latitude": 34.0151, "longitude": 71.5249, "csv": text})
            d = r.get_json()
            self.assertEqual(r.status_code, 200)
            self.assertEqual(d["metrics"]["valid_observations"], 10)          # fill value (-0.3) and cloudy (reliability 3) dropped
            self.assertEqual(d["location"]["latitude"], 34.0151)
            r = self.post("ndvi", {"latitude": 30.0, "longitude": 70.0, "csv": text})   # file point differs from what was entered
            self.assertTrue(any("not the coordinates entered" in w for w in r.get_json()["warnings"]))
            ok = {"latitude": 34.0, "longitude": 71.5}
            for bad in ("hello world", "Date,Value\n2026-01-01,5", head + "\nA,,1,1,2026-01-01,0.3,0\nB,,2,2,2026-01-17,0.3,0",
                        "x" * 250000):
                self.assertEqual(self.post("ndvi", {**ok, "csv": bad}).status_code, 400, bad[:40])
            self.assertEqual(self.post("ndvi", ok).status_code, 501)           # no file and live service off

        def test_ndvi_tab_always_present_with_file_box(self):
            html = self.c.get("/").get_data(as_text=True)
            for needle in ('ndvi:["ndvibox"]', "ndvicsv", "ndvi_none", "ndvi_help"):
                self.assertIn(needle, html)

        def test_hostile_inputs_are_clean_400s(self):
            ok = {"latitude": 34.0, "longitude": 71.5}
            cell = "date,tmax\n2026-10-01," + "9" * 150000                      # one cell above the csv module's 128 KB limit
            ndvi_cell = "date,NDVI\n2026-10-01," + "9" * 150000
            for path, body in (("validate", {**ok, "csv": cell}), ("ndvi", {**ok, "csv": ndvi_cell})):
                self.assertEqual(self.post(path, body).status_code, 400, path)
            for path in ("validate", "ndvi", "rotation"):                          # JSON nested far beyond Python's recursion limit
                r = self.c.post(f"/api/{path}", data='{"a":' * 20000 + "1" + "}" * 20000, content_type="application/json")
                self.assertEqual(r.status_code, 400 if path != "rotation" else 413, path)

        def test_rotation_warns_when_not_meaningful(self):
            with self._rot_fake():
                south = self.post("rotation", {"latitude": -33.9, "longitude": 18.4}).get_json()
                home = self.post("rotation", {"latitude": 34.0, "longitude": 71.5}).get_json()
                zero = self.post("rotation", {"latitude": 34.0, "longitude": 71.5,
                                              "priorities": {k: 0 for k in ("water", "soil", "erosion", "forage", "income", "resilience", "pest")}}).get_json()
            self.assertTrue(any("NOT meaningful" in w for w in south["warnings"]))
            self.assertFalse(any("NOT meaningful" in w for w in home["warnings"]))
            self.assertTrue(any("All priorities are 0" in w for w in zero["warnings"]))

        def test_labels_are_associated_with_inputs(self):
            html = self.c.get("/").get_data(as_text=True)
            for iid in ("lat", "lon", "days", "crop", "soil", "q", "valcsv", "ndvicsv"):
                self.assertTrue(f'for="{iid}"' in html and f'id="{iid}"' in html, iid)
            self.assertTrue('id="out" aria-live="polite"' in html)

    print(f"\nSpace2Earth v{VERSION} offline test-suite\n")
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(T)
    res = unittest.TextTestRunner(verbosity=2).run(suite)
    try:
        write_test_summary(res)
        print("\nSaved test_results.json")
    except OSError as exc:
        print("\n(could not write test_results.json:", exc, ")")
    return 0 if res.wasSuccessful() else 1


def live_check() -> int:
    """One real NASA POWER round-trip (Peshawar). Verifies parameter names/units on YOUR network."""
    print("Calling NASA POWER for Peshawar (34.0151, 71.5249) ...")
    try:
        b = get_power_bundle(34.0151, 71.5249, 30)
    except PowerError as exc:
        print("FAILED:", exc)
        return 1
    print("Window:", b["start"], "->", b["end"])
    print("Coverage %:", json.dumps(b["coverage"]))
    print("Missing:", b["missing"] or "none", "| climatology:", "yes" if b["climatology"] else "no")
    for mod, fn in MODULES.items():
        loc = Loc(34.0151, 71.5249, "wheat", "Peshawar", 30, None)
        env = build_envelope(mod, loc, b, fn(b, loc))
        print(f"\n[{mod}] level = {env['assessment']['level']}")
        for r_ in env["assessment"]["reasons"]:
            print("  -", r_)
    try:
        lv = fetch_live(34.0151, 71.5249)
        print("\n[live] Open-Meteo OK:", lv["current"], "| forecast days:", len(lv["dates"]))
    except PowerError as exc:
        print("\n[live] FAILED:", exc)
        return 1
    try:
        tr = fetch_trend(34.0151, 71.5249)
        print("\n[trend] NASA POWER monthly OK:", tr if tr else "too few valid years (trend disabled)")
    except PowerError as exc:
        print("\n[trend] FAILED (rotation tab still works without it):", exc)
    try:
        nd = fetch_ndvi_series(34.0151, 71.5249, 1)
        ok = sum(1 for _, v in nd["series"] if v is not None)
        print(f"\n[ndvi] MODIS via ORNL DAAC OK: {ok} valid of {len(nd['series'])} observations. "
              f"To show the experimental tab set ENABLE_NDVI=1.")
    except PowerError as exc:
        print("\n[ndvi] FAILED (leave ENABLE_NDVI unset; everything else works):", exc)
    return 0


def run_demo() -> int:
    """One command for reviewers: tests + live data checks (+ validation if station.csv exists) -> demo_report.json."""
    print(f"Space2Earth v{VERSION} demo. Independent project, not affiliated with NASA.\n")
    report = {"version": VERSION, "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "independence": INDEPENDENCE, "steps": []}
    rc = run_tests()
    try:
        with open("test_results.json", encoding="utf-8") as fh:
            tr = json.load(fh)
    except (OSError, ValueError):
        tr = {}
    report["steps"].append({"step": "offline_tests", "ok": rc == 0,
                            **{k: tr.get(k) for k in ("tests_run", "passed", "failures", "errors")}})
    lat, lon = 34.0151, 71.5249

    def attempt(name, fn):
        try:
            report["steps"].append({"step": name, "ok": True, "detail": fn()})
            print(f"[ok] {name}")
        except Exception as exc:       # network problems must not hide the other results
            report["steps"].append({"step": name, "ok": False, "error": str(exc)})
            print(f"[FAILED] {name}: {exc}")
    attempt("nasa_power_daily", lambda: {"coverage_pct": get_power_bundle(lat, lon, 30)["coverage"]})
    attempt("open_meteo_live", lambda: {"forecast_days": len(fetch_live(lat, lon)["dates"])})
    attempt("nasa_power_trend", lambda: fetch_trend(lat, lon) or "too few valid years (trend disabled)")
    attempt("ndvi_modis_ornl_experimental",
            lambda: {"valid_observations": sum(1 for _, v in fetch_ndvi_series(lat, lon, 1)["series"] if v is not None)})
    if os.path.exists("station.csv"):
        attempt("validation_station_csv", lambda: run_validation(read_obs_csv("station.csv"), lat, lon)["results"])
    else:
        report["steps"].append({"step": "validation_station_csv", "ok": None,
                                "detail": "station.csv not found: validation was NOT run"})
        print("[skipped] validation: put your own observations in station.csv to run it")
    with open("demo_report.json", "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False, default=str)
    print("\nSaved demo_report.json")
    return 0 if all(st["ok"] in (True, None) for st in report["steps"]) else 1


if __name__ == "__main__":
    _flags = {"--test", "--demo", "--live", "--validate", "--lat", "--lon", "--help"}
    _bad = [a for a in sys.argv[1:] if a.startswith("--") and a not in _flags]
    if "--help" in sys.argv or "-h" in sys.argv or _bad:
        print(__doc__)
        if _bad:
            print("Unknown option(s):", " ".join(_bad))
        sys.exit(2 if _bad else 0)
    if "--test" in sys.argv:
        sys.exit(run_tests())
    if "--demo" in sys.argv:
        sys.exit(run_demo())
    if "--live" in sys.argv:
        sys.exit(live_check())
    if "--validate" in sys.argv:
        def _arg(flag):
            i = sys.argv.index(flag)
            return sys.argv[i + 1] if i + 1 < len(sys.argv) else None
        try:
            sys.exit(validate_cli(_arg("--validate"), float(_arg("--lat")), float(_arg("--lon"))))
        except (TypeError, ValueError):
            print("usage: python space2earth.py --validate station.csv --lat 34.0151 --lon 71.5249")
            sys.exit(2)
    host, port = os.getenv("HOST", "127.0.0.1"), int(os.getenv("PORT", 5000))
    log.info("Starting Space2Earth v%s on http://%s:%d", VERSION, host, port)
    app.run(host=host, port=port, debug=False, threaded=True)
