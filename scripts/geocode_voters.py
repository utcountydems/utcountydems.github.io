#!/usr/bin/env python3
"""
geocode_voters.py
=================
Matches the voter registration CSV to parcel shapefile coordinates and writes
per-precinct JSON files to assets/voters/{precinctId}.json for the canvass map.

USAGE
-----
  # Use parcel shapefile (recommended — fast, no API calls):
  python scripts/geocode_voters.py --parcels assets/parcels/Parcels_Utah_LIR.shp

  # Fallback: Census batch geocoder (slow, ~3 hours for 181K rows):
  python scripts/geocode_voters.py

REQUIREMENTS
------------
  pip install requests        (only needed for Census fallback)

HOW PARCEL MATCHING WORKS
--------------------------
The shapefile contains one polygon per parcel with a PARCEL_ADD field
("2761 S 3300 E") and PARCEL_CIT field ("Spanish Fork").  The script
computes each polygon's centroid, converts from Web Mercator to WGS84
lat/lng, then joins to the voter CSV by normalized address + city.
"""

import csv
import json
import math
import os
import re
import struct
import sys
import time
import argparse
from io import StringIO

# ── Configuration ────────────────────────────────────────────────────────────

INPUT_CSV     = "assets/mock_vr.csv"
OUTPUT_DIR    = "assets/voters"
CACHE_FILE    = "scripts/.geocode_cache.json"
BATCH_SIZE    = 1000
REQUEST_DELAY = 1.0
CENSUS_URL    = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"

# Utah County bounding box (sanity check)
LAT_MIN, LAT_MAX =  39.82,  40.66
LNG_MIN, LNG_MAX = -112.25, -111.42

# ── Address helpers ───────────────────────────────────────────────────────────

def na(val):
    v = (val or '').strip()
    return '' if v.upper() in ('NA', 'N/A', 'NONE', '') else v

def build_street(row):
    """Build street portion of address from voter file columns."""
    parts = []
    house = na(row.get('house_number'))
    hsuf  = na(row.get('house_number_suffix'))
    dpre  = na(row.get('street_direction'))
    st    = na(row.get('street'))
    dsuf  = na(row.get('street_direction_suffix'))
    stype = na(row.get('street_type'))
    utype = na(row.get('unit_type'))
    unum  = na(row.get('unit_number'))

    if house:
        parts.append(house + hsuf)
    if dpre:
        parts.append(dpre)
    if st:
        parts.append(st)
    if dsuf:
        parts.append(dsuf)
    if stype:
        parts.append(stype)
    # Unit info kept separate — not used for parcel address matching
    return ' '.join(parts), (f"{utype} {unum}".strip() if utype or unum else '')

def build_full_address(row):
    street, unit = build_street(row)
    city   = na(row.get('city'))
    zip_   = na(row.get('zip'))
    parts  = [p for p in [street, unit, city, f"UT {zip_}"] if p]
    return ', '.join(parts)

def normalize_addr(addr, city=''):
    """Uppercase, collapse whitespace, strip punctuation for matching."""
    s = re.sub(r'\s+', ' ', (addr or '').upper().strip())
    c = re.sub(r'\s+', ' ', (city or '').upper().strip())
    return s, c

# ── Web Mercator → WGS84 ──────────────────────────────────────────────────────

def webmercator_to_latlon(x, y):
    """Convert EPSG:3857 (meters) to EPSG:4326 (lat/lng degrees)."""
    lng = x / 20037508.342789244 * 180.0
    lat = math.degrees(2 * math.atan(math.exp(y * math.pi / 20037508.342789244)) - math.pi / 2)
    return lat, lng

# ── Shapefile reader (no extra libraries needed) ──────────────────────────────

def read_dbf_fields(f):
    """Read field descriptors from an open .dbf file. File position must be at byte 32."""
    fields = []
    while True:
        desc = f.read(32)
        if not desc or desc[0] == 0x0D:
            break
        name   = desc[:11].replace(b'\x00', b'').decode('ascii', errors='ignore')
        length = desc[16]
        fields.append((name, length))
    return fields

def read_dbf(dbf_path, wanted_fields):
    """
    Generator: yields one dict per record with only the requested field names.
    Skips deleted records (flag byte = 0x2A).
    """
    with open(dbf_path, 'rb') as f:
        header      = f.read(32)
        num_records = struct.unpack('<I', header[4:8])[0]
        header_size = struct.unpack('<H', header[8:10])[0]
        record_size = struct.unpack('<H', header[10:12])[0]
        f.seek(32)
        fields = read_dbf_fields(f)
        f.seek(header_size)
        wanted_set = set(wanted_fields)
        for _ in range(num_records):
            raw = f.read(record_size)
            if not raw:
                break
            if raw[0] == 0x2A:   # deleted record
                yield None
                continue
            offset = 1
            record = {}
            for name, length in fields:
                val = raw[offset:offset + length].decode('ascii', errors='ignore').strip()
                if name in wanted_set:
                    record[name] = val
                offset += length
            yield record

def shp_polygon_centroid(content):
    """
    Parse a shapefile polygon record content (after the 4-byte shape type)
    and return the simple centroid (average of all ring vertices) in the
    file's native coordinate system.
    Returns None for null or unsupported shapes.
    """
    shape_type = struct.unpack('<I', content[:4])[0]
    if shape_type == 0:
        return None
    if shape_type not in (5, 15, 25):   # Polygon, PolygonZ, PolygonM
        return None
    num_parts  = struct.unpack('<I', content[36:40])[0]
    num_points = struct.unpack('<I', content[40:44])[0]
    pts_offset = 44 + num_parts * 4
    xs, ys = [], []
    for i in range(num_points):
        x, y = struct.unpack('<dd', content[pts_offset + i * 16: pts_offset + i * 16 + 16])
        xs.append(x)
        ys.append(y)
    if not xs:
        return None
    return sum(xs) / len(xs), sum(ys) / len(ys)

def read_shp_centroids(shp_path):
    """
    Read polygon centroids from a .shp file.
    Returns a list (one entry per record) of (x, y) tuples or None.
    """
    centroids = []
    with open(shp_path, 'rb') as f:
        f.read(100)   # skip 100-byte file header
        while True:
            rec_header = f.read(8)
            if len(rec_header) < 8:
                break
            content_len = struct.unpack('>I', rec_header[4:8])[0] * 2
            content     = f.read(content_len)
            if len(content) < 4:
                break
            centroids.append(shp_polygon_centroid(content))
    return centroids

# ── Parcel join ───────────────────────────────────────────────────────────────

# Unit-suffix pattern to strip from parcel PARCEL_ADD before indexing
# e.g. "1377 S 100 E UNIT 1" → "1377 S 100 E"
UNIT_STRIP_RE = re.compile(
    r'\s+(UNIT|APT|APARTMENT|#|STE|SUITE|LOT|BLDG|BLDG\.|RM|ROOM|FL|FLOOR)\s*\S*$',
    re.IGNORECASE
)

def strip_parcel_unit(addr):
    return UNIT_STRIP_RE.sub('', addr).strip()

def build_parcel_index(shp_path):
    """
    Read the parcel shapefile and build two lookup dicts:
      index_with_city  : (NORM_ADDR, NORM_CITY) → (lat, lng)
      index_no_city    : NORM_ADDR → (lat, lng)   fallback for empty/mismatched city

    Units are stripped from PARCEL_ADD so "1377 S 100 E UNIT 1" indexes
    as "1377 S 100 E", matching voters who only have the base address.
    """
    dbf_path = shp_path.replace('.shp', '.dbf')
    print(f"  Reading parcel geometry from {os.path.basename(shp_path)} …")
    centroids = read_shp_centroids(shp_path)
    print(f"  {len(centroids):,} parcel shapes read")

    print(f"  Reading parcel attributes from {os.path.basename(dbf_path)} …")
    index_with_city = {}
    index_no_city   = {}
    skipped = 0

    for i, record in enumerate(read_dbf(dbf_path, ['PARCEL_ADD', 'PARCEL_CIT'])):
        if record is None or i >= len(centroids) or centroids[i] is None:
            skipped += 1
            continue
        raw_addr = (record.get('PARCEL_ADD') or '').strip()
        city     = (record.get('PARCEL_CIT') or '').strip()
        if not raw_addr:
            skipped += 1
            continue
        cx, cy = centroids[i]
        lat, lng = webmercator_to_latlon(cx, cy)
        if not (LAT_MIN <= lat <= LAT_MAX and LNG_MIN <= lng <= LNG_MAX):
            skipped += 1
            continue

        # Strip unit suffix so multi-unit parcels match on base address
        addr = strip_parcel_unit(raw_addr)
        norm_addr, norm_city = normalize_addr(addr, city)

        key_full  = (norm_addr, norm_city)
        key_noct  = norm_addr   # city-less fallback

        if key_full not in index_with_city:
            index_with_city[key_full] = (lat, lng)
        if key_noct not in index_no_city:
            index_no_city[key_noct] = (lat, lng)

    print(f"  {len(index_with_city):,} address+city keys  |  "
          f"{len(index_no_city):,} address-only keys  ({skipped:,} skipped)")
    return index_with_city, index_no_city

# ── Census batch geocoder (fallback) ─────────────────────────────────────────

def geocode_batch(batch):
    try:
        import requests
    except ImportError:
        print("    'requests' not installed. Run: pip install requests")
        return {}

    lines = []
    for uid, street, city, state, zipcode in batch:
        def esc(s): return str(s).replace('"', '""')
        lines.append(f'"{esc(uid)}","{esc(street)}","{esc(city)}","{esc(state)}","{esc(zipcode)}"')

    payload = '\n'.join(lines)
    files   = {'addressFile': ('addresses.csv', payload, 'text/plain')}
    data    = {'benchmark': 'Public_AR_Current'}

    try:
        resp = requests.post(CENSUS_URL, files=files, data=data, timeout=120)
        if resp.status_code != 200:
            print(f"    Census HTTP {resp.status_code}")
            return {}

        if not hasattr(geocode_batch, '_debug_done'):
            geocode_batch._debug_done = True
            print(f"    [debug] Census preview: {resp.text.splitlines()[:2]}")

        results = {}
        reader  = csv.reader(StringIO(resp.text))
        for row in reader:
            if len(row) < 6:
                continue
            uid    = row[0].strip().strip('"')
            status = row[2].strip()
            if status in ('Match', 'Tie'):
                try:
                    parts = row[5].strip().split(',')
                    lng = float(parts[0])
                    lat = float(parts[1])
                    if LAT_MIN <= lat <= LAT_MAX and LNG_MIN <= lng <= LNG_MAX:
                        results[uid] = (lat, lng)
                except (ValueError, IndexError):
                    pass
        return results

    except Exception as exc:
        print(f"    Batch error: {exc}")
        return {}

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Build per-precinct voter JSON for canvass map')
    parser.add_argument('--parcels', metavar='SHP',
                        help='Path to parcel shapefile (.shp) — fast, no API calls')
    parser.add_argument('--input', default=INPUT_CSV,
                        help=f'Voter CSV (default: {INPUT_CSV})')
    args = parser.parse_args()

    # ── Read voter CSV ──
    print(f"Reading {args.input} …")
    rows = []
    with open(args.input, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            rows.append(row)
    print(f"  {len(rows):,} voters loaded")

    precinct_rows = {}
    id_to_row     = {}
    for row in rows:
        pid = na(row.get('precinct'))
        uid = str(row.get('id', '')).strip()
        if pid:
            precinct_rows.setdefault(pid, []).append(row)
        if uid:
            id_to_row[uid] = row
    print(f"  {len(precinct_rows)} precincts")

    # ── Get coordinates ──
    coords_cache = {}

    if args.parcels:
        # ─── Option A: parcel shapefile join ───
        print(f"\nBuilding parcel address index …")
        index_with_city, index_no_city = build_parcel_index(args.parcels)

        print(f"\nMatching voters to parcels …")
        matched = matched_fallback = unmatched = 0
        for row in rows:
            uid = str(row.get('id', '')).strip()
            if not uid:
                continue
            street, _unit  = build_street(row)
            city           = na(row.get('city'))
            norm_addr, norm_city = normalize_addr(street, city)

            # Pass 1: exact address + city match
            coords = index_with_city.get((norm_addr, norm_city))
            # Pass 2: address-only fallback (handles empty/mismatched city in parcel)
            if not coords:
                coords = index_no_city.get(norm_addr)
                if coords:
                    matched_fallback += 1

            if coords:
                coords_cache[uid] = list(coords)
                matched += 1
            else:
                unmatched += 1

        print(f"  Matched (exact)   : {matched - matched_fallback:,}")
        print(f"  Matched (no-city) : {matched_fallback:,}")
        print(f"  Total matched     : {matched:,}")
        print(f"  Unmatched         : {unmatched:,}  (not in parcel file)")

    else:
        # ─── Option B: Census batch geocoder ───
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE) as f:
                coords_cache = json.load(f)
            print(f"\nCache: {len(coords_cache):,} previously geocoded")

        to_geocode = []
        for row in rows:
            uid = str(row.get('id', '')).strip()
            if not uid or uid in coords_cache:
                continue
            street, _ = build_street(row)
            city       = na(row.get('city'))
            zipcode    = na(row.get('zip'))
            if street and city:
                to_geocode.append((uid, street, city, 'UT', zipcode))

        print(f"\nAddresses to geocode: {len(to_geocode):,}")
        total_batches = (len(to_geocode) + BATCH_SIZE - 1) // BATCH_SIZE

        for batch_num, i in enumerate(range(0, len(to_geocode), BATCH_SIZE), 1):
            batch = to_geocode[i:i + BATCH_SIZE]
            print(f"  Batch {batch_num}/{total_batches} ({len(batch)}) …", end=' ', flush=True)
            results = geocode_batch(batch)
            print(f"{len(results)}/{len(batch)} matched")
            coords_cache.update({uid: list(c) for uid, c in results.items()})
            os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
            with open(CACHE_FILE, 'w') as f:
                json.dump(coords_cache, f)
            if i + BATCH_SIZE < len(to_geocode):
                time.sleep(REQUEST_DELAY)

    # ── Write per-precinct JSON ──
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"\nWriting per-precinct files to {OUTPUT_DIR}/ …")
    written = placed = skipped = 0

    for pid, precinct_voter_rows in sorted(precinct_rows.items()):
        records = []
        for row in precinct_voter_rows:
            uid    = str(row.get('id', '')).strip()
            coords = coords_cache.get(uid)
            if not coords:
                skipped += 1
                continue
            lat, lng   = coords
            street, unit = build_street(row)
            city       = na(row.get('city'))
            zip_       = na(row.get('zip'))
            addr_parts = [p for p in [street, unit, city, f"UT {zip_}"] if p]
            records.append({
                'id':       uid,
                'name':     na(row.get('name')),
                'party':    na(row.get('party')),
                'address':  ', '.join(addr_parts),
                'precinct': na(row.get('precinct')),
                'hd':       na(row.get('hd')),
                'v_score':  na(row.get('v_score')),
                'decile':   na(row.get('decile')),
                'lat':      lat,
                'lng':      lng,
            })
            placed += 1

        if records:
            out = os.path.join(OUTPUT_DIR, f"{pid}.json")
            with open(out, 'w', encoding='utf-8') as f:
                json.dump(records, f, separators=(',', ':'))
            written += 1

    print(f"\nDone.")
    print(f"  Precinct files : {written}")
    print(f"  Voters on map  : {placed:,}")
    print(f"  No coordinates : {skipped:,}")

if __name__ == '__main__':
    main()
