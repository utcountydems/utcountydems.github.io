#!/usr/bin/env python3
"""
patch_voter_fields.py
=====================
Reads mock_vr.csv and patches phone, sd, ssbd, lsbd into the existing
per-precinct voter JSON files without re-geocoding.
"""
import csv, json, os

INPUT_CSV  = "assets/mock_vr.csv"
OUTPUT_DIR = "assets/voters"

def na(val):
    v = (val or '').strip()
    return '' if v.upper() in ('NA', 'N/A', 'NONE', '') else v

print(f"Reading {INPUT_CSV} …")
extra = {}   # vid -> {phone, sd, ssbd, lsbd}
with open(INPUT_CSV, newline='', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        vid = str(row.get('vid', '')).strip()
        if vid:
            extra[vid] = {
                'phone': na(row.get('phone')),
                'sd':    na(row.get('sd')),
                'ssbd':  na(row.get('ssbd')),
                'lsbd':  na(row.get('lsbd')),
            }
print(f"  {len(extra):,} voter records indexed")

patched_files = patched_voters = 0
for fname in os.listdir(OUTPUT_DIR):
    if not fname.endswith('.json'):
        continue
    path = os.path.join(OUTPUT_DIR, fname)
    with open(path, encoding='utf-8') as f:
        records = json.load(f)
    if not isinstance(records, list):
        continue
    changed = False
    for rec in records:
        if not isinstance(rec, dict):
            continue
        vid = str(rec.get('vid', ''))
        if vid in extra:
            for field, val in extra[vid].items():
                if rec.get(field) != val:
                    rec[field] = val
                    changed = True
            patched_voters += 1
    if changed:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(records, f, separators=(',', ':'))
        patched_files += 1

print(f"Patched {patched_voters:,} voter records across {patched_files} files.")
