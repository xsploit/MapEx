#!/usr/bin/env python3
"""Check small public layer summaries; never download feature records."""
import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

BASE = 'https://maps.rdck.bc.ca/server2/rest/services/RDCK_Public_Web_Map_Service_MIL/MapServer'
LAYERS = [('address_points', 20, 'last_edited_date'), ('cadastre', 25, 'Last_Edited_Date')]


def check_layer(layer_id, edit_field):
    stats = [
        {'statisticType': 'count', 'onStatisticField': 'OBJECTID', 'outStatisticFieldName': 'feature_count'},
        {'statisticType': 'max', 'onStatisticField': edit_field, 'outStatisticFieldName': 'latest_edit'},
    ]
    params = urllib.parse.urlencode({'where': '1=1', 'returnGeometry': 'false', 'f': 'json', 'outStatistics': json.dumps(stats)})
    request = urllib.request.Request(f'{BASE}/{layer_id}/query?{params}', headers={'User-Agent': 'MapEx-layer-status/1.0 (monthly status check)'})
    with urllib.request.urlopen(request, timeout=30) as response:
        data = json.load(response)
    if data.get('error'):
        raise RuntimeError(f"RDCK layer {layer_id}: {data['error'].get('message', 'service error')}")
    features = data.get('features') or []
    attrs = features[0].get('attributes', {}) if features else {}
    count, edited = attrs.get('feature_count'), attrs.get('latest_edit')
    if not isinstance(count, int) or count < 0 or not isinstance(edited, (int, float)):
        raise RuntimeError(f'RDCK layer {layer_id}: incomplete statistics')
    return {'feature_count': count, 'latest_edit_epoch_ms': int(edited)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, default=Path('data/rdck-layer-status.json'))
    args = parser.parse_args()
    layers = {}
    for index, (name, layer_id, field) in enumerate(LAYERS):
        if index:
            time.sleep(3)
        layers[name] = check_layer(layer_id, field)
    result = {'source': BASE, 'note': 'Advisory only: count and latest edit may miss changes; this does not refresh cached features.', 'layers': layers}
    encoded = json.dumps(result, indent=2, sort_keys=True) + '\n'
    args.output.parent.mkdir(parents=True, exist_ok=True)
    previous = args.output.read_text(encoding='utf-8') if args.output.exists() else None
    if previous != encoded:
        temporary = args.output.with_name(args.output.name + '.tmp')
        temporary.write_text(encoded, encoding='utf-8')
        temporary.replace(args.output)
        print('Layer indicators changed, or no previous baseline exists.')
    else:
        print('No change detected in count or latest-edit indicators; caches were not refreshed.')
    print(encoded, end='')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(f'RDCK check failed; previous status was preserved: {error}', file=sys.stderr)
        sys.exit(1)
