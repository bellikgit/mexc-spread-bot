#!/usr/bin/env python3
"""Offline regression suite. No exchange connectivity and no live confirmation."""
import argparse
import difflib
import gzip
import json
import os
from pathlib import Path
import re
import struct
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]

def run(cmd, env=None, timeout=120):
    result = subprocess.run(cmd, cwd=ROOT, env=env, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f'{cmd[0]} exited {result.returncode}\n{result.stdout[-4000:]}\n{result.stderr[-4000:]}')
    return result.stdout

def varint(n):
    out = bytearray()
    while n > 127:
        out.append((n & 127) | 128)
        n >>= 7
    out.append(n)
    return bytes(out)

def field(n, value):
    if isinstance(value, str):
        value = value.encode()
    return varint(n * 8 + 2) + varint(len(value)) + value

def integer(n, value):
    return varint(n * 8) + varint(value)

def same(a, b, name):
    if a != b:
        diff = ''.join(difflib.unified_diff(a.splitlines(True), b.splitlines(True), 'legacy', 'modular'))
        raise RuntimeError(f'{name} mismatch\n{diff[:6000]}')

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-dir', default='build')
    args = parser.parse_args()
    build = (ROOT / args.build_dir).resolve()
    build.mkdir(parents=True, exist_ok=True)
    env = {k: v for k, v in os.environ.items() if not k.startswith('MM_')}
    run(['cc', '-shared', '-fPIC', str(ROOT / 'tests/offline_guard.c'), '-o', str(build / 'offline_guard.so')])
    env['LD_PRELOAD'] = str(build / 'offline_guard.so')
    env['MM_LIVE'] = '0'
    reference = build / 'reference'
    reference.mkdir(exist_ok=True)
    (reference / 'spred_legacy.cpp').write_bytes(gzip.decompress((ROOT / 'tests/reference/spred_legacy.cpp.gz').read_bytes()))
    reference_binary = build / 'test_legacy'
    reference_inputs = [ROOT / 'tests/test_main.cpp', ROOT / 'tests/cases.hpp',
                        ROOT / 'tests/reference/spred_legacy.cpp.gz', Path(__file__)]
    if not reference_binary.exists() or any(p.stat().st_mtime > reference_binary.stat().st_mtime for p in reference_inputs):
        run([os.environ.get('CXX', 'g++'), '-std=c++17', '-O2', '-DSPRED_LEGACY_REFERENCE', '-I' + str(build),
             '-I' + str(ROOT / 'tests'), str(ROOT / 'tests/test_main.cpp'), '-o', str(reference_binary),
             '-lssl', '-lcrypto', '-lpthread'], timeout=240)
    programs = [build / 'test_legacy', build / 'test_bot']
    snapshots = [run([str(p)], env) for p in programs]
    for p, output in zip(programs, snapshots):
        (build / (p.name + '.snapshot.txt')).write_text(output)
    same(*snapshots, 'unit/accounting snapshots')
    print(snapshots[1].splitlines()[-1] + '; exact match with original implementation')

    # Replay still requests symbol metadata in the original program. Replace only
    # curl's metadata response; all native connect()/DNS calls remain denied.
    fakebin = build / 'fakebin'
    fakebin.mkdir(exist_ok=True)
    metadata = json.dumps({'symbol': 'TESTUSDT', 'baseAsset': 'TEST', 'quoteAsset': 'USDT',
                           'tickSize': '0.01', 'stepSize': '0.01', 'minQuoteAmount': '1',
                           'orderTypes': ['LIMIT_MAKER', 'LIMIT', 'MARKET']})
    mock = fakebin / 'curl'
    mock.write_text('#!/usr/bin/env python3\nimport sys\n'
                    'expected="https://127.0.0.1:1/api/v3/exchangeInfo?symbol=TESTUSDT"\n'
                    'if expected not in sys.argv[1:]: sys.exit(97)\n'
                    f'print({metadata!r})\n')
    mock.chmod(0o755)
    env.update(PATH=str(fakebin) + os.pathsep + env.get('PATH', ''), MM_API_HOST='127.0.0.1:1',
               MM_REPLAY_SPEED='0', MM_SEED='2026')
    raw = build / 'synthetic.raw'
    with raw.open('wb') as stream:
        for i in range(1000):
            timestamp = 1000000 + i * 100
            mid = 100 + ((i // 30) % 5 - 2) * 0.03
            bid, ask = f'{mid - 0.05:.2f}', f'{mid + 0.05:.2f}'
            lv = lambda px: field(1, px) + field(2, '10')
            depth = field(1, lv(ask)) + field(2, lv(bid))
            ticker = field(1, bid) + field(2, '10') + field(3, ask) + field(4, '10')
            deal = field(1, bid if i % 2 else ask) + field(2, '3') + integer(3, 2 if i % 2 else 1) + integer(4, timestamp)
            body = field(303, depth) + field(315, ticker) + field(314, field(1, deal))
            payload = field(1, 'offline-test') + field(3, 'TESTUSDT') + integer(6, timestamp) + body
            stream.write(struct.pack('<dI', timestamp, len(payload)) + payload)
    env['MM_REPLAY'] = str(raw)
    scenarios = [{}, {'MM_QUEUE_MODE': '1', 'MM_LAT_DIST': '1', 'MM_LAT_P90_MULT': '2.0'},
                 {'MM_PRINT_FILTER': '0', 'MM_PRINT_FILL_P': '0.4', 'MM_LOG_DEALS': '1'}]
    for index, extra in enumerate(scenarios):
        outputs = []
        for program in programs:
            result = run([str(program), '--run-cli', 'TESTUSDT', '0', '20', '22'], env | extra)
            (build / f'replay-{index}-{program.name}.txt').write_text(result)
            # Only wall-clock labels are nondeterministic. All counters and P&L stay intact.
            outputs.append(re.sub(r'\b\d{2}:\d{2}:\d{2}(?:\.\d{3})?\b', '<wallclock>', result))
        same(*outputs, f'replay scenario {index + 1}')
        if 'TESTUSDT' not in outputs[1] or 'SUMMARY' not in outputs[1] or '[FILL ' not in outputs[1]:
            raise RuntimeError('Replay did not reach summary')
        print(f'PASS offline replay {index + 1}: exact normalized output match (1000 frames)')

    for label, extra, arguments, expected in [
        ('missing live credentials', {'MM_LIVE': '1'}, [], 2),
        ('missing live confirmation', {'MM_LIVE': '1', 'MM_API_KEY': 'offline-dummy', 'MM_API_SECRET': 'offline-dummy'}, [], 2),
        ('too many symbols', {}, [','.join(f'S{i}USDT' for i in range(11))], 1),
        ('missing replay', {'MM_REPLAY': str(build / 'does-not-exist.raw')}, [], 1),
    ]:
        safeenv = {k: v for k, v in env.items() if not k.startswith('MM_')} | extra
        for program in programs:
            result = subprocess.run([str(program), '--run-cli'] + arguments, cwd=ROOT, env=safeenv,
                                    capture_output=True, text=True, timeout=15)
            if result.returncode != expected:
                raise RuntimeError(f'{label}: expected {expected}, got {result.returncode}')
        print('PASS guard: ' + label)

    headers = sorted((ROOT / 'include').rglob('*.hpp'))
    for header in headers:
        subprocess.run([os.environ.get('CXX', 'g++'), '-std=c++17', '-Iinclude', '-fsyntax-only', '-x', 'c++', '-'],
                       cwd=ROOT, input=f'#include "{header.relative_to(ROOT / "include")}"\n', text=True, check=True,
                       capture_output=True, timeout=60)
    print(f'PASS {len(headers)} self-contained public headers')
    tools = sorted((ROOT / 'tools').glob('*.py'))
    run([sys.executable, '-m', 'py_compile', *map(str, tools)])
    shells = sorted((ROOT / 'scripts').rglob('*.sh'))
    for script in shells:
        run(['bash', '-n', str(script)])
    print(f'PASS syntax: {len(tools)} Python tools, {len(shells)} shell scripts')
    print('All checks passed. No real exchange connection or trade was performed.')

if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, subprocess.SubprocessError) as error:
        print(f'FAIL: {error}', file=sys.stderr)
        sys.exit(1)
