#!/bin/bash
# check.sh — состояние сервера: версии, процессы, рекордер, ресурсы, текущие прогоны
cd "$(dirname "$0")/.."
echo "== версии"
[ -x spred ] && strings spred | grep -m1 "SPRED v3" | sed 's/^ */бинарник spred: /'
echo "исходник: $(grep -m1 -o 'spred.cpp v[0-9.]*' src/spred.cpp)"
for f in tools/xscan.py tools/xtune.py tools/pickdays.py; do echo "$f: $(grep -m1 -o 'v[0-9]\.[0-9]' $f)"; done
echo "== процессы"; pgrep -af "spred |xrec|xscan|xtune" | cut -c1-120
echo "== рекордер"; systemctl is-active xrec 2>/dev/null; ls data/x 2>/dev/null | tail -1; ls data/x 2>/dev/null | wc -l
echo "== ресурсы"; free -m | sed -n 2p; df -h . | tail -1
echo "== прогоны"; for f in data/paper_*.out; do [ -f "$f" ] && echo "$f $(du -h "$f" | cut -f1) последняя: $(grep -a '^\[..:..:..\] mid' "$f" | tail -1 | cut -c1-10)"; done
