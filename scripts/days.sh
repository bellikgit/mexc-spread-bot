#!/bin/bash
# days.sh — xscan по каждому дню записи (для pickdays.py). Пропускает готовые дни. Последний (неполный) день пересчитывает.
cd "$(dirname "$0")/.."; R=rep/days; mkdir -p $R
while pgrep -f "xtune.py|xscan.py" >/dev/null; do sleep 30; done
LAST=$(ls data/x | sed -n 's/^xr_\([0-9]\{8\}\)_.*/\1/p' | sort -u | tail -1)
for D in $(ls data/x | sed -n 's/^xr_\([0-9]\{8\}\)_.*/\1/p' | sort -u); do
  [ -f $R/xscan_$D.csv ] && [ "$D" != "$LAST" ] && continue
  echo "день $D: $(ls data/x/xr_${D}_*.csv.gz | wc -l) ч"
  nice -n 19 python3 tools/xscan.py data/x --glob "xr_${D}_*.csv.gz" --csv $R/xscan_$D.csv > $R/xscan_$D.txt 2>&1
done
tar czf rep/days.tgz -C rep days && ls -la rep/days.tgz && echo ГОТОВО
