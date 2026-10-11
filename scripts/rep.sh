#!/bin/bash
# rep.sh NAME 'data/paper_fair6?.out' — упаковать итог paper-прогона в rep/NAME.tgz
cd "$(dirname "$0")/.."; N=${1:-p6}; G=${2:-data/paper_fair6?.out}
mkdir -p rep/$N; cp $G rep/$N/ 2>/dev/null
python3 tools/paper_sum.py $G | tee rep/$N/sum.txt
grep -ah "HALT-FLAT\|exceeded" $G | grep -v STATUS > rep/$N/events.txt
tar czf rep/$N.tgz -C rep $N && ls -la rep/$N.tgz
