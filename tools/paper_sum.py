#!/usr/bin/env python3
# paper_sum.py — итог по парам и t-стат по пар-часам из .out spred.  python3 paper_sum.py data/paper_fair3a.out data/paper_fair3b.out
import re,sys,statistics as st
pat=re.compile(r'^\[(\d\d):\d\d:\d\d\] mid (\S+) .*?\| pos (\S+) \| mm (\S+) inv (\S+) \| eq \S+ \| P&L (\S+) real (\S+) \| fills (\d+) \(b(\d+)/s(\d+) .*?\| vol (\d+) .*\| (\w+USD[TC])$')
R={};H={};halt={}
for f in sys.argv[1:]:
    for ln in open(f,errors='replace'):
        if 'exceeded limit' in ln:
            m=re.search(r'\[(\w+USD[TC])\] \[(\d\d)',ln); halt[m.group(1)]=m.group(2) if m else '?'
        m=pat.match(ln)
        if not m: continue
        hh,mid,pos,mm,inv,pnl,real,fl,b,s,vol,sym=m.groups()
        r=R.setdefault(sym,dict(mid0=float(mid)));r.update(mid=float(mid),pos=float(pos),mm=float(mm),inv=float(inv),pnl=float(pnl),fills=int(fl),b=b,s=s,vol=int(vol))
        h=H.setdefault(sym,[]); 
        if not h or h[-1][0]!=hh: h.append([hh,float(mm)])
        else: h[-1][1]=float(mm)
print(f"{'sym':11}{'P&L':>8}{'mm':>7}{'inv':>7}{'fills':>6}{'b/s':>8}{'pos':>6}{'dmid%':>7} halt")
for s,d in sorted(R.items(),key=lambda x:x[1]['mm']):
    print(f"{s:11}{d['pnl']:8.2f}{d['mm']:7.2f}{d['inv']:7.2f}{d['fills']:6}{d['b']+'/'+d['s']:>8}{d['pos']:6.1f}{(d['mid']/d['mid0']-1)*100:7.1f} {halt.get(s,'')}")
x=[]
for s,h in H.items():
    p=0
    for _,v in h: x.append(v-p); p=v
print(f"TOTAL P&L {sum(d['pnl'] for d in R.values()):.2f}  mm {sum(d['mm'] for d in R.values()):.2f}  inv {sum(d['inv'] for d in R.values()):.2f}")
if len(x)>2:
    m=st.mean(x); sd=st.stdev(x); print(f"пар-часов {len(x)}  mm/ч/пару {m:+.3f}  sd {sd:.3f}  t {m/(sd/len(x)**.5):+.2f}  (micro-live: >=+0.05 и t>=2)")
