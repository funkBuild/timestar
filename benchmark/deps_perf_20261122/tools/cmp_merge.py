#!/usr/bin/env python3
"""cmp_merge.py <rounds> <cpu> <binary>... : alternate the binaries, print the median ms per case per binary."""
import subprocess, sys, re, statistics, collections
rounds=int(sys.argv[1]); cpu=sys.argv[2]; bins=sys.argv[3:]
res=collections.defaultdict(lambda: collections.defaultdict(list)); order=[]
for r in range(rounds):
    for b in (bins if r%2==0 else bins[::-1]):
        out=subprocess.run(['taskset','-c',cpu,'./'+b,'9'],capture_output=True,text=True).stdout
        for l in out.splitlines():
            m=re.match(r'(\S+)\s+(\S+)\s+out=(\d+) ms=\s*([\d.]+) check=(\S+)',l)
            if m:
                k=(m[1],m[2]); res[k][b].append(float(m[4])); res[k]['check_'+b]=m[5]
                if k not in order: order.append(k)
print('%-12s %-15s' % ('layout','method') + ''.join('%12s' % b[-10:] for b in bins) + '   same-result')
for k in order:
    chk={res[k]['check_'+b] for b in bins}
    print('%-12s %-15s' % k + ''.join('%12.3f' % statistics.median(res[k][b]) for b in bins) + ('   yes' if len(chk)==1 else '   DIFF'))
