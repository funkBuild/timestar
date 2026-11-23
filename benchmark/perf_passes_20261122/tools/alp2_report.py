import re,statistics,sys
tag=sys.argv[1] if len(sys.argv)>1 else 'alp2'
def load(f):
    d={}
    for l in open(f):
        m=re.match(r'shape=(\S+)\s+n=(\d+)\s+ns/value=\s*([\d.]+)\s+bytes/value=\s*([\d.]+)\s+fnv=(\S+) decode_ns=\s*([\d.]+) (\S+)',l)
        if m: d[(m[1],int(m[2]))]=(float(m[3]),float(m[4]),m[5],float(m[6]),m[7])
    return d
B=[load(f'res/{tag}_base_{i}.txt') for i in (1,2,3)]; N=[load(f'res/{tag}_new_{i}.txt') for i in (1,2,3)]
print(f"{'shape':9}{'n':>6} | encode ns/value before->after  | decode ns/value before->after | bytes/value before->after")
for k in B[0]:
    eb=statistics.median(x[k][0] for x in B); en=statistics.median(x[k][0] for x in N); db=statistics.median(x[k][3] for x in B); dn=statistics.median(x[k][3] for x in N)
    print(f"{k[0]:9}{k[1]:>6} | {eb:7.2f} -> {en:7.2f} ({eb/en:4.2f}x) | {db:6.2f} -> {dn:6.2f} ({db/dn:4.2f}x) | {B[0][k][1]:6.3f} -> {N[0][k][1]:6.3f} ({100*(N[0][k][1]/B[0][k][1]-1):+5.1f}%) {N[0][k][4]}")
