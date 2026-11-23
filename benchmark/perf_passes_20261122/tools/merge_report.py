import re,statistics
def load(f):
    d={}
    for l in open(f):
        m=re.match(r'(\S+)\s+(\S+)\s+out=(\d+) ms=\s*([\d.]+) check=(\S+)',l)
        if m: d[(m[1],m[2])]=(float(m[4]),m[5])
    return d
B=[load(f'res/merge_base_{i}.txt') for i in (1,2,3)]; N=[load(f'res/merge_new_{i}.txt') for i in (1,2,3)]
for k in B[0]:
    b=statistics.median(x[k][0] for x in B); a=statistics.median(x[k][0] for x in N); r=sorted(x[k][0]/y[k][0] for x,y in zip(B,N))
    print(f"{k[0]:12} {k[1]:15} before={b:7.3f} after={a:7.3f} {b/a:5.2f}x (pairs {r[0]:.2f}-{r[-1]:.2f}) {'same' if B[0][k][1]==N[0][k][1] else 'DIFF'}")
