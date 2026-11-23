import json,statistics,glob,sys
S='/tmp/claude-1000/-home-matt-Desktop-source-tsdb/9342c4ea-1293-40f9-832b-6ed676dbaaf6/scratchpad/bench/res'
B=[json.load(open(f)) for f in sorted(glob.glob(f'{S}/bucket_base_*.json'))]; N=[json.load(open(f)) for f in sorted(glob.glob(f'{S}/bucket_new_*.json'))]
unexplained=[q for q in B[0] if not ({r[q]['digest'] for r in N} & {r[q]['digest'] for r in B})]
varying=[q for q in B[0] if len({r[q]['digest'] for r in B+N})!=1]
print('queries',len(B[0]),'| results varying run to run (float fold order across shards, in either build):',len(varying),'| new build never matching any baseline run:',len(unexplained),unexplained[:6])
rows=[]
for q in B[0]:
    b=statistics.median(r[q]['ms'] for r in B); n=statistics.median(r[q]['ms'] for r in N); rows.append((b/n,q,B[0][q]['points'],b,n))
top=int(sys.argv[1]) if len(sys.argv)>1 else 24
print('%-40s %9s %10s %10s %7s' % ('query','buckets','before ms','after ms','x'))
for x,q,p,b,n in sorted(rows,key=lambda r:-r[3])[:top]: print('%-40s %9d %10.2f %10.2f %6.2fx' % (q,p,b,n,x))
xs=sorted(r[0] for r in rows); print('speedup over all %d queries: min %.2fx median %.2fx max %.2fx' % (len(xs),xs[0],xs[len(xs)//2],xs[-1]))
print('slower by >5%:',[(q,round(x,2),round(b,2),round(n,2)) for x,q,p,b,n in rows if x<0.95])
