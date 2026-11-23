"""Report retained point capacities and numeric errors from existing CPU logs."""
from pathlib import Path
import json,sys
root=Path(sys.argv[1]).resolve()
rows={}
for log in root.glob('*.log'):
 mode=log.name.split('-')[0]
 if mode not in ('before','after'):continue
 for line in log.read_text().splitlines():
  if 'point_capacity_bytes=' not in line:continue
  tokens=line.split();fields=dict(token.split('=',1) for token in tokens[1:])
  key=tokens[0]+'_'+fields['n']
  row=rows.setdefault(key,{})
  metrics={name:int(fields[name]) for name in ('series_object_bytes','stats_object_bytes','point_capacity_bytes')}
  if 'storage' in row:assert row['storage']==metrics,(log,key)
  row['storage']=metrics
  errors=row.setdefault(mode,dict(sum_relative_error=0,m2_relative_error=0))
  for name in errors:errors[name]=max(errors[name],float(fields[name]))
assert len(rows)==24
(root/'memory-and-error-summary.json').write_text(json.dumps(rows,indent=2)+'\n')
