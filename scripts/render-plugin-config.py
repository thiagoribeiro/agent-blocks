#!/usr/bin/env python3
"""Materialize the private static registry without printing credentials."""
import argparse
import json
import os
from pathlib import Path
parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,default=Path('deploy/plugins.json'));args=parser.parse_args()
plugins=[]
# Keep in sync with internal/catalog/plugins.go. Optional plugins are skipped when their token is unset.
REQUIRED=['approval','workflow','form_stateful','checklist','poll','agent_status']
OPTIONAL=['monthly_bills']
for name in REQUIRED+OPTIONAL:
    suffix=name.upper();token=os.environ.get('BRIAN_'+suffix+'_TOKEN')
    if not token and name in OPTIONAL:continue
    if not token:parser.error('missing BRIAN_'+suffix+'_TOKEN')
    target=os.environ.get('BRIAN_'+suffix+'_URL','http://brian-'+name.replace('_','-')+':4010')
    plugins.append({'name':name,'url':target,'token':token})
args.output.parent.mkdir(parents=True,exist_ok=True)
fd=os.open(args.output,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
os.fchmod(fd,0o600)
with os.fdopen(fd,'w') as f:json.dump(plugins,f,indent=2);f.write('\n')
print('Registry written:',args.output)
