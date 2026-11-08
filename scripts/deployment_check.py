#!/usr/bin/env python3
"""Exercise a deployed test instance using synthetic events; no production SDK changes."""
import argparse
import json
from pathlib import Path
from credentials import load_credentials
import time
import urllib.error
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True)
    parser.add_argument('--credentials', type=Path, required=True)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    keys = load_credentials(args.credentials)
    keys.validate_target(args.url)
    root = args.url.rstrip('/')
    pid = keys['project_id']
    path = f'/api/v1/projects/{pid}'
    run = 'deployment-' + uuid.uuid4().hex
    results = []

    def request(method, endpoint, body=None, key=None, headers=None):
        hdr = {'Content-Type': 'application/json', **(headers or {})}
        if key:
            hdr['Authorization'] = 'Bearer ' + key
        req = urllib.request.Request(root + endpoint, method=method, headers=hdr,
                                     data=json.dumps(body).encode() if body is not None else None)
        try:
            with keys.open_api(req, timeout=20) as response:
                raw = response.read()
                return response.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as err:
            return err.code, None

    def check(name, condition):
        if not condition:
            raise RuntimeError('Check failed: ' + name)
        results.append(name)

    def read_token():
        return keys.get('read_token') or keys['admin_token']
    check('health', request('GET', '/healthz')[0] == 200)
    check('anonymous management denied', request('GET', '/api/v1/projects')[0] == 401)
    check('OpenAPI accessible', request('GET', '/openapi.json')[0] == 200)
    check('reader cannot create project', request('POST', '/api/v1/projects', {'name': run}, read_token())[0] == 403)
    ev = {'api_key': keys['ingest_token'], 'uuid': run, 'event': 'deployment_check',
          'distinct_id': 'synthetic-user', 'properties': {'synthetic': True, 'run_id': run, '$browser': 'Synthetic'}}
    check('ingestion', request('POST', '/capture/', ev)[0] == 200)
    check('duplicate accepted idempotently', request('POST', '/capture/', ev)[0] == 200)
    check('invalid ingestion token denied', request('POST', '/capture/', {**ev, 'api_key': 'invalid'})[0] in (401, 403))
    query = {'sql': 'SELECT count() AS total FROM events WHERE uuid={uuid:String}', 'parameters': {'uuid': run}}
    for _ in range(60):
        status, response = request('POST', path + '/query', query, read_token())
        if status == 200 and response['result']['data'][0]['total'] == '1' and response['delivery']['pending'] == 0:
            break
        time.sleep(1)
    else:
        raise RuntimeError('Delivery or analytical query unavailable')
    check('durable delivery and logical deduplication', True)
    check('schema discovery', request('GET', path+'/schema', key=read_token())[0] == 200)
    status, catalog = request('GET', path+'/catalog?event=deployment_check', key=read_token())
    check('event and property discovery', status == 200 and any(p['key'] == 'run_id' for p in catalog['properties']))
    check('unsafe SQL rejected', request('POST', path+'/query', {'sql': 'SELECT * FROM system.users'}, read_token())[0] == 400)
    check('SQL writes rejected', request('POST', path+'/query', {'sql': 'DELETE FROM events'}, keys['admin_token'])[0] == 400)
    status, other = request('POST', '/api/v1/projects', {'name': run+'-isolation'}, keys['admin_token'])
    check('second synthetic project', status == 201)
    check('project boundary enforced', request('POST', f"/api/v1/projects/{other['project']['id']}/query", query, read_token())[0] in (403,404))
    status, empty = request('POST', f"/api/v1/projects/{other['project']['id']}/query", query, keys['admin_token'])
    check('other project sees zero matching events', status == 200 and empty['result']['data'][0]['total'] == '0')

    def mcp(method, params):
        return request('POST', '/mcp', {'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}, keys.get('mcp_read_token') or read_token(),
                       {'Accept': 'application/json, text/event-stream', 'MCP-Protocol-Version':'2025-11-25'})
    status, initialized = mcp('initialize', {'protocolVersion':'2025-11-25','capabilities':{},'clientInfo':{'name':'deployment-check','version':'1'}})
    check('MCP initialization over deployed transport', status == 200 and 'result' in initialized)
    status, tools = mcp('tools/list', {})
    check('MCP two-tool discovery', status == 200 and {t['name'] for t in tools['result']['tools']} == {'porcupine_docs','porcupine_call'})
    status, docs = mcp('tools/call', {'name':'porcupine_docs','arguments':{'operation_id':'query-events'}})
    check('MCP query contract', status == 200 and not docs['result'].get('isError'))
    status, queried = mcp('tools/call', {'name':'porcupine_call','arguments':{'operation_id':'query-events','path':{'id':pid},'body':query}})
    check('MCP project SQL execution', status == 200 and not queried['result'].get('isError') and queried['result']['structuredContent']['body']['result']['data'][0]['total'] == '1')
    status, denied = mcp('tools/call', {'name':'porcupine_call','arguments':{'operation_id':'query-events','path':{'id':other['project']['id']},'body':query}})
    check('MCP project boundary', status == 200 and denied['result'].get('isError') is True)
    status, current = request('GET', path+'/events/'+run, key=read_token())
    check('read delivered event', status == 200 and current['revision'] == '0')
    replacement = {'idempotency_key':run+'-edit','expected_revision':current['revision'],
                   'snapshot':{'event':'deployment_check_corrected','distinct_id':'synthetic-user',
                               'properties':{'synthetic':True,'run_id':run,'corrected':True},
                               'timestamp':current['event']['timestamp']}}
    check('reader cannot edit', request('PATCH',path+'/events/'+run,replacement,read_token())[0] == 403)
    status, mutation = request('PATCH',path+'/events/'+run,replacement,keys['admin_token'])
    check('durable edit accepted',status == 202)
    status, repeated = request('PATCH',path+'/events/'+run,replacement,keys['admin_token'])
    check('edit retry is idempotent',status == 202 and mutation['mutation_id'] == repeated['mutation_id'])
    for _ in range(60):
        status, audit = request('GET',path+'/event-mutations/'+mutation['mutation_id'],key=read_token())
        if status == 200 and audit['state'] == 'applied':
            break
        time.sleep(1)
    else:
        raise RuntimeError('Mutation did not project')
    status, current = request('GET',path+'/events/'+run,key=read_token())
    check('corrected event projected',status == 200 and current['event']['properties']['corrected'] is True)
    deletion = {'idempotency_key':run+'-delete','expected_revision':current['revision']}
    check('reader cannot delete',request('DELETE',path+'/events/'+run,deletion,read_token())[0] == 403)
    status, deleted = request('DELETE',path+'/events/'+run,deletion,keys['admin_token'])
    check('durable delete accepted',status == 202)
    check('deleted event gone from API',request('GET',path+'/events/'+run,key=read_token())[0] == 410)
    for _ in range(60):
        status, response = request('POST',path+'/query',query,read_token())
        if status == 200 and response['result']['data'][0]['total'] == '0' and response['delivery']['pending_mutations'] == 0:
            break
        time.sleep(1)
    else:
        raise RuntimeError('Deletion did not disappear from SQL')
    check('deleted event excluded from analytical SQL',True)
    report = {'url':root,'run_id':run,'passed':results,'production_connected':False}
    text = json.dumps(report,indent=2)+'\n'
    if args.output:
        args.output.write_text(text)
    print(text)


if __name__ == '__main__':
    main()
