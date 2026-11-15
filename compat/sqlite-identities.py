"""Read only synthetic identity rows from the contract test's private SQLite DB."""
import json
from pathlib import Path
import sqlite3
import sys

connection = sqlite3.connect(Path(sys.argv[1]).resolve().as_uri() + '?mode=ro', uri=True)
connection.row_factory = sqlite3.Row
result = {}
for table in ('identity_profiles', 'identity_links'):
    rows = [dict(row) for row in connection.execute(
        'SELECT * FROM ' + table + ' WHERE project_id=?', (sys.argv[2],))]
    for row in rows:
        if 'traits' in row:
            row['traits'] = json.loads(row['traits'])
    result[table] = rows
# Control type comes from the recorded SDK wire. Its immutable ID can be in
# either the pending queue or permanent receipt ledger after native delivery.
result['control_events'] = []
for control in json.loads(sys.argv[3] if len(sys.argv) > 3 else '[]'):
    row = connection.execute("SELECT id FROM events WHERE project_id=? AND uuid=? UNION ALL SELECT event_id AS id FROM event_receipts WHERE project_id=? AND uuid=? LIMIT 1", (sys.argv[2], control['uuid'], sys.argv[2], control['uuid'])).fetchone()
    if row is not None:
        result['control_events'].append({'id': row['id'], **control})
result['control_events'].sort(key=lambda row: row['id'])
connection.close()
print(json.dumps(result))
