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
result['control_events'] = [dict(row) for row in connection.execute(
    "SELECT id,uuid,event FROM events WHERE project_id=? AND event IN ('$identify','$set') ORDER BY id", (sys.argv[2],))]
connection.close()
print(json.dumps(result))
