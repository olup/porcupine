package lake

import (
	"context"
	"database/sql"
)

type projectWriteSocketSettings struct {
	idle, interval, count int
	userTimeout           sql.NullInt32
}

// Commit the socket settings before trying to take the session lock. Rolling
// back a canceled acquisition statement does not roll back an advisory lock.
// An interval, count or idle value <= 0 means that PostgreSQL could not read it
// for this socket. A zero user timeout also represents the default on Linux.
const configureProjectWriteSocketSQL = `
WITH previous AS (
 SELECT current_setting('tcp_keepalives_idle') AS idle,
        current_setting('tcp_keepalives_interval') AS probe_interval,
        current_setting('tcp_keepalives_count') AS probe_count,
        current_setting('tcp_user_timeout', true) AS user_timeout
 OFFSET 0
)
SELECT idle, probe_interval, probe_count, user_timeout,
       CASE WHEN idle::int > 0 THEN set_config('tcp_keepalives_idle', LEAST(idle::int,15)::text, false) END,
       CASE WHEN probe_interval::int > 0 THEN set_config('tcp_keepalives_interval', LEAST(probe_interval::int,5)::text, false) END,
       CASE WHEN probe_count::int > 0 THEN set_config('tcp_keepalives_count', LEAST(probe_count::int,3)::text, false) END,
       CASE WHEN inet_client_addr() IS NOT NULL AND user_timeout IS NOT NULL AND
                      (user_timeout::int > 0 OR (user_timeout='0' AND split_part(version(),',',1) LIKE '%linux%'))
            THEN set_config('tcp_user_timeout', LEAST(NULLIF(user_timeout::int,0),30000)::text, false) END
FROM previous`

// Use the previous effective values rather than RESET, which would drop any
// settings that were already in effect on this pooled session.
const restoreProjectWriteSocketSQL = `(
 CASE WHEN $2::int > 0 THEN set_config('tcp_keepalives_idle',$2::text,false) = $2::text ELSE true END
 AND CASE WHEN $3::int > 0 THEN set_config('tcp_keepalives_interval',$3::text,false) = $3::text ELSE true END
 AND CASE WHEN $4::int > 0 THEN set_config('tcp_keepalives_count',$4::text,false) = $4::text ELSE true END
 AND CASE WHEN $5::int >= 0 THEN set_config('tcp_user_timeout',$5::text,false) = $5::text ELSE true END
)`

const tryProjectWriteSQL = `
WITH lock_attempt AS (SELECT pg_try_advisory_lock($1) AS acquired, pg_backend_pid() AS pid)
SELECT acquired, pid, CASE WHEN acquired THEN true ELSE ` + restoreProjectWriteSocketSQL + ` END
FROM lock_attempt`

const releaseProjectWriteSQL = `
WITH lock_release AS (SELECT pg_advisory_unlock($1) AS unlocked)
SELECT CASE WHEN unlocked THEN ` + restoreProjectWriteSocketSQL + ` ELSE false END
FROM lock_release`

func (s *ProjectWriteSession) configureSocket(ctx context.Context) error {
	var appliedIdle, appliedInterval, appliedCount, appliedTimeout any
	return s.conn.QueryRowContext(ctx, configureProjectWriteSocketSQL).Scan(
		&s.socket.idle, &s.socket.interval, &s.socket.count, &s.socket.userTimeout,
		&appliedIdle, &appliedInterval, &appliedCount, &appliedTimeout,
	)
}
