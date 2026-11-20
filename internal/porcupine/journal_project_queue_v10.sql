CREATE TABLE project_queue_usage (
 project_id TEXT PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
 pending_count INTEGER NOT NULL CHECK(pending_count>=0),
 pending_bytes INTEGER NOT NULL CHECK(pending_bytes>=0)
) WITHOUT ROWID;

-- Use the same charged byte expression as the instance queue_usage triggers.
-- Delivered legacy rows and receipt-only history do not occupy the pending queue.
INSERT INTO project_queue_usage(project_id,pending_count,pending_bytes)
SELECT project_id,count(*),sum(
 64+length(CAST(project_id AS BLOB))+length(CAST(uuid AS BLOB))+length(CAST(event AS BLOB))+
 length(CAST(distinct_id AS BLOB))+length(CAST(properties AS BLOB))+length(CAST(timestamp AS BLOB))+
 length(CAST(received_at AS BLOB))+length(CAST(identity_snapshot AS BLOB))+length(CAST(ingestion_metadata AS BLOB))
) FROM events WHERE delivered=0 GROUP BY project_id;

CREATE TRIGGER project_queue_usage_insert AFTER INSERT ON events WHEN NEW.delivered=0 BEGIN
 INSERT INTO project_queue_usage(project_id,pending_count,pending_bytes)
 VALUES(NEW.project_id,1,
  64+length(CAST(NEW.project_id AS BLOB))+length(CAST(NEW.uuid AS BLOB))+length(CAST(NEW.event AS BLOB))+
  length(CAST(NEW.distinct_id AS BLOB))+length(CAST(NEW.properties AS BLOB))+length(CAST(NEW.timestamp AS BLOB))+
  length(CAST(NEW.received_at AS BLOB))+length(CAST(NEW.identity_snapshot AS BLOB))+length(CAST(NEW.ingestion_metadata AS BLOB))
 )
 ON CONFLICT(project_id) DO UPDATE SET pending_count=pending_count+1,pending_bytes=pending_bytes+excluded.pending_bytes;
END;

CREATE TRIGGER project_queue_usage_delete AFTER DELETE ON events WHEN OLD.delivered=0 BEGIN
 UPDATE project_queue_usage SET pending_count=pending_count-1,pending_bytes=pending_bytes-(
  64+length(CAST(OLD.project_id AS BLOB))+length(CAST(OLD.uuid AS BLOB))+length(CAST(OLD.event AS BLOB))+
  length(CAST(OLD.distinct_id AS BLOB))+length(CAST(OLD.properties AS BLOB))+length(CAST(OLD.timestamp AS BLOB))+
  length(CAST(OLD.received_at AS BLOB))+length(CAST(OLD.identity_snapshot AS BLOB))+length(CAST(OLD.ingestion_metadata AS BLOB))
 ) WHERE project_id=OLD.project_id;
END;

CREATE TRIGGER project_queue_usage_update AFTER UPDATE ON events WHEN OLD.delivered=0 OR NEW.delivered=0 BEGIN
 UPDATE project_queue_usage
 SET pending_count=pending_count-1+(NEW.delivered=0 AND OLD.project_id=NEW.project_id),
 pending_bytes=pending_bytes-(
  64+length(CAST(OLD.project_id AS BLOB))+length(CAST(OLD.uuid AS BLOB))+length(CAST(OLD.event AS BLOB))+
  length(CAST(OLD.distinct_id AS BLOB))+length(CAST(OLD.properties AS BLOB))+length(CAST(OLD.timestamp AS BLOB))+
  length(CAST(OLD.received_at AS BLOB))+length(CAST(OLD.identity_snapshot AS BLOB))+length(CAST(OLD.ingestion_metadata AS BLOB))
 )+CASE WHEN NEW.delivered=0 AND OLD.project_id=NEW.project_id THEN (
  64+length(CAST(NEW.project_id AS BLOB))+length(CAST(NEW.uuid AS BLOB))+length(CAST(NEW.event AS BLOB))+
  length(CAST(NEW.distinct_id AS BLOB))+length(CAST(NEW.properties AS BLOB))+length(CAST(NEW.timestamp AS BLOB))+
  length(CAST(NEW.received_at AS BLOB))+length(CAST(NEW.identity_snapshot AS BLOB))+length(CAST(NEW.ingestion_metadata AS BLOB))
 ) ELSE 0 END
 WHERE OLD.delivered=0 AND project_id=OLD.project_id;

 INSERT INTO project_queue_usage(project_id,pending_count,pending_bytes)
 SELECT NEW.project_id,1,
  64+length(CAST(NEW.project_id AS BLOB))+length(CAST(NEW.uuid AS BLOB))+length(CAST(NEW.event AS BLOB))+
  length(CAST(NEW.distinct_id AS BLOB))+length(CAST(NEW.properties AS BLOB))+length(CAST(NEW.timestamp AS BLOB))+
  length(CAST(NEW.received_at AS BLOB))+length(CAST(NEW.identity_snapshot AS BLOB))+length(CAST(NEW.ingestion_metadata AS BLOB))
 WHERE NEW.delivered=0 AND (OLD.delivered<>0 OR OLD.project_id<>NEW.project_id)
 ON CONFLICT(project_id) DO UPDATE SET pending_count=pending_count+1,pending_bytes=pending_bytes+excluded.pending_bytes;
END;
