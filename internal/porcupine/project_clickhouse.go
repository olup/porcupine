package porcupine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
)

func projectNamespace(database, projectID string) string {
	digest := sha256.Sum256([]byte(database + "\x00" + projectID))
	return "pc_" + hex.EncodeToString(digest[:16])
}

// deleteProjectClickHouse runs after the deletion fence has stopped admission
// and drained in-flight work. A repeated call resumes a partial cleanup.
func (s *Service) deleteProjectClickHouse(ctx context.Context, projectID string) error {
	if s.ch == nil {
		var previouslyConfigured bool
		if err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM metadata WHERE key='query_master_key')").Scan(&previouslyConfigured); err != nil {
			return err
		}
		if previouslyConfigured {
			return errors.New("project cleanup requires its configured ClickHouse")
		}
		return nil
	}
	// Attest the current projection table before deleting its data. This also
	// finishes the migration of any legacy event rows into that table.
	if err := s.initializeDelivery(ctx); err != nil {
		return err
	}
	if err := s.dropProjectReaders(ctx, projectID); err != nil {
		return err
	}
	table := s.ch.config.Database + "." + eventTable
	project := hex.EncodeToString([]byte(projectID))
	predicate := "project_id=unhex('" + project + "')"
	// Read the raw table, including old revisions, identity records and
	// deletion tombstones that would be hidden by the public FINAL views.
	exists, err := s.ch.admin(ctx, "SELECT 1 FROM "+table+" WHERE "+predicate+" LIMIT 1 SETTINGS final=0 FORMAT TabSeparated")
	if err != nil || len(exists) == 0 {
		return err
	}
	// The HTTP client can time out while a mutation continues inside ClickHouse.
	// Don't queue another rewrite of cold parts while that mutation is active.
	active, err := s.ch.admin(ctx, "SELECT 1 FROM system.mutations WHERE database='"+s.ch.config.Database+"' AND table='"+eventTable+"' AND NOT is_done AND position(command,'''"+project+"''')>0 LIMIT 1 FORMAT TabSeparated")
	if err != nil {
		return err
	}
	if len(active) > 0 {
		return errors.New("ClickHouse project deletion in progress")
	}
	_, err = s.ch.admin(ctx, "ALTER TABLE "+table+" DELETE WHERE "+predicate+" SETTINGS mutations_sync=1")
	return err
}

func (s *Service) dropProjectReaders(ctx context.Context, projectID string) error {
	if err := s.queryMu.Lock(ctx); err != nil {
		return err
	}
	defer s.queryMu.Unlock()
	delete(s.queryReady, projectID)
	namespace := projectNamespace(s.ch.config.Database, projectID)
	table := s.ch.config.Database + "." + eventTable
	readers := []string{namespace + "_reader", namespace + "_cached_reader", namespace + "_export", namespace + "_pipeline_export"}
	for _, reader := range readers {
		if _, err := s.ch.admin(ctx, "DROP ROW POLICY IF EXISTS "+reader+"_scope ON "+table); err != nil {
			return err
		}
		if _, err := s.ch.admin(ctx, "DROP USER IF EXISTS "+reader); err != nil {
			return err
		}
	}
	if _, err := s.ch.admin(ctx, "KILL QUERY WHERE user IN ('"+strings.Join(readers, "','")+"') SYNC"); err != nil {
		return err
	}
	if _, err := s.ch.admin(ctx, "DROP ROW POLICY IF EXISTS "+namespace+"_admin ON "+table); err != nil {
		return err
	}
	_, err := s.ch.admin(ctx, "DROP DATABASE IF EXISTS "+namespace+" SYNC")
	return err
}
