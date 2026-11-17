package output

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/delivery"
)

func TestSharedPipelineJobsEncryptAndRetireDestination(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, effects := pipelineFixture(t)
	_, err := db.Exec(`CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);` + PipelineJobsPostgresSchemaV1)
	if err != nil {
		t.Fatal(err)
	}
	d := HostedPipelineDependencies{
		PipelineDependencies: PipelineDependencies{Control: db}, Effects: effects,
		ProjectTransaction: func(ctx context.Context, _ string, run func(context.Context, *sql.Tx) error) error {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			if err = run(ctx, tx); err != nil {
				return err
			}
			return tx.Commit()
		},
	}
	config := &PipelineConfig{Key: bytes.Repeat([]byte{1}, 32)}
	first, err := NewHostedPipelines(d, config)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewHostedPipelines(d, config)
	if err != nil {
		t.Fatal(err)
	}
	for _, worker := range []*PipelineJobs{first, second} {
		if err = worker.Initialize(ctx); err != nil {
			t.Fatal(err)
		}
	}
	request := PipelineRequest{Kind: "webhook", Name: "fixture", Webhook: &WebhookPipelineSpec{Endpoint: "https://example.test/a/private"}, Credentials: PipelineCredentials{BearerToken: "test-bearer"}}
	created, err := first.Define(ctx, "project", request, auth.Principal{ID: "creator"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = first.Define(ctx, "other", request, auth.Principal{ID: "creator"}); err == nil {
		t.Fatal("foreign tenant admitted creator")
	}
	if _, err = second.Get(ctx, "other", created.ID); err == nil {
		t.Fatal("foreign project retrieved definition")
	}
	got, err := second.Get(ctx, "project", created.ID)
	if err != nil || got.ID != created.ID {
		t.Fatalf("cold read: %+v %v", got, err)
	}
	stored, err := second.hostedPipeline(ctx, "project", created.ID)
	if err != nil || strings.Contains(string(stored.Config), "private") || bytes.Contains(stored.Secret, []byte("test-bearer")) || bytes.Contains(stored.Secret, []byte("private")) {
		t.Fatalf("definition leaked destination metadata, err=%v", err)
	}

	freeze := func() PipelineTarget {
		t.Helper()
		var target PipelineTarget
		pipelineTx(t, db, func(tx *sql.Tx) error {
			plan, err := effects.FreezeLivePlanTx(ctx, tx, "tenant", PipelineScope{"source", "project", "catalog", "stream"})
			for _, entry := range plan.Targets {
				if entry.ID == created.ID {
					target = entry
				}
			}
			return err
		})
		if target.ID == "" {
			t.Fatal("missing hosted rule")
		}
		return target
	}
	original := freeze()
	raw, err := second.HostedPipelineRule(ctx, "tenant", "project", original)
	if err != nil {
		t.Fatal(err)
	}
	rule := json.NewDecoder(bytes.NewReader(raw))
	rule.DisallowUnknownFields()
	if err = rule.Decode(new(delivery.WebhookRule)); err != nil {
		t.Fatal(err)
	}
	altered := original
	altered.Rule.Bytes++
	if _, err = second.HostedPipelineRule(ctx, "tenant", "project", altered); !errors.Is(err, ErrPipelineConflict) {
		t.Fatalf("changed rule unexpectedly verified: %v", err)
	}

	pipelineTx(t, db, func(tx *sql.Tx) error {
		destination, err := second.HostedWebhookDestinationTx(ctx, tx, "tenant", "project", created.ID, original.Incarnation, original.DestinationRevision)
		if err != nil || destination.Endpoint != request.Webhook.Endpoint || destination.Headers["Authorization"] != "Bearer test-bearer" {
			t.Fatalf("cold decrypt failed: %v", err)
		}
		return nil
	})
	if _, err = db.Exec(`INSERT INTO pipeline_effect_jobs(source_id,project_id,catalog_id,stream_id,tenant_id,id,batch_id,pipeline_id,incarnation,rule_revision,destination_revision,creator_id,generation,kind,sequence,body_key,body_version,body_sha,body_bytes,state,refs,bytes,rows,artifacts)
VALUES('source','project','catalog','stream','tenant','run','batch',$1,$2,$3,$4,'creator','generation','delivery',1,'body','','body-sha',10,'pending',1,10,1,10)`, created.ID, original.Incarnation, original.RuleRevision, original.DestinationRevision); err != nil {
		t.Fatal(err)
	}
	if _, err = second.SetState(ctx, "project", created.ID, "paused"); err != nil {
		t.Fatal(err)
	}
	if n := pipelineCount(t, db, `SELECT count(*) FROM pipeline_effect_jobs WHERE state='pending'`); n != 1 {
		t.Fatal("pause retired pending work")
	}
	pipelineTx(t, db, func(tx *sql.Tx) error {
		plan, err := effects.FreezeLivePlanTx(ctx, tx, "tenant", PipelineScope{"source", "project", "catalog", "stream"})
		for _, entry := range plan.Targets {
			if entry.ID == created.ID {
				t.Fatal("paused definition admitted new work")
			}
		}
		return err
	})
	if _, err = second.SetState(ctx, "project", created.ID, "active"); err != nil {
		t.Fatal(err)
	}
	if _, err = second.Replace(ctx, "project", created.ID, PipelineChange{Credentials: PipelineCredentials{BearerToken: "replacement-token"}}); err != nil {
		t.Fatal(err)
	}
	revised := freeze()
	if revised.DestinationRevision == original.DestinationRevision || revised.Rule != original.Rule || revised.Incarnation != original.Incarnation {
		t.Fatal("credential rotation changed wrong revision")
	}
	pipelineTx(t, db, func(tx *sql.Tx) error {
		_, err := first.HostedWebhookDestinationTx(ctx, tx, "tenant", "project", created.ID, original.Incarnation, original.DestinationRevision)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("retired target decrypted: %v", err)
		}
		destination, err := first.HostedWebhookDestinationTx(ctx, tx, "tenant", "project", created.ID, revised.Incarnation, revised.DestinationRevision)
		if err != nil || destination.Headers["Authorization"] != "Bearer replacement-token" {
			t.Fatalf("revised target unavailable: %v", err)
		}
		return nil
	})
	if n := pipelineCount(t, db, `SELECT count(*) FROM pipeline_effect_jobs WHERE state='retired' AND version=1`); n != 1 {
		t.Fatal("rotation did not fence pending run")
	}
	if _, err = db.Exec(`UPDATE hosted_pipeline_configs SET secret=$1 WHERE project_id='project' AND id=$2`, stored.Secret, created.ID); err != nil {
		t.Fatal(err)
	}
	pipelineTx(t, db, func(tx *sql.Tx) error {
		_, err := second.HostedWebhookDestinationTx(ctx, tx, "tenant", "project", created.ID, revised.Incarnation, revised.DestinationRevision)
		if !errors.Is(err, ErrPipelineConflict) {
			t.Fatalf("old ciphertext decrypted at revised target: %v", err)
		}
		return nil
	})
	bad, err := NewHostedPipelines(d, &PipelineConfig{Key: bytes.Repeat([]byte{2}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	if err = bad.Initialize(ctx); err == nil {
		t.Fatal("different worker key accepted")
	}
	if err = second.Delete(ctx, "project", created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = first.Get(ctx, "project", created.ID); err == nil {
		t.Fatal("deleted definition remains visible")
	}
}
