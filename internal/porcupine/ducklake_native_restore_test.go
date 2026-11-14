//go:build ducklake

package porcupine

import (
	"context"
	"database/sql"
	"encoding/pem"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func nativeRestoreTarget(t *testing.T, id, bundle, source string) NativeRestoreRequest {
	t.Helper()
	root := t.TempDir()
	return NativeRestoreRequest{BackupID: id, SourceID: source, LocalBundlePath: bundle, TargetDBPath: filepath.Join(root, "target", "queue.db"), TargetDuckLakeDirectory: filepath.Join(root, "target", "lake"), ResetCredentialsAcknowledged: true, BootstrapToken: strings.Repeat("private-recovery-bootstrap", 2)}
}

func TestDuckLakeNativeRestoreIsolatedCurrentStateAndCredentialReset(t *testing.T) {
	s, config := nativeBackupService(t)
	a, ingest := project(t, s)
	b, otherIngest := project(t, s)
	state := lakeMaintenanceLifecycle(t, s, a, ingest, "restore", true)
	// Include 600 rows with 4 KiB properties, then verify exact count and width.
	for batch := 0; batch < 6; batch++ {
		var body strings.Builder
		body.WriteByte('[')
		for i := 0; i < 100; i++ {
			if i > 0 {
				body.WriteByte(',')
			}
			fmt.Fprintf(&body, `{"type":"track","message_id":"bulk-%d","actor_id":"bulk-person","name":"bulk-restore","properties":{"padding":%q}}`, batch*100+i, strings.Repeat("x", 4096))
		}
		body.WriteByte(']')
		nativeRequest(t, s, ingest, body.String(), 200)
	}
	duckLakeJourneyDrain(t, s)
	nativeRequest(t, s, otherIngest, `{"type":"track","message_id":"other-project","actor_id":"other","name":"untouched"}`, 200)
	duckLakeJourneyDrain(t, s)
	if err := s.lake.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.deliveryCancel()
	<-s.deliveryDone
	pending := `{"type":"track","message_id":"pending-checkpoint","actor_id":"person","name":"pending"}`
	pendingID := receiptFrom(t, nativeRequest(t, s, ingest, pending, 200), 0)["event_id"]
	if err := s.createNativeBackup(context.Background()); err != nil {
		t.Fatal(err)
	}
	id := s.nativeBackupStatus().LatestBundle
	bundle := filepath.Join(config.NativeBackup.Directory, id)
	source, err := s.lake.SourceID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(filepath.Dir(config.DBPath)); err != nil {
		t.Fatal(err)
	}
	r := nativeRestoreTarget(t, id, bundle, source)
	result, err := RestoreNativeBackup(context.Background(), NativeRestoreConfig{Request: r, DuckLake: DuckLakeConfig{MemoryLimitMB: 256, Threads: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if result.SourceID != source || result.BackupID != id || len(result.IngestionKeys) != 2 {
		t.Fatalf("restore result changed source/project identities: %+v", result)
	}
	var rotated string
	for _, key := range result.IngestionKeys {
		if key.ProjectID == a {
			rotated = key.Token
		}
	}
	// Move the whole archive away before restart and every native maintenance
	// operation. Neither new metadata nor cleanup may refer to source objects.
	if err = os.Rename(bundle, bundle+".offline"); err != nil {
		t.Fatal(err)
	}
	restoredConfig := Config{DBPath: r.TargetDBPath, DuckLake: &DuckLakeConfig{Directory: r.TargetDuckLakeDirectory, MemoryLimitMB: 256, Threads: 1, MaintenanceInterval: 24 * time.Hour}}
	restored, err := Open(restoredConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { restored.Close() }()
	request(t, restored, "GET", "/api/v1/projects", adminToken, "", 401)
	request(t, restored, "GET", "/api/v1/projects", r.BootstrapToken, "", 200)
	nativeRequest(t, restored, ingest, pending, 401)
	duckLakeJourneyDrain(t, restored)
	// Lifecycle verifier uses the source fixture bootstrap token, so query these
	// preserved fields directly with the new recovery credential.
	rows := duckLakeJourneyQuery(t, restored, a, r.BootstrapToken, "SELECT event_id,event,revision,person_id,person_properties FROM events WHERE uuid=$uuid", map[string]string{"uuid": state.receipts[0]["uuid"].(string)})
	if len(rows) != 1 {
		t.Fatal("restored edit missing", rows)
	}
	row := rows[0].(map[string]any)
	if row["event_id"] != state.receipts[0]["event_id"] || row["revision"] != state.revision || row["event"] != "lifecycle-corrected" || row["person_id"] != "restore-person" || !strings.Contains(row["person_properties"].(string), `"plan":"team"`) {
		t.Fatal("restored event/edit/identity changed", row)
	}
	bulk := duckLakeJourneyQuery(t, restored, a, r.BootstrapToken, "SELECT count(*) AS total,min(length(properties)) AS width FROM events WHERE event='bulk-restore'", nil)
	if bulk[0].(map[string]any)["total"] != "600" || bulk[0].(map[string]any)["width"] != "4110" {
		t.Fatal("bounded transfer lost or changed wide rows", bulk)
	}
	deleted, err := restored.lake.Load(context.Background(), a, state.receipts[1]["uuid"].(string))
	if err != nil || !deleted.IsDeleted {
		t.Fatal("restored tombstone lost", err)
	}
	retried := nativeRequest(t, restored, rotated, lakeMaintenancePages("restore"), 200)
	if retried["duplicates"] != float64(2) || receiptFrom(t, retried, 0)["event_id"] != state.receipts[0]["event_id"] {
		t.Fatal("rotated-key retry changed receipts")
	}
	retryPending := nativeRequest(t, restored, rotated, pending, 200)
	if retryPending["duplicates"] != float64(1) || receiptFrom(t, retryPending, 0)["event_id"] != pendingID {
		t.Fatal("pending queue retry changed receipts")
	}
	currentSource, err := restored.lake.SourceID(context.Background())
	if err != nil || currentSource != source {
		t.Fatal("source identity changed", err)
	}
	if err = restored.Close(); err != nil {
		t.Fatal(err)
	}
	restored, err = Open(restoredConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err = restored.lake.Maintain(context.Background()); err != nil {
		t.Fatal("target-only maintenance failed", err)
	}
	lake := restored.lake.(*duckLakeStore)
	for i := 0; i < 2; i++ {
		if err = lake.cleanupDue(context.Background(), time.Now().Add(2*time.Hour)); err != nil {
			t.Fatal("target-only cleanup failed", err)
		}
	}
	request(t, restored, "DELETE", "/api/v1/projects/"+a, r.BootstrapToken, "", 204)
	remaining := duckLakeJourneyQuery(t, restored, b, r.BootstrapToken, "SELECT count(*) AS total FROM events", nil)
	if remaining[0].(map[string]any)["total"] != "1" {
		t.Fatal("restore cleanup affected second project", remaining)
	}
	if _, err = os.Stat(bundle + ".offline/manifest.json"); err != nil {
		t.Fatal("target cleanup touched offline archive", err)
	}
}

func TestDuckLakeNativeRestoreRefusesIncompleteTamperedAndExistingTargets(t *testing.T) {
	s, config := nativeBackupService(t)
	a, ingest := project(t, s)
	nativeRequest(t, s, ingest, `{"type":"track","message_id":"fixture","actor_id":"person","name":"test"}`, 200)
	duckLakeJourneyDrain(t, s)
	if err := s.lake.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.createNativeBackup(context.Background()); err != nil {
		t.Fatal(err)
	}
	id := s.nativeBackupStatus().LatestBundle
	bundle := filepath.Join(config.NativeBackup.Directory, id)
	source, _ := s.lake.SourceID(context.Background())
	for _, kind := range []string{"missing", "checksum", "manifest", "existing", "overlap", "source", "version", "schema", "fresh-bootstrap"} {
		t.Run(kind, func(t *testing.T) {
			archiveRoot := t.TempDir()
			copyPath := filepath.Join(archiveRoot, id)
			if err := os.CopyFS(copyPath, os.DirFS(bundle)); err != nil {
				t.Fatal(err)
			}
			r := nativeRestoreTarget(t, id, copyPath, source)
			switch kind {
			case "missing":
				os.Remove(filepath.Join(copyPath, "acceptance.sqlite"))
			case "checksum":
				file, err := os.OpenFile(filepath.Join(copyPath, "acceptance.sqlite"), os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.WriteString("bad")
				file.Close()
			case "manifest":
				os.Remove(filepath.Join(copyPath, "manifest.json"))
			case "existing":
				os.MkdirAll(r.TargetDuckLakeDirectory, 0700)
			case "overlap":
				r.TargetDBPath = filepath.Join(copyPath, "target.db")
			case "source":
				r.SourceID = "00000000-0000-0000-0000-000000000000"
			case "fresh-bootstrap":
				r.BootstrapToken = adminToken + strings.Repeat("x", 32) // Check exact reused token separately below.
				r.BootstrapToken = adminToken
			case "version", "schema":
				manifest := readNativeManifest(t, copyPath)
				if kind == "version" {
					manifest.Version = "future-release"
				} else {
					db, err := sql.Open("sqlite", filepath.Join(copyPath, "acceptance.sqlite"))
					if err != nil {
						t.Fatal(err)
					}
					_, err = db.Exec("ALTER TABLE projects ADD COLUMN future TEXT")
					db.Close()
					if err != nil {
						t.Fatal(err)
					}
					for i, f := range manifest.Files {
						if f.Path == "acceptance.sqlite" {
							manifest.Files[i], err = hashBackupFile(context.Background(), filepath.Join(copyPath, f.Path), f.Path)
							if err != nil {
								t.Fatal(err)
							}
						}
					}
				}
				os.Remove(filepath.Join(copyPath, "manifest.json"))
				if err := writeBackupManifest(copyPath, manifest); err != nil {
					t.Fatal(err)
				}
			}
			_, err := RestoreNativeBackup(context.Background(), NativeRestoreConfig{Request: r, DuckLake: DuckLakeConfig{MemoryLimitMB: 128, Threads: 1}})
			if err == nil {
				t.Fatal("unsafe restore accepted", a)
			}
			if kind == "schema" {
				if _, err = os.Stat(r.TargetDBPath + ".native-restore-incomplete"); err != nil {
					t.Fatal("failed restore lost incomplete marker", err)
				}
				if _, err = Open(Config{DBPath: r.TargetDBPath, DuckLake: &DuckLakeConfig{Directory: r.TargetDuckLakeDirectory}}); err == nil {
					t.Fatal("startup accepted incomplete restore")
				}
			}
		})
	}
}

func TestDuckLakeS3NativeRestoreRebuildsIndependentTargetAndCleanup(t *testing.T) {
	fixture := nativeS3Fixture(t)
	root := t.TempDir()
	backup := fixture.storage
	backup.Prefix += "-backup"
	// Give the independent archive a different private HTTPS trust root from the
	// live source and restore destination. Native copy needs both simultaneously.
	if strings.HasPrefix(fixture.storage.Endpoint, "https://") {
		upstream, err := url.Parse(fixture.storage.Endpoint)
		if err != nil {
			t.Fatal(err)
		}
		forward := httputil.NewSingleHostReverseProxy(upstream)
		forward.Transport = fixture.client.Options().HTTPClient.(*http.Client).Transport
		proxy := httptest.NewTLSServer(forward)
		defer proxy.Close()
		caPath := filepath.Join(root, "backup-ca.pem")
		if err = os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: proxy.Certificate().Raw}), 0600); err != nil {
			t.Fatal(err)
		}
		backup.Endpoint, backup.CACertFile = proxy.URL, caPath
	}

	config := Config{DBPath: filepath.Join(root, "live", "queue.db"), BootstrapToken: adminToken, DuckLake: &DuckLakeConfig{Directory: filepath.Join(root, "live", "lake"), MemoryLimitMB: 256, Threads: 1, S3: &fixture.storage, MaintenanceInterval: 24 * time.Hour}, NativeBackup: &NativeBackupConfig{S3: &backup, Retain: 1}}
	s, err := Open(config)
	s3FixtureError(t, fixture, "open restore source", err)
	defer s.Close()
	s.stopNativeBackups()
	a, ingest := project(t, s)
	b, otherIngest := project(t, s)
	state := lakeMaintenanceLifecycle(t, s, a, ingest, "s3-restore", true)
	nativeRequest(t, s, otherIngest, `{"type":"track","message_id":"second-project","actor_id":"other","name":"untouched"}`, 200)
	duckLakeJourneyDrain(t, s)
	fixture.flush(t, s.lake.(*duckLakeStore), a)
	fixture.flush(t, s.lake.(*duckLakeStore), b)
	s.deliveryCancel()
	<-s.deliveryDone
	pending := `{"type":"track","message_id":"restore-pending","actor_id":"person","name":"pending"}`
	pendingID := receiptFrom(t, nativeRequest(t, s, ingest, pending, 200), 0)["event_id"]
	err = s.createNativeBackup(context.Background())
	s3FixtureError(t, fixture, "capture independent restore archive", err)
	id := s.nativeBackupStatus().LatestBundle
	source, err := s.lake.SourceID(context.Background())
	s3FixtureError(t, fixture, "read restore source ID", err)
	// Remove all original analytical objects and local source catalogs before
	// opening the backup. Successful restore can only read the independent archive.
	for _, project := range []string{a, b} {
		s3FixtureError(t, fixture, "remove original analytical source", s.lake.DeleteProject(context.Background(), project))
	}
	s3FixtureError(t, fixture, "close original restore source", s.Close())
	if err = os.RemoveAll(filepath.Dir(config.DBPath)); err != nil {
		t.Fatal(err)
	}
	targetS3 := fixture.storage
	targetS3.Prefix += "-restored"
	r := nativeRestoreTarget(t, id, "", source)
	restoreConfig := NativeRestoreConfig{Request: r, Backup: NativeBackupConfig{S3: &backup}, DuckLake: DuckLakeConfig{S3: &targetS3, MemoryLimitMB: 256, Threads: 1}}
	originalOverlap := restoreConfig
	originalOverlap.DuckLake.S3 = &fixture.storage
	if _, err = RestoreNativeBackup(context.Background(), originalOverlap); err == nil {
		t.Fatal("empty original analytical namespace was accepted as a restore target")
	}
	overlap := restoreConfig
	overlap.DuckLake.S3 = &backup
	if _, err = RestoreNativeBackup(context.Background(), overlap); err == nil {
		t.Fatal("overlapping archive and target prefix accepted")
	}
	fixture.put(t, targetS3.Prefix+"/existing", "existing target sentinel")
	if _, err = RestoreNativeBackup(context.Background(), restoreConfig); err == nil {
		t.Fatal("nonempty remote target accepted")
	}
	_, err = fixture.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(targetS3.Bucket), Key: aws.String(targetS3.Prefix + "/existing")})
	s3FixtureError(t, fixture, "clear fixture target sentinel", err)
	result, err := RestoreNativeBackup(context.Background(), restoreConfig)
	s3FixtureError(t, fixture, "restore isolated new analytical S3 target", err)
	var rotated string
	for _, key := range result.IngestionKeys {
		if key.ProjectID == a {
			rotated = key.Token
		}
	}
	if result.SourceID != source || rotated == "" {
		t.Fatal("source or ingestion namespace changed")
	}
	if err = filepath.WalkDir(r.TargetDuckLakeDirectory, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if strings.HasSuffix(path, ".parquet") {
			t.Error("remote restore materialized analytical files on local disk")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Make both the original data and the entire independent backup unavailable.
	// Restarts, native cleanup and project deletion must use only target objects.
	reader, err := newNativeBackupS3(context.Background(), backup, source)
	s3FixtureError(t, fixture, "open fixture archive cleanup", err)
	s3FixtureError(t, fixture, "remove fixture archive after completed restore", reader.remove(context.Background(), id))
	reader.Close()
	outside := fixture.storage.Prefix + "/outside-recovery-sentinel"
	fixture.put(t, outside, "source output must stay untouched")
	targetConfig := Config{DBPath: r.TargetDBPath, DuckLake: &DuckLakeConfig{Directory: r.TargetDuckLakeDirectory, S3: &targetS3, MemoryLimitMB: 256, Threads: 1, MaintenanceInterval: 24 * time.Hour}}
	restored, err := Open(targetConfig)
	s3FixtureError(t, fixture, "start restored target with archive unreachable", err)
	defer func() { restored.Close() }()
	nativeRequest(t, restored, ingest, pending, 401)
	request(t, restored, "GET", "/api/v1/projects", adminToken, "", 401)
	duckLakeJourneyDrain(t, restored)
	rows := duckLakeJourneyQuery(t, restored, a, r.BootstrapToken, "SELECT event_id,event,revision,person_id,person_properties FROM events WHERE uuid=$uuid", map[string]string{"uuid": state.receipts[0]["uuid"].(string)})
	if len(rows) != 1 || rows[0].(map[string]any)["event_id"] != state.receipts[0]["event_id"] || rows[0].(map[string]any)["revision"] != state.revision || rows[0].(map[string]any)["person_id"] != "s3-restore-person" || !strings.Contains(rows[0].(map[string]any)["person_properties"].(string), `"plan":"team"`) {
		t.Fatal("S3 restore changed edit, identity or IDs", rows)
	}
	deleted, err := restored.lake.Load(context.Background(), a, state.receipts[1]["uuid"].(string))
	if err != nil || !deleted.IsDeleted {
		t.Fatal("S3 restore lost tombstone", err)
	}
	retry := nativeRequest(t, restored, rotated, lakeMaintenancePages("s3-restore"), 200)
	if retry["duplicates"] != float64(2) || receiptFrom(t, retry, 0)["event_id"] != state.receipts[0]["event_id"] {
		t.Fatal("S3 restored retry changed IDs")
	}
	queued := nativeRequest(t, restored, rotated, pending, 200)
	if queued["duplicates"] != float64(1) || receiptFrom(t, queued, 0)["event_id"] != pendingID {
		t.Fatal("S3 restored pending queue changed IDs")
	}
	s3FixtureError(t, fixture, "stop completed restored target", restored.Close())
	restored, err = Open(targetConfig)
	s3FixtureError(t, fixture, "restart completed restored target", err)
	s3FixtureError(t, fixture, "maintain isolated restored target", restored.lake.Maintain(context.Background()))
	lake := restored.lake.(*duckLakeStore)
	for i := 0; i < 2; i++ {
		s3FixtureError(t, fixture, "cleanup isolated restored target", lake.cleanupDue(context.Background(), time.Now().Add(2*time.Hour)))
	}
	request(t, restored, "DELETE", "/api/v1/projects/"+a, r.BootstrapToken, "", 204)
	remaining := duckLakeJourneyQuery(t, restored, b, r.BootstrapToken, "SELECT count(*) AS total FROM events", nil)
	if remaining[0].(map[string]any)["total"] != "1" {
		t.Fatal("restored target cleanup changed second project", remaining)
	}
	if len(fixture.keys(t, outside)) != 1 {
		t.Fatal("restored target cleanup deleted source output")
	}
}

func TestDuckLakeNativeRestoreRefusesCanonicalRemoteOverlap(t *testing.T) {
	backup := DuckLakeS3Config{Bucket: "porcupine-fixture", Prefix: "archive", Region: "us-east-1", Endpoint: "http://127.0.0.1:1/", UsePathStyle: true}
	target := backup
	target.Endpoint = "http://127.0.0.1:1"
	target.Prefix = "archive/native-backups/source/pc_backup_test/restored"
	err := validateNativeRestoreS3Target(context.Background(), &target, &backup)
	if err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatal("normalized backup endpoint overlap was not rejected before network access", err)
	}
}

func TestDuckLakeNativeRestoreRefusesMissingAndOriginalSourceStorage(t *testing.T) {
	target := DuckLakeS3Config{Bucket: "porcupine-fixture", Prefix: "original-source/nested", Region: "us-east-1", Endpoint: "http://127.0.0.1:1"}
	if err := validateNativeRestoreSourceStorage(&target, nativeBackupStorageIdentity{}, false); err == nil {
		t.Fatal("remote target without original storage identity accepted")
	}
	if err := validateNativeRestoreSourceStorage(nil, nativeBackupStorageIdentity{}, true); err == nil {
		t.Fatal("remote archive without original storage identity accepted")
	}
	source := nativeBackupStorageIdentity{Mode: "s3", Bucket: target.Bucket, Prefix: "original-source", Region: target.Region, Endpoint: "http://127.0.0.1:2"}
	if err := validateNativeRestoreSourceStorage(&target, source, true); err == nil {
		t.Fatal("original source namespace overlap through an endpoint alias accepted")
	}
	source.Prefix = "other-source"
	if err := validateNativeRestoreSourceStorage(&target, source, true); err != nil {
		t.Fatal("independent target refused", err)
	}
}

func TestDuckLakeNativeRestoreRefusesAbsoluteOriginalDataReference(t *testing.T) {
	s, config := nativeBackupService(t)
	projectID, ingest := project(t, s)
	nativeRequest(t, s, ingest, `{"type":"track","message_id":"absolute-source-probe","actor_id":"person","name":"probe"}`, 200)
	duckLakeJourneyDrain(t, s)
	if err := s.lake.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	lake := s.lake.(*duckLakeStore)
	p, release, err := lake.leaseProject(context.Background(), projectID)
	if err != nil {
		t.Fatal(err)
	}
	var original string
	err = p.db.QueryRow(`SELECT data_file FROM ducklake_list_files('lake','current_events') LIMIT 1`).Scan(&original)
	release()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(original); err != nil {
		t.Fatal("test needs an accessible original physical file", err)
	}
	if err = s.createNativeBackup(context.Background()); err != nil {
		t.Fatal(err)
	}
	id := s.nativeBackupStatus().LatestBundle
	bundle := filepath.Join(config.NativeBackup.Directory, id)
	manifest := readNativeManifest(t, bundle)
	catalog := manifest.Catalogs[0]
	// Only this negative fixture rewires private metadata. Restore must reject
	// an otherwise checksummed archive rather than reading the original file.
	db, err := sql.Open("duckdb", filepath.Join(bundle, filepath.FromSlash(catalog.Path)))
	if err != nil {
		t.Fatal(err)
	}
	result, err := db.Exec(`UPDATE ducklake_data_file SET path=?,path_is_relative=false WHERE path LIKE '%' || ?`, original, filepath.Base(original))
	db.Close()
	if err != nil {
		t.Fatal(err)
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		t.Fatal("fixture did not replace one physical reference", changed, err)
	}
	for i, f := range manifest.Files {
		if f.Path == catalog.Path {
			manifest.Files[i], err = hashBackupFile(context.Background(), filepath.Join(bundle, filepath.FromSlash(f.Path)), f.Path)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = os.Remove(filepath.Join(bundle, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	if err = writeBackupManifest(bundle, manifest); err != nil {
		t.Fatal(err)
	}
	r := nativeRestoreTarget(t, id, bundle, manifest.SourceID)
	if _, err = RestoreNativeBackup(context.Background(), NativeRestoreConfig{Request: r, DuckLake: DuckLakeConfig{MemoryLimitMB: 256, Threads: 1}}); err == nil {
		t.Fatal("restore read an absolute original data reference outside the archive")
	}
	if _, err = os.Stat(r.TargetDBPath); !os.IsNotExist(err) {
		t.Fatal("failed restore published acceptance state", err)
	}
	if _, err = os.Stat(r.TargetDBPath + ".native-restore-incomplete"); err != nil {
		t.Fatal("failed restore did not preserve incomplete marker", err)
	}
}
