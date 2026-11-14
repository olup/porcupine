//go:build ducklake

package porcupine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type duckLakeObjectStore struct {
	config    DuckLakeS3Config
	namespace string
	client    *s3.Client
	transport *http.Transport
}

func newDuckLakeObjectStore(ctx context.Context, c DuckLakeS3Config, source string) (*duckLakeObjectStore, error) {
	// Registry IDs are generated UUIDs. Refuse malformed persisted IDs before
	// they can influence an object prefix.
	if len(source) != 36 || !lakePrefixSegmentPattern.MatchString(source) {
		return nil, errors.New("invalid analytical source identity")
	}
	client, transport, err := newNativeS3Client(ctx, c)
	if err != nil {
		return nil, analyticalS3Error(ctx)
	}
	return &duckLakeObjectStore{config: c, namespace: c.Prefix + "/" + source + "/", client: client, transport: transport}, nil
}

func analyticalS3Error(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.New("DuckLake analytical S3 operation failed; inspect the operator's storage configuration")
}

// Keep only our own constant stage, never an extension error or its SQL text.
type analyticalS3ConfigurationError struct{ stage string }

func (e *analyticalS3ConfigurationError) Error() string {
	return "DuckLake analytical S3 configuration failed at " + e.stage + "; inspect the operator's storage configuration"
}

func analyticalS3ConfigurationFailure(ctx context.Context, stage string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return &analyticalS3ConfigurationError{stage: stage}
}

func projectLakeDigest(project string) string {
	digest := sha256.Sum256([]byte(project))
	return hex.EncodeToString(digest[:])
}
func (s *duckLakeObjectStore) projectPrefix(project string) string {
	return s.namespace + projectLakeDigest(project) + "/"
}
func (s *duckLakeObjectStore) projectPath(project string) string {
	return "s3://" + s.config.Bucket + "/" + s.projectPrefix(project)
}

func (s *duckLakeObjectStore) deleteProject(ctx context.Context, project string) error {
	prefix := s.projectPrefix(project)
	// Re-list the first page after each deletion. Continuation tokens obtained
	// before deleting a page can skip surviving keys in some compatible stores.
	previousPage := ""
	for {
		page, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(s.config.Bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(1000)})
		if err != nil {
			return analyticalS3Error(ctx)
		}
		if len(page.Contents) == 0 {
			if aws.ToBool(page.IsTruncated) {
				return analyticalS3Error(ctx)
			}
			return nil
		}
		objects := make([]types.ObjectIdentifier, 0, len(page.Contents))
		for _, object := range page.Contents {
			if object.Key == nil || !strings.HasPrefix(*object.Key, prefix) {
				return analyticalS3Error(ctx)
			}
			objects = append(objects, types.ObjectIdentifier{Key: object.Key})
		}
		keyDigest := sha256.New()
		for _, object := range objects {
			keyDigest.Write([]byte(aws.ToString(object.Key)))
			keyDigest.Write([]byte{0})
		}
		currentPage := hex.EncodeToString(keyDigest.Sum(nil))
		if currentPage == previousPage {
			return analyticalS3Error(ctx)
		}
		previousPage = currentPage
		removed, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(s.config.Bucket), Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)}})
		if err != nil || len(removed.Errors) > 0 {
			return analyticalS3Error(ctx)
		}
	}
}

func (s *duckLakeObjectStore) configure(ctx context.Context, db *sql.DB, project string) (string, error) {
	return s.configureNamed(ctx, db, project, "pc_analytics_s3")
}

func (s *duckLakeObjectStore) configureNamed(ctx context.Context, db *sql.DB, project, secretName string) (string, error) {
	extensions := []string{"httpfs"}
	if s.config.AccessKeyID == "" {
		extensions = append(extensions, "aws")
	}
	for _, extension := range extensions {
		if _, err := db.ExecContext(ctx, "LOAD "+extension); err != nil {
			if _, err := db.ExecContext(ctx, "INSTALL "+extension+"; LOAD "+extension); err != nil {
				return "", analyticalS3ConfigurationFailure(ctx, "extension loading")
			}
		}
	}
	c := s.config
	for _, statement := range []string{fmt.Sprintf("SET GLOBAL http_timeout=%d", c.RequestTimeoutSeconds), "SET GLOBAL http_retries=0", "SET GLOBAL enable_server_cert_verification=true"} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return "", analyticalS3ConfigurationFailure(ctx, "HTTP options")
		}
	}
	caFile := c.CACertFile
	if c.tlsEnabled() {
		if caFile == "" {
			for _, candidate := range []string{"/etc/ssl/certs/ca-certificates.crt", "/etc/ssl/cert.pem", "/etc/pki/tls/certs/ca-bundle.crt"} {
				if file, err := os.Open(candidate); err == nil {
					info, statErr := file.Stat()
					file.Close()
					if statErr == nil && info.Mode().IsRegular() {
						caFile = candidate
						break
					}
				}
			}
		}
		if caFile == "" {
			return "", errors.New("analytical S3 HTTPS requires a CA certificate bundle")
		}
		if _, err := db.ExecContext(ctx, "SET GLOBAL ca_cert_file="+lakeLiteral(caFile)); err != nil {
			return "", analyticalS3ConfigurationFailure(ctx, "TLS trust configuration")
		}
	}
	provider := "PROVIDER credential_chain, REFRESH auto"
	if c.AccessKeyID != "" {
		provider = "PROVIDER config, VERIFY_SSL true, KEY_ID " + lakeLiteral(c.AccessKeyID) + ", SECRET " + lakeLiteral(c.SecretAccessKey)
		if c.SessionToken != "" {
			provider += ", SESSION_TOKEN " + lakeLiteral(c.SessionToken)
		}
	}
	style := "vhost"
	if c.UsePathStyle {
		style = "path"
	}
	ssl := "true"
	if !c.tlsEnabled() {
		ssl = "false"
	}
	statement := "CREATE SECRET " + secretName + " (TYPE s3, " + provider + ", REGION " + lakeLiteral(c.Region) + ", ENDPOINT " + lakeLiteral(c.secretEndpoint()) + ", URL_STYLE " + lakeLiteral(style) + ", USE_SSL " + ssl + ", SCOPE " + lakeLiteral(s.projectPath(project)) + ")"
	// A native bind error can quote the whole CREATE SECRET statement. Never
	// expose that error to delivery status, logs, query diagnostics or agents.
	if _, err := db.ExecContext(ctx, statement); err != nil {
		return "", analyticalS3ConfigurationFailure(ctx, "credential provider")
	}
	return caFile, nil
}

func pinDuckLakeStorage(ctx context.Context, db *sql.DB, root string, remote *DuckLakeS3Config) error {
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS storage_configuration (id INTEGER PRIMARY KEY CHECK(id=1),fingerprint TEXT NOT NULL)"); err != nil {
		return err
	}
	mode := "local\x00" + root
	if remote != nil {
		partition := "endpoint"
		if remote.Endpoint == "" {
			partition = "aws"
			if strings.HasPrefix(remote.Region, "cn-") {
				partition = "aws-cn"
			}
			if strings.HasPrefix(remote.Region, "us-gov-") {
				partition = "aws-us-gov"
			}
		}
		mode = "s3\x00" + remote.Bucket + "\x00" + remote.Prefix + "\x00" + remote.Endpoint + "\x00" + partition
	}
	sum := sha256.Sum256([]byte(mode))
	fingerprint := hex.EncodeToString(sum[:])
	var actual string
	err := db.QueryRowContext(ctx, "SELECT fingerprint FROM storage_configuration WHERE id=1").Scan(&actual)
	if errors.Is(err, sql.ErrNoRows) {
		if remote != nil {
			var catalogs int
			if err := db.QueryRowContext(ctx, "SELECT count(*) FROM catalogs").Scan(&catalogs); err != nil {
				return err
			}
			if catalogs != 0 {
				return errors.New("existing local DuckLake archive requires explicit migration before analytical S3 can be enabled")
			}
		}
		_, err = db.ExecContext(ctx, "INSERT INTO storage_configuration VALUES(1,?)", fingerprint)
		return err
	}
	if err != nil {
		return err
	}
	if actual != fingerprint {
		return errors.New("DuckLake analytical storage configuration changed; use the original storage configuration or an explicit migration")
	}
	return nil
}
