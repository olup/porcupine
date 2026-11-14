package porcupine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
)

type nativeBackupReader interface {
	Manifest(context.Context, string) (nativeBackupManifest, error)
	Open(context.Context, string, backupFile) (io.ReadCloser, error)
}
type localNativeBackupReader struct{ directory string }

func (r *localNativeBackupReader) Manifest(ctx context.Context, id string) (nativeBackupManifest, error) {
	if !validNativeBackupID(id) {
		return nativeBackupManifest{}, errors.New("invalid backup identity")
	}
	return verifyNativeBackup(ctx, filepath.Join(r.directory, id))
}
func (r *localNativeBackupReader) Open(ctx context.Context, id string, f backupFile) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validNativeBackupID(id) || !validBackupFile(f) {
		return nil, errors.New("unsafe backup inventory")
	}
	path := filepath.Join(r.directory, id, filepath.FromSlash(f.Path))
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() != f.Bytes {
		return nil, errors.New("backup file size mismatch")
	}
	return os.Open(path)
}
func newNativeBackupReader(ctx context.Context, c NativeBackupConfig, source string) (nativeBackupReader, error) {
	if c.S3 == nil {
		return &localNativeBackupReader{c.Directory}, nil
	}
	return newNativeBackupS3(ctx, *c.S3, source)
}

type nativeBackupS3 struct {
	config    DuckLakeS3Config
	namespace string
	client    *s3.Client
	transport *http.Transport
}

func newNativeBackupS3(ctx context.Context, c DuckLakeS3Config, source string) (*nativeBackupS3, error) {
	if len(source) != 36 || !lakePrefixSegmentPattern.MatchString(source) {
		return nil, errors.New("invalid backup source identity")
	}
	normalized, err := normalizeDuckLakeS3(&c)
	if err != nil {
		return nil, errors.New("invalid backup S3 configuration")
	}
	timeout := time.Hour
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			return nil, ctx.Err()
		}
	}
	client, transport, err := newNativeS3ClientWithTimeout(ctx, *normalized, timeout)
	if err != nil {
		return nil, backupS3Error(ctx)
	}
	return &nativeBackupS3{*normalized, normalized.Prefix + "/native-backups/" + source + "/", client, transport}, nil
}
func (s *nativeBackupS3) Close() error { s.transport.CloseIdleConnections(); return nil }
func backupS3Error(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.New("native backup S3 operation failed; inspect the operator's backup configuration")
}
func validNativeBackupID(id string) bool {
	return strings.HasPrefix(id, "pc_backup_") && len(id) > len("pc_backup_") && lakePrefixSegmentPattern.MatchString(id)
}
func validBackupFile(f backupFile) bool {
	p := filepath.FromSlash(f.Path)
	_, err := hex.DecodeString(f.SHA256)
	return filepath.IsLocal(p) && p != "." && f.Path == filepath.ToSlash(filepath.Clean(p)) && f.Path != "manifest.json" && f.Bytes >= 0 && len(f.SHA256) == 64 && err == nil && !strings.Contains(f.Path, "\\")
}
func (s *nativeBackupS3) key(id, path string) (string, error) {
	if !validNativeBackupID(id) {
		return "", errors.New("invalid backup identity")
	}
	if path != "manifest.json" && !validBackupFile(backupFile{Path: path, SHA256: strings.Repeat("0", 64)}) {
		return "", errors.New("unsafe backup path")
	}
	return s.namespace + id + "/" + path, nil
}
func (s *nativeBackupS3) Open(ctx context.Context, id string, f backupFile) (io.ReadCloser, error) {
	if !validBackupFile(f) {
		return nil, errors.New("unsafe backup inventory")
	}
	key, err := s.key(id, f.Path)
	if err != nil {
		return nil, err
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(key)})
	if err != nil {
		return nil, backupS3Error(ctx)
	}
	if aws.ToInt64(out.ContentLength) != f.Bytes {
		out.Body.Close()
		return nil, errors.New("backup file size mismatch")
	}
	return &boundedBackupBody{Reader: io.LimitReader(out.Body, f.Bytes+1), Closer: out.Body}, nil
}

type boundedBackupBody struct {
	io.Reader
	io.Closer
}

func (s *nativeBackupS3) Manifest(ctx context.Context, id string) (nativeBackupManifest, error) {
	var m nativeBackupManifest
	key, err := s.key(id, "manifest.json")
	if err != nil {
		return m, err
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(key)})
	if err != nil {
		return m, backupS3Error(ctx)
	}
	defer out.Body.Close()
	if aws.ToInt64(out.ContentLength) > nativeBackupManifestLimit {
		return m, errors.New("backup manifest is too large")
	}
	body, err := io.ReadAll(io.LimitReader(out.Body, nativeBackupManifestLimit+1))
	if err != nil || len(body) > nativeBackupManifestLimit {
		return m, errors.New("invalid backup manifest")
	}
	if json.Unmarshal(body, &m) != nil {
		return m, errors.New("invalid backup manifest")
	}
	if err := validateNativeBackupManifest(m); err != nil {
		return m, err
	}
	if err := validateNativeBackupStorageIdentity(m.SourceStorage, false); err != nil {
		return m, err
	}
	if m.ID != id || s.namespace != s.config.Prefix+"/native-backups/"+m.SourceID+"/" {
		return m, errors.New("backup source identity mismatch")
	}
	return m, nil
}
func (s *nativeBackupS3) put(ctx context.Context, id, path string, body io.Reader, size int64) error {
	if size < 0 || size > 5<<30 {
		return errors.New("native backup object exceeds the 5 GiB single-upload limit")
	}
	key, err := s.key(id, path)
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(key), Body: body, ContentLength: aws.Int64(size)}, func(o *s3.Options) {
		o.RetryMaxAttempts = 1
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware(stack)
		})
	})
	if err != nil {
		return backupS3Error(ctx)
	}
	return nil
}
func (s *nativeBackupS3) transfer(ctx context.Context, id, path string, body io.Reader, size int64) (backupFile, error) {
	hash := sha256.New()
	f := backupFile{Path: path, Bytes: size}
	limited := &io.LimitedReader{R: body, N: size + 1}
	counted := &backupCountingReader{reader: io.TeeReader(limited, hash)}
	if err := s.put(ctx, id, path, counted, size); err != nil {
		return f, err
	}
	if counted.bytes != size {
		return f, errors.New("backup source object size changed")
	}
	f.SHA256 = hex.EncodeToString(hash.Sum(nil))
	if err := s.verify(ctx, id, f); err != nil {
		return f, err
	}
	return f, nil
}

type backupCountingReader struct {
	reader io.Reader
	bytes  int64
}

func (r *backupCountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.bytes += int64(n)
	return n, err
}
func (s *nativeBackupS3) verify(ctx context.Context, id string, f backupFile) error {
	body, err := s.Open(ctx, id, f)
	if err != nil {
		return err
	}
	defer body.Close()
	h := sha256.New()
	n, err := io.CopyBuffer(h, body, make([]byte, 128<<10))
	if err != nil {
		return backupS3Error(ctx)
	}
	if n != f.Bytes || hex.EncodeToString(h.Sum(nil)) != f.SHA256 {
		return errors.New("backup destination checksum mismatch")
	}
	return nil
}
func (s *nativeBackupS3) publish(ctx context.Context, m nativeBackupManifest) error {
	if err := validateNativeBackupManifest(m); err != nil {
		return err
	}
	if err := validateNativeBackupStorageIdentity(m.SourceStorage, false); err != nil {
		return err
	}
	body, err := json.Marshal(m)
	if err != nil || len(body) > nativeBackupManifestLimit {
		return errors.New("backup manifest is too large")
	}
	if err := s.put(ctx, m.ID, "manifest.json", bytes.NewReader(body), int64(len(body))); err != nil {
		return err
	}
	read, err := s.Manifest(ctx, m.ID)
	if err != nil {
		return err
	}
	roundTrip, marshalErr := json.Marshal(read)
	if marshalErr != nil || !bytes.Equal(roundTrip, body) {
		return errors.New("backup publication verification failed")
	}
	return nil
}
func (s *nativeBackupS3) list(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: aws.String(s.config.Bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(1000)})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, backupS3Error(ctx)
		}
		for _, o := range page.Contents {
			key := aws.ToString(o.Key)
			if !strings.HasPrefix(key, prefix) {
				return nil, errors.New("backup S3 inventory escaped its namespace")
			}
			keys = append(keys, key)
			if len(keys) > nativeBackupFileLimit+1 {
				return nil, errors.New("backup destination inventory limit reached")
			}
		}
	}
	return keys, nil
}
func (s *nativeBackupS3) remove(ctx context.Context, id string) error {
	prefix, err := s.key(id, "manifest.json")
	if err != nil {
		return err
	}
	prefix = strings.TrimSuffix(prefix, "manifest.json")
	keys, err := s.list(ctx, prefix)
	if err != nil {
		return err
	}
	// Unpublish first so interrupted retention never leaves a complete marker over partial data.
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(prefix + "manifest.json")}); err != nil {
		return backupS3Error(ctx)
	}
	for len(keys) > 0 {
		n := min(1000, len(keys))
		objects := make([]types.ObjectIdentifier, n)
		for i, key := range keys[:n] {
			objects[i].Key = aws.String(key)
		}
		out, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(s.config.Bucket), Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)}})
		if err != nil || len(out.Errors) > 0 {
			return backupS3Error(ctx)
		}
		keys = keys[n:]
	}
	return nil
}
func (s *nativeBackupS3) cleanupIncomplete(ctx context.Context) error {
	ids, err := s.bundleIDs(ctx)
	if err != nil {
		return err
	}
	for id, complete := range ids {
		if !complete {
			if err := s.remove(ctx, id); err != nil {
				return err
			}
		}
	}
	return nil
}
func (s *nativeBackupS3) bundleIDs(ctx context.Context) (map[string]bool, error) {
	ids := map[string]bool{}
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: aws.String(s.config.Bucket), Prefix: aws.String(s.namespace), MaxKeys: aws.Int32(1000)})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, backupS3Error(ctx)
		}
		for _, o := range page.Contents {
			key := aws.ToString(o.Key)
			if !strings.HasPrefix(key, s.namespace) {
				return nil, errors.New("backup inventory escaped source namespace")
			}
			id, rest, ok := strings.Cut(strings.TrimPrefix(key, s.namespace), "/")
			if ok && validNativeBackupID(id) {
				ids[id] = ids[id] || rest == "manifest.json"
				if len(ids) > 1001 {
					return nil, errors.New("backup destination bundle limit reached")
				}
			}
		}
	}
	return ids, nil
}
func (s *nativeBackupS3) prune(ctx context.Context, retain int, protected string) error {
	ids, err := s.bundleIDs(ctx)
	if err != nil {
		return err
	}
	type completed struct {
		id      string
		created time.Time
	}
	var all []completed
	for id, complete := range ids {
		if !complete {
			continue
		}
		m, err := s.Manifest(ctx, id)
		if err != nil {
			continue
		}
		created, err := time.Parse(time.RFC3339Nano, m.CreatedAt)
		if err == nil {
			all = append(all, completed{id, created})
		}
	}
	found := false
	for _, c := range all {
		found = found || c.id == protected
	}
	if !found {
		return errors.New("newly verified backup missing from retention inventory")
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].id == protected {
			return all[j].id != protected
		}
		if all[j].id == protected {
			return false
		}
		return all[i].created.After(all[j].created)
	})
	for _, c := range all[min(retain, len(all)):] {
		if err := s.remove(ctx, c.id); err != nil {
			return err
		}
	}
	return nil
}
