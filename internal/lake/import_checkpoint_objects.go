package lake

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const MaxImmutableImportCheckpointBytes int64 = 2 << 20

// HostedImportCheckpointPrefix identifies the parser checkpoint namespace
// reserved for one import file in its existing hosted stream.
func (s *DuckLakeObjectStore) HostedImportCheckpointPrefix(scope HostedProjectScope, job, file string) (string, error) {
	if job == "" || file == "" || len(job) > 200 || len(file) > 200 || scope.Key.SourceID != s.sourceID || scope.Key.ProjectID == "" || scope.CatalogID != SharedNativeCatalogID(scope.Key.SourceID, scope.Key.ProjectID) || len(scope.StreamID) != 36 || !LakePrefixSegmentPattern.MatchString(scope.StreamID) {
		return "", errors.New("immutable import checkpoint object scope is invalid")
	}
	return s.ProjectPrefix(scope.Key.ProjectID) + "imports/" + scope.StreamID + "/" + importCheckpointOwner(job, file) + "/", nil
}

// PutImmutableImportCheckpoint stores bounded private parser state under the
// exact project stream, job and file. It never overwrites an existing object.
func (s *DuckLakeObjectStore) PutImmutableImportCheckpoint(ctx context.Context, scope HostedProjectScope, job, file string, body []byte) (ImmutableObjectRef, error) {
	prefix, err := s.HostedImportCheckpointPrefix(scope, job, file)
	if err != nil {
		return ImmutableObjectRef{}, err
	}
	if len(body) == 0 || int64(len(body)) > MaxImmutableImportCheckpointBytes {
		return ImmutableObjectRef{}, errors.New("immutable import checkpoint object attempt or size is invalid")
	}
	digest := sha256.Sum256(body)
	hash := hex.EncodeToString(digest[:])
	key := prefix + hash + ".json"
	ref := ImmutableObjectRef{Key: key, SHA256: hash, Bytes: int64(len(body))}
	out, putErr := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.Config.Bucket), Key: aws.String(key), Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))), ContentType: aws.String("application/json"), IfNoneMatch: aws.String("*")})
	if putErr == nil {
		ref.VersionID = aws.ToString(out.VersionId)
		if len(ref.VersionID) > 1024 {
			return ImmutableObjectRef{}, errors.New("immutable import checkpoint version identifier exceeds bound")
		}
		return ref, nil
	}
	// A lost successful response or an immutable-key collision is resolved by
	// reading the same content address, never by replacing its bytes.
	existing, version, readErr := s.readImmutableImportCheckpoint(ctx, scope, job, file, ref)
	if readErr == nil && bytes.Equal(existing, body) {
		ref.VersionID = version
		return ref, nil
	}
	return ImmutableObjectRef{}, analyticalS3Error(ctx)
}
func (s *DuckLakeObjectStore) ReadImmutableImportCheckpoint(ctx context.Context, scope HostedProjectScope, job, file string, ref ImmutableObjectRef) ([]byte, error) {
	raw, _, err := s.readImmutableImportCheckpoint(ctx, scope, job, file, ref)
	return raw, err
}
func (s *DuckLakeObjectStore) readImmutableImportCheckpoint(ctx context.Context, scope HostedProjectScope, job, file string, ref ImmutableObjectRef) ([]byte, string, error) {
	prefix, err := s.HostedImportCheckpointPrefix(scope, job, file)
	if err != nil {
		return nil, "", err
	}
	suffix, ok := strings.CutPrefix(ref.Key, prefix)
	digest, hashErr := hex.DecodeString(ref.SHA256)
	if len(ref.VersionID) > 1024 || !ok || suffix != ref.SHA256+".json" || hashErr != nil || len(digest) != sha256.Size || ref.Bytes <= 0 || ref.Bytes > MaxImmutableImportCheckpointBytes {
		return nil, "", errors.New("immutable import checkpoint object reference is invalid")
	}
	return s.readImmutableObject(ctx, ref, "immutable import checkpoint")
}

func importCheckpointOwner(job, file string) string {
	raw, _ := json.Marshal([]string{job, file})
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
