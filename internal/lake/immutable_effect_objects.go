package lake

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const MaxImmutableEffectObjectBytes int64 = 8 << 20

func (s *DuckLakeObjectStore) effectPrefix(scope HostedProjectScope) (string, error) {
	if scope.Key.SourceID != s.sourceID || scope.Key.ProjectID == "" || scope.CatalogID != SharedNativeCatalogID(scope.Key.SourceID, scope.Key.ProjectID) || len(scope.StreamID) != 36 || !LakePrefixSegmentPattern.MatchString(scope.StreamID) {
		return "", errors.New("immutable effect object scope is invalid")
	}
	return s.ProjectPrefix(scope.Key.ProjectID) + "effects/" + scope.StreamID + "/", nil
}

// DescribeImmutableEffectObject computes the exact immutable key without I/O.
// Reservation ownership must cover the returned acquisition prefix before PUT.
func (s *DuckLakeObjectStore) DescribeImmutableEffectObject(scope HostedProjectScope, attemptID string, body []byte) (ImmutableObjectRef, string, error) {
	prefix, err := s.effectPrefix(scope)
	if err != nil {
		return ImmutableObjectRef{}, "", err
	}
	if len(attemptID) != 36 || !LakePrefixSegmentPattern.MatchString(attemptID) || len(body) == 0 || int64(len(body)) > MaxImmutableEffectObjectBytes {
		return ImmutableObjectRef{}, "", errors.New("invalid immutable effect preparation")
	}
	digest := sha256.Sum256(body)
	hash := hex.EncodeToString(digest[:])
	prefix += attemptID + "/"
	return ImmutableObjectRef{Key: prefix + hash + ".json", SHA256: hash, Bytes: int64(len(body))}, prefix, nil
}

// PutImmutableEffectObject creates one content-addressed JSON object under an
// isolated acquisition attempt. It never overwrites an existing object.
func (s *DuckLakeObjectStore) PutImmutableEffectObject(ctx context.Context, scope HostedProjectScope, attemptID string, body []byte) (ImmutableObjectRef, error) {
	prefix, err := s.effectPrefix(scope)
	if err != nil {
		return ImmutableObjectRef{}, err
	}
	if len(attemptID) != 36 || !LakePrefixSegmentPattern.MatchString(attemptID) || len(body) == 0 || int64(len(body)) > MaxImmutableEffectObjectBytes {
		return ImmutableObjectRef{}, errors.New("immutable effect object attempt or size is invalid")
	}
	digest := sha256.Sum256(body)
	hash := hex.EncodeToString(digest[:])
	key := prefix + attemptID + "/" + hash + ".json"
	ref := ImmutableObjectRef{Key: key, SHA256: hash, Bytes: int64(len(body))}
	out, putErr := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.Config.Bucket), Key: aws.String(key), Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))), ContentType: aws.String("application/json"), IfNoneMatch: aws.String("*")})
	if putErr == nil {
		ref.VersionID = aws.ToString(out.VersionId)
		if len(ref.VersionID) > 1024 {
			return ImmutableObjectRef{}, errors.New("immutable effect version identifier exceeds bound")
		}
		return ref, nil
	}
	// A lost successful response or an immutable-key collision is resolved by
	// reading the same content address, never by replacing its bytes.
	existing, version, readErr := s.readImmutableEffectObject(ctx, scope, ref)
	if readErr == nil && bytes.Equal(existing, body) {
		ref.VersionID = version
		return ref, nil
	}
	return ImmutableObjectRef{}, analyticalS3Error(ctx)
}
func (s *DuckLakeObjectStore) ReadImmutableEffectObject(ctx context.Context, scope HostedProjectScope, ref ImmutableObjectRef) ([]byte, error) {
	raw, _, err := s.readImmutableEffectObject(ctx, scope, ref)
	return raw, err
}
func (s *DuckLakeObjectStore) readImmutableEffectObject(ctx context.Context, scope HostedProjectScope, ref ImmutableObjectRef) ([]byte, string, error) {
	prefix, err := s.effectPrefix(scope)
	if err != nil {
		return nil, "", err
	}
	suffix, ok := strings.CutPrefix(ref.Key, prefix)
	parts := strings.Split(suffix, "/")
	digest, hashErr := hex.DecodeString(ref.SHA256)
	if len(ref.VersionID) > 1024 || !ok || len(parts) != 2 || len(parts[0]) != 36 || !LakePrefixSegmentPattern.MatchString(parts[0]) || parts[1] != ref.SHA256+".json" || hashErr != nil || len(digest) != sha256.Size || ref.Bytes <= 0 || ref.Bytes > MaxImmutableEffectObjectBytes {
		return nil, "", errors.New("immutable effect object reference is invalid")
	}
	input := &s3.GetObjectInput{Bucket: aws.String(s.Config.Bucket), Key: aws.String(ref.Key)}
	if ref.VersionID != "" {
		input.VersionId = aws.String(ref.VersionID)
	}
	out, err := s.client.GetObject(ctx, input)
	if err != nil {
		return nil, "", analyticalS3Error(ctx)
	}
	defer out.Body.Close()
	if ref.VersionID != "" && aws.ToString(out.VersionId) != ref.VersionID {
		return nil, "", errors.New("immutable effect version pin mismatch")
	}
	raw, err := io.ReadAll(io.LimitReader(out.Body, ref.Bytes+1))
	if err != nil || int64(len(raw)) != ref.Bytes {
		return nil, "", errors.New("immutable effect object size mismatch")
	}
	actual := sha256.Sum256(raw)
	if hex.EncodeToString(actual[:]) != ref.SHA256 {
		return nil, "", errors.New("immutable effect object checksum mismatch")
	}
	return raw, aws.ToString(out.VersionId), nil
}
