package lake

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/olup/porcupine/internal/exportstore"
	"github.com/olup/porcupine/internal/s3client"
)

var errHostedEffectOwnership = errors.New("invalid hosted effect artifact ownership")

func (s *DuckLakeObjectStore) HostedEffectStorageID() string {
	return exportstore.NamespaceOwnershipID(exportstore.Config{
		Bucket: s.Config.Bucket, Prefix: s.Namespace, Endpoint: s.Config.Endpoint, Region: s.Config.Region,
	})
}

// InventoryHostedEffectArtifacts lists only a verified immutable key or a
// single acquisition directory recorded before object creation.
func (s *DuckLakeObjectStore) InventoryHostedEffectArtifacts(ctx context.Context, scope HostedProjectScope, key string, attemptPrefix bool) ([]s3client.Artifact, error) {
	if !s.validHostedEffectArtifact(scope, key, attemptPrefix) {
		return nil, errHostedEffectOwnership
	}
	artifacts, err := s3client.InventoryArtifacts(ctx, s.client, s.Config.Bucket, key, func(candidate string) bool {
		if attemptPrefix {
			return strings.HasPrefix(candidate, key)
		}
		return candidate == key
	})
	if err != nil {
		return nil, analyticalS3Error(ctx)
	}
	for _, artifact := range artifacts {
		if !s.validHostedEffectArtifact(scope, artifact.Key, false) {
			return nil, errHostedEffectOwnership
		}
	}
	return artifacts, nil
}

func (s *DuckLakeObjectStore) DeleteHostedEffectArtifact(ctx context.Context, scope HostedProjectScope, storageID string, artifact s3client.Artifact) error {
	if !s.validHostedEffectArtifact(scope, artifact.Key, false) || s.HostedEffectStorageID() != storageID {
		return errHostedEffectOwnership
	}
	if err := s3client.DeleteArtifact(ctx, s.client, s.Config.Bucket, artifact); err != nil {
		return analyticalS3Error(ctx)
	}
	return nil
}

func (s *DuckLakeObjectStore) validHostedEffectArtifact(scope HostedProjectScope, key string, attemptPrefix bool) bool {
	if s == nil {
		return false
	}
	prefix, err := s.hostedStreamObjectPrefix(scope, "effects")
	if err != nil {
		return false
	}
	suffix, owned := strings.CutPrefix(key, prefix)
	attempt, name, split := strings.Cut(suffix, "/")
	if !owned || !split || len(attempt) != 36 || !LakePrefixSegmentPattern.MatchString(attempt) {
		return false
	}
	if attemptPrefix {
		return name == ""
	}
	digest, hasExtension := strings.CutSuffix(name, ".json")
	_, err = hex.DecodeString(digest)
	return hasExtension && len(digest) == 64 && err == nil && strings.ToLower(digest) == digest
}
