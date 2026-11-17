package s3client

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

var ErrArtifactIdentity = errors.New("invalid S3 artifact inventory identity")

type Artifact struct {
	Key, VersionID, UploadID string
}

// InventoryArtifacts lists exact provider identities accepted by owns. The
// caller supplies a previously verified key or owned prefix.
func InventoryArtifacts(ctx context.Context, client *s3.Client, bucket, prefix string, owns func(string) bool) ([]Artifact, error) {
	versions, err := InventoryObjectVersions(ctx, client, bucket, prefix, owns)
	if err != nil {
		return nil, err
	}
	uploads, err := InventoryMultipartUploads(ctx, client, bucket, prefix, owns)
	if err != nil {
		return nil, err
	}
	return append(versions, uploads...), nil
}

func InventoryObjectVersions(ctx context.Context, client *s3.Client, bucket, prefix string, owns func(string) bool) ([]Artifact, error) {
	var artifacts []Artifact
	appendVersion := func(candidate, version *string) error {
		key := aws.ToString(candidate)
		if owns(key) {
			id := aws.ToString(version)
			if id == "" || len(id) > 1024 {
				return ErrArtifactIdentity
			}
			artifacts = append(artifacts, Artifact{Key: key, VersionID: id})
		}
		return nil
	}
	versions := &s3.ListObjectVersionsInput{Bucket: aws.String(bucket), Prefix: aws.String(prefix)}
	seen := make(map[[2]string]struct{})
	for {
		page, err := client.ListObjectVersions(ctx, versions)
		if err != nil {
			return nil, err
		}
		for _, version := range page.Versions {
			if err := appendVersion(version.Key, version.VersionId); err != nil {
				return nil, err
			}
		}
		for _, marker := range page.DeleteMarkers {
			if err := appendVersion(marker.Key, marker.VersionId); err != nil {
				return nil, err
			}
		}
		if !aws.ToBool(page.IsTruncated) {
			break
		}
		if err := rememberArtifactMarker(seen, page.NextKeyMarker, page.NextVersionIdMarker); err != nil {
			return nil, err
		}
		versions.KeyMarker, versions.VersionIdMarker = page.NextKeyMarker, page.NextVersionIdMarker
	}
	return artifacts, nil
}

func InventoryMultipartUploads(ctx context.Context, client *s3.Client, bucket, prefix string, owns func(string) bool) ([]Artifact, error) {
	var artifacts []Artifact
	uploads := &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket), Prefix: aws.String(prefix)}
	seen := make(map[[2]string]struct{})
	for {
		page, err := client.ListMultipartUploads(ctx, uploads)
		if err != nil {
			return nil, err
		}
		for _, upload := range page.Uploads {
			key := aws.ToString(upload.Key)
			if owns(key) {
				id := aws.ToString(upload.UploadId)
				if id == "" || len(id) > 4096 {
					return nil, ErrArtifactIdentity
				}
				artifacts = append(artifacts, Artifact{Key: key, UploadID: id})
			}
		}
		if !aws.ToBool(page.IsTruncated) {
			return artifacts, nil
		}
		if err := rememberArtifactMarker(seen, page.NextKeyMarker, page.NextUploadIdMarker); err != nil {
			return nil, err
		}
		uploads.KeyMarker, uploads.UploadIdMarker = page.NextKeyMarker, page.NextUploadIdMarker
	}
}

func rememberArtifactMarker(seen map[[2]string]struct{}, key, id *string) error {
	marker := [2]string{aws.ToString(key), aws.ToString(id)}
	if _, duplicate := seen[marker]; duplicate || marker[0] == "" {
		return ErrArtifactIdentity
	}
	seen[marker] = struct{}{}
	return nil
}

func DeleteArtifact(ctx context.Context, client *s3.Client, bucket string, a Artifact) error {
	if a.Key == "" || len(a.VersionID) > 1024 || len(a.UploadID) > 4096 || (a.VersionID == "") == (a.UploadID == "") {
		return ErrArtifactIdentity
	}
	var err error
	if a.UploadID != "" {
		_, err = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(a.Key), UploadId: aws.String(a.UploadID),
		})
	} else {
		_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(a.Key), VersionId: aws.String(a.VersionID),
		})
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		if a.UploadID != "" && code == "NoSuchUpload" || a.VersionID != "" && (code == "NoSuchVersion" || code == "NoSuchKey") {
			return nil
		}
	}
	return err
}
