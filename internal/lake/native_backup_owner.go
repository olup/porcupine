package lake

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (s *NativeBackupS3) ClaimOwner(ctx context.Context, source, writer string) error {
	owner := nativeBackupOwner{source, writer}
	body, err := json.Marshal(owner)
	if err != nil {
		return err
	}
	key := s.Namespace + "owner.json"
	_, putErr := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(key), Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))), IfNoneMatch: aws.String("*")})
	if putErr != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(key)})
	if err != nil {
		return BackupS3Error(ctx)
	}
	defer out.Body.Close()
	stored, err := io.ReadAll(io.LimitReader(out.Body, 4097))
	var actual nativeBackupOwner
	if err != nil || len(stored) > 4096 || json.Unmarshal(stored, &actual) != nil {
		return errors.New("invalid native backup destination owner")
	}
	if actual != owner {
		return errors.New("native backup destination belongs to another writer; configure a fresh backup prefix")
	}
	return nil
}

func ClaimLocalNativeBackupOwner(directory, source, writer string) error {
	if len(source) != 36 || !LakePrefixSegmentPattern.MatchString(source) {
		return errors.New("invalid native backup source identity")
	}
	owner := nativeBackupOwner{source, writer}
	body, err := json.Marshal(owner)
	if err != nil {
		return err
	}
	path := filepath.Join(directory, ".porcupine-owner-"+source+".json")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err == nil {
		_, writeErr := file.Write(body)
		syncErr := file.Sync()
		closeErr := file.Close()
		if failure := errors.Join(writeErr, syncErr, closeErr); failure != nil {
			return failure
		}
		return SyncBackupDirectory(directory)
	}
	if !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 4096 {
		return errors.New("invalid native backup destination owner")
	}
	stored, err := os.ReadFile(path)
	var actual nativeBackupOwner
	if err != nil || json.Unmarshal(stored, &actual) != nil {
		return errors.New("invalid native backup destination owner")
	}
	if actual != owner {
		return errors.New("native backup destination belongs to another writer; configure a fresh backup directory")
	}
	return nil
}

type nativeBackupOwner struct {
	SourceID string `json:"source_id"`
	WriterID string `json:"writer_id"`
}
