package porcupine

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

type nativeBackupOwner struct {
	SourceID string `json:"source_id"`
	WriterID string `json:"writer_id"`
}

func (s *Service) nativeBackupWriterID(ctx context.Context) (string, error) {
	if _, err := s.db.ExecContext(ctx, "INSERT INTO metadata(key,value) VALUES('native_backup_writer_id',?) ON CONFLICT(key) DO NOTHING", newID()); err != nil {
		return "", err
	}
	var writer string
	err := s.db.QueryRowContext(ctx, "SELECT value FROM metadata WHERE key='native_backup_writer_id'").Scan(&writer)
	if err != nil {
		return "", err
	}
	if len(writer) != 36 || !lakePrefixSegmentPattern.MatchString(writer) {
		return "", errors.New("invalid native backup writer identity")
	}
	return writer, nil
}
func (s *nativeBackupS3) claimOwner(ctx context.Context, source, writer string) error {
	owner := nativeBackupOwner{source, writer}
	body, err := json.Marshal(owner)
	if err != nil {
		return err
	}
	key := s.namespace + "owner.json"
	_, putErr := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(key), Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))), IfNoneMatch: aws.String("*")})
	if putErr != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(key)})
	if err != nil {
		return backupS3Error(ctx)
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
func claimLocalNativeBackupOwner(directory, source, writer string) error {
	if len(source) != 36 || !lakePrefixSegmentPattern.MatchString(source) {
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
		return syncBackupDirectory(directory)
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
