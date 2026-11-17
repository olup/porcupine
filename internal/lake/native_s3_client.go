package lake

import (
	"context"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/olup/porcupine/internal/s3client"
)

func NewNativeS3Client(ctx context.Context, c DuckLakeS3Config) (*s3.Client, *http.Transport, error) {
	return newNativeS3ClientWithTimeout(ctx, c, 45*time.Second)
}

func newNativeS3ClientWithTimeout(ctx context.Context, c DuckLakeS3Config, timeout time.Duration) (*s3.Client, *http.Transport, error) {
	return s3client.New(ctx, s3client.Config{
		Region: c.Region, Endpoint: c.Endpoint, AccessKeyID: c.AccessKeyID, SecretAccessKey: c.SecretAccessKey, SessionToken: c.SessionToken,
		UsePathStyle: c.UsePathStyle, CACertFile: c.CACertFile, Timeout: timeout,
	})
}
