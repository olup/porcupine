// Package exportstore writes private export objects through S3 multipart uploads.
package exportstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
)

const (
	PartSize = 8 << 20
	MaxParts = 10000
)

var (
	ErrConfig      = errors.New("invalid S3 export configuration")
	ErrKey         = errors.New("invalid export object key")
	ErrStorage     = errors.New("S3 export operation failed")
	ErrClosed      = errors.New("multipart upload is closed")
	ErrFailed      = errors.New("multipart upload has failed; abort required")
	ErrPartLimit   = errors.New("multipart upload exceeds 10000 parts")
	segmentPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	regionPattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)
	bucketPattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
)

type Config struct {
	Region, Bucket, Prefix, Endpoint           string
	AccessKeyID, SecretAccessKey, SessionToken string
	UsePathStyle                               bool
}

type Store struct {
	client         *s3.Client
	presigner      *s3.PresignClient
	bucket, prefix string
	transport      *http.Transport
}

// Upload is a sequential writer. The caller must serialize its methods, validate
// the complete query before Complete, and Abort on every failure with a fresh context.
type Upload struct {
	store     *Store
	ctx       context.Context
	key, id   string
	buffer    []byte
	parts     []types.CompletedPart
	failed    error
	closed    bool
	completed bool
}

func New(ctx context.Context, c Config) (*Store, error) {
	prefix := strings.TrimSuffix(c.Prefix, "/")
	if !regionPattern.MatchString(c.Region) || !bucketPattern.MatchString(c.Bucket) || strings.Contains(c.Bucket, "..") || net.ParseIP(c.Bucket) != nil || !safePath(prefix) {
		return nil, ErrConfig
	}
	if (c.AccessKeyID == "") != (c.SecretAccessKey == "") || c.SessionToken != "" && c.AccessKeyID == "" {
		return nil, ErrConfig
	}
	if c.Endpoint != "" && !ValidEndpoint(c.Endpoint) {
		return nil, ErrConfig
	}
	transport := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		ExpectContinueTimeout: time.Second,
		IdleConnTimeout:       60 * time.Second,
		MaxIdleConns:          16, MaxIdleConnsPerHost: 8, MaxResponseHeaderBytes: 64 << 10,
	}
	httpClient := &http.Client{Transport: boundedResponses{transport}, Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return ErrStorage }}
	options := []func(*config.LoadOptions) error{
		config.WithRegion(c.Region), config.WithHTTPClient(httpClient),
		config.WithLogger(logging.Nop{}), config.WithLogConfigurationWarnings(false),
		config.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) { o.MaxAttempts = 3; o.MaxBackoff = time.Second })
		}),
	}
	if c.AccessKeyID != "" {
		options = append(options, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, c.SessionToken)))
	}
	cfg, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, storageError(ctx)
	}
	// Credentials use the default chain, but endpoint overrides come only from Config.
	cfg.BaseEndpoint = nil
	cfg.ConfigSources = nil
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = c.UsePathStyle
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
		// Avoid optional streaming checksum trailers unsupported by some S3-compatible stores.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return &Store{client: client, presigner: s3.NewPresignClient(client), bucket: c.Bucket, prefix: prefix + "/", transport: transport}, nil
}

// Close releases idle connections after callers have stopped all uploads.
func (s *Store) Close() { s.transport.CloseIdleConnections() }

func safePath(value string) bool {
	if value == "" || len(value) > 1024 {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if !segmentPattern.MatchString(part) || part == "." || part == ".." {
			return false
		}
	}
	return true
}

// ValidEndpoint accepts HTTPS and explicitly local HTTP stores.
func ValidEndpoint(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" {
		return false
	}
	if u.Scheme == "https" {
		return true
	}
	if u.Scheme != "http" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	ip := net.ParseIP(host)
	return host == "localhost" || host == "minio" || strings.HasSuffix(host, ".internal") || strings.HasSuffix(host, ".local") || ip != nil && (ip.IsLoopback() || ip.IsPrivate())
}

func (s *Store) objectKey(key string) (string, error) {
	if !safePath(key) || len(s.prefix)+len(key) > 1024 {
		return "", ErrKey
	}
	return s.prefix + key, nil
}

func storageError(ctx context.Context) error {
	if ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrStorage, ctx.Err())
	}
	return ErrStorage
}

func (s *Store) Begin(ctx context.Context, key string) (*Upload, error) {
	fullKey, err := s.objectKey(key)
	if err != nil {
		return nil, err
	}
	out, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(s.bucket), Key: aws.String(fullKey), ContentType: aws.String("application/gzip")})
	if err != nil || out.UploadId == nil || *out.UploadId == "" {
		return nil, storageError(ctx)
	}
	return &Upload{store: s, ctx: ctx, key: key, id: *out.UploadId, buffer: make([]byte, 0, PartSize)}, nil
}

func (u *Upload) ID() string { return u.id }

func (u *Upload) Write(p []byte) (int, error) {
	if u.closed {
		return 0, ErrClosed
	}
	if u.failed != nil {
		return 0, u.failed
	}
	written := 0
	for len(p) > 0 {
		if len(u.parts) >= MaxParts {
			u.failed = ErrPartLimit
			return written, u.failed
		}
		n := min(len(p), PartSize-len(u.buffer))
		u.buffer = append(u.buffer, p[:n]...)
		p = p[n:]
		written += n
		if len(u.buffer) == PartSize {
			if err := u.uploadPart(u.ctx); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func (u *Upload) uploadPart(ctx context.Context) error {
	if len(u.parts) >= MaxParts {
		u.failed = ErrPartLimit
		return u.failed
	}
	key, _ := u.store.objectKey(u.key)
	out, err := u.store.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(u.store.bucket), Key: aws.String(key), UploadId: aws.String(u.id), PartNumber: aws.Int32(int32(len(u.parts) + 1)), Body: bytes.NewReader(u.buffer), ContentLength: aws.Int64(int64(len(u.buffer)))})
	if err != nil || out.ETag == nil || *out.ETag == "" {
		u.failed = storageError(ctx)
		return u.failed
	}
	u.parts = append(u.parts, types.CompletedPart{ETag: out.ETag, PartNumber: aws.Int32(int32(len(u.parts) + 1))})
	u.buffer = u.buffer[:0]
	return nil
}

func (u *Upload) Complete(ctx context.Context) error {
	if u.completed {
		return nil
	}
	if u.closed {
		return ErrClosed
	}
	if u.failed != nil {
		return ErrFailed
	}
	if len(u.buffer) > 0 || len(u.parts) == 0 {
		if err := u.uploadPart(ctx); err != nil {
			return err
		}
	}
	key, _ := u.store.objectKey(u.key)
	_, err := u.store.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(u.store.bucket), Key: aws.String(key), UploadId: aws.String(u.id), MultipartUpload: &types.CompletedMultipartUpload{Parts: u.parts}})
	if err != nil {
		u.failed = storageError(ctx)
		return u.failed
	}
	u.closed, u.completed = true, true
	u.buffer = nil
	u.parts = nil
	return nil
}

func (u *Upload) Abort(ctx context.Context) error {
	if u.completed {
		return ErrClosed
	}
	if u.closed {
		return nil
	}
	if err := u.store.Abort(ctx, u.key, u.id); err != nil {
		return err
	}
	u.closed = true
	u.buffer = nil
	u.parts = nil
	return nil
}

func (s *Store) Abort(ctx context.Context, key, uploadID string) error {
	fullKey, err := s.objectKey(key)
	if err != nil {
		return err
	}
	if uploadID == "" || len(uploadID) > 4096 {
		return ErrKey
	}
	_, err = s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(s.bucket), Key: aws.String(fullKey), UploadId: aws.String(uploadID)})
	var apiError smithy.APIError
	if errors.As(err, &apiError) && apiError.ErrorCode() == "NoSuchUpload" {
		return nil
	}
	if err != nil {
		return storageError(ctx)
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	fullKey, err := s.objectKey(key)
	if err != nil {
		return err
	}
	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(fullKey)})
	if err != nil {
		return storageError(ctx)
	}
	return nil
}

func (s *Store) Presign(ctx context.Context, key string, ttl time.Duration) (string, error) {
	fullKey, err := s.objectKey(key)
	if err != nil {
		return "", err
	}
	if ttl < time.Second || ttl > 7*24*time.Hour {
		return "", ErrConfig
	}
	out, err := s.presigner.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(fullKey)}, func(o *s3.PresignOptions) { o.Expires = ttl })
	if err != nil {
		return "", storageError(ctx)
	}
	return out.URL, nil
}

type boundedResponses struct{ next http.RoundTripper }

func (b boundedResponses) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := b.next.RoundTrip(request)
	if err == nil && response.Body != nil {
		limit := int64(1 << 20)
		if request.Method == http.MethodGet && response.StatusCode >= 200 && response.StatusCode < 300 {
			limit = MaxManifestBytes + 1
		}
		response.Body = &limitedBody{Reader: io.LimitReader(response.Body, limit), Closer: response.Body}
	}
	return response, err
}

type limitedBody struct {
	io.Reader
	io.Closer
}
