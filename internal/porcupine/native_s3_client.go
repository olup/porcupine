package porcupine

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"
)

func newNativeS3Client(ctx context.Context, c DuckLakeS3Config) (*s3.Client, *http.Transport, error) {
	return newNativeS3ClientWithTimeout(ctx, c, 45*time.Second)
}

func newNativeS3ClientWithTimeout(ctx context.Context, c DuckLakeS3Config, timeout time.Duration) (*s3.Client, *http.Transport, error) {
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 15 * time.Second, IdleConnTimeout: 60 * time.Second, MaxIdleConns: 16, MaxIdleConnsPerHost: 8, MaxResponseHeaderBytes: 64 << 10}
	if c.CACertFile != "" {
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		pem, err := os.ReadFile(c.CACertFile)
		if err != nil || !roots.AppendCertsFromPEM(pem) {
			return nil, nil, errors.New("S3 CA certificate file is unavailable")
		}
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	}
	options := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(c.Region), awsconfig.WithLogger(logging.Nop{}), awsconfig.WithLogConfigurationWarnings(false),
		awsconfig.WithHTTPClient(&http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("analytical S3 redirects are disabled") }}),
		awsconfig.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) { o.MaxAttempts = 3; o.MaxBackoff = time.Second })
		}),
	}
	if c.AccessKeyID != "" {
		options = append(options, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, c.SessionToken)))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, nil, errors.New("S3 client configuration failed")
	}
	// Endpoint overrides are explicit operator configuration, never ambient AWS
	// endpoint environment variables that differ from the native engine's scope.
	cfg.BaseEndpoint = nil
	cfg.ConfigSources = nil
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = c.UsePathStyle
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return client, transport, nil
}
