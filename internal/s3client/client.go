// Package s3client constructs operator-configured S3 clients. Each caller keeps
// its own credentials, transport, and object namespace.
package s3client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
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

type Config struct {
	Region, Endpoint                           string
	AccessKeyID, SecretAccessKey, SessionToken string
	UsePathStyle                               bool
	CACertFile                                 string
	Timeout                                    time.Duration
	// Zero leaves the body uncapped. Callers streaming objects choose their own
	// byte limits; multipart stores cap XML responses and manifest downloads here.
	ResponseBytes, GetResponseBytes int64
}

func New(ctx context.Context, c Config) (*s3.Client, *http.Transport, error) {
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 15 * time.Second, ExpectContinueTimeout: time.Second, IdleConnTimeout: 60 * time.Second, MaxIdleConns: 16, MaxIdleConnsPerHost: 8, MaxResponseHeaderBytes: 64 << 10}
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
	var roundTripper http.RoundTripper = transport
	if c.ResponseBytes > 0 || c.GetResponseBytes > 0 {
		roundTripper = boundedResponses{transport, c.ResponseBytes, c.GetResponseBytes}
	}
	options := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(c.Region), awsconfig.WithLogger(logging.Nop{}), awsconfig.WithLogConfigurationWarnings(false),
		awsconfig.WithHTTPClient(&http.Client{Transport: roundTripper, Timeout: c.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("S3 redirects are disabled") }}),
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
	// Ambient AWS endpoint overrides must not redirect one storage namespace to
	// another. Credentials can still use the standard AWS provider chain.
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

type boundedResponses struct {
	next                    http.RoundTripper
	responseBytes, getBytes int64
}

func (b boundedResponses) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := b.next.RoundTrip(request)
	if err == nil && response.Body != nil {
		limit := b.responseBytes
		if request.Method == http.MethodGet && response.StatusCode >= 200 && response.StatusCode < 300 {
			limit = b.getBytes
		}
		if limit > 0 {
			response.Body = &limitedBody{Reader: io.LimitReader(response.Body, limit), Closer: response.Body}
		}
	}
	return response, err
}

type limitedBody struct {
	io.Reader
	io.Closer
}
