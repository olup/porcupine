// ducklake-s3-integration runs native analytical storage tests on an isolated,
// loopback-only S3-compatible server. It never reads AWS credentials.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const siloImage = "pgsty/silo:RELEASE.2026-10-26T00-00-00Z@sha256:7bb2aff6b89efe1cb1303c10fc21d579447434ed06649ac9ec05a5e2dbf30ad7"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	image := flag.String("image", "", "Test this existing server image as nonroot with TLS on an offline Docker network")
	credentialChain := flag.Bool("credential-chain", false, "Use isolated AWS environment credentials in image mode")
	expectedVersion := flag.String("expected-version", "", "Require this server version in image mode")
	useTLS := flag.Bool("tls", false, "Use a private TLS S3 fixture for the native test suite")
	probe := flag.Bool("probe", false, "Run the isolated internal image probe")
	flag.Parse()
	execute := func() error {
		if *probe {
			return runImageProbe(ctx, *expectedVersion)
		}
		if *image != "" {
			return runImage(ctx, *image, *expectedVersion, *credentialChain)
		}
		return run(ctx, *useTLS)
	}
	if err := execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *image == "" && !*probe {
		fmt.Println("ok: native DuckLake S3 ingestion, identity, mutations, isolation, cleanup and restart")
	}
}

func run(ctx context.Context, useTLS bool) error {
	suffix, err := randomHex(8)
	if err != nil {
		return err
	}
	access, err := randomHex(16)
	if err != nil {
		return err
	}
	secret, err := randomHex(32)
	if err != nil {
		return err
	}
	container := "porcupine-ducklake-s3-" + suffix
	bucket := "porcupine-ducklake-test-" + suffix
	fixtureEnv := environment(map[string]string{"MINIO_ROOT_USER": access, "MINIO_ROOT_PASSWORD": secret})
	scheme := "http"
	certificates := ""
	var fixtureCA []byte
	arguments := []string{"run", "-d", "--name", container, "--label", "porcupine.integration=true", "--memory", "512m", "--cpus", "1", "-p", "127.0.0.1::9000", "-e", "MINIO_ROOT_USER", "-e", "MINIO_ROOT_PASSWORD"}
	if useTLS {
		directory, err := os.MkdirTemp("", "porcupine-native-s3-tls-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(directory)
		certificates = directory
		fixtureCA, err = createFixtureCertificates(directory, container)
		if err != nil {
			return err
		}
		scheme = "https"
		arguments = append(arguments, "--mount", "type=bind,src="+directory+",dst=/certs,readonly", siloImage, "--certs-dir", "/certs", "server", "/data", "--address", ":9000")
	} else {
		arguments = append(arguments, siloImage, "server", "/data", "--address", ":9000")
	}
	start := exec.CommandContext(ctx, "docker", arguments...)
	start.Env, start.Stdout, start.Stderr = fixtureEnv, io.Discard, os.Stderr
	if err := start.Run(); err != nil {
		return errors.New("cannot start the isolated DuckLake S3 fixture")
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		command := exec.CommandContext(cleanup, "docker", "rm", "-fv", container)
		command.Stdout, command.Stderr = io.Discard, os.Stderr
		command.Run()
	}()
	port, err := exec.CommandContext(ctx, "docker", "port", container, "9000/tcp").Output()
	if err != nil {
		return errors.New("cannot inspect the isolated S3 fixture")
	}
	binding := strings.TrimSpace(string(port))
	if !strings.HasPrefix(binding, "127.0.0.1:") || strings.Contains(binding, "\n") {
		return errors.New("S3 fixture is not bound exclusively to loopback")
	}
	endpoint := scheme + "://" + binding
	transport := &http.Transport{}
	if useTLS {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(fixtureCA) {
			return errors.New("invalid private fixture CA")
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	}
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	deadline := time.Now().Add(30 * time.Second)
	for {
		request, err := http.NewRequestWithContext(ctx, "GET", endpoint+"/minio/health/live", nil)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err == nil {
			response.Body.Close()
			if response.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			return errors.New("isolated S3 fixture did not become ready")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	storage := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(endpoint), UsePathStyle: true,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(access, secret, "")), HTTPClient: client, RetryMaxAttempts: 2})
	if _, err := storage.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return errors.New("cannot create the isolated S3 test bucket")
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		clearBucket(cleanup, storage, bucket)
	}()
	awsConfigDirectory, err := os.MkdirTemp("", "porcupine-isolated-aws-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(awsConfigDirectory)
	awsConfigFile := filepath.Join(awsConfigDirectory, "config")
	if err := os.WriteFile(awsConfigFile, nil, 0600); err != nil {
		return err
	}
	tests := exec.CommandContext(ctx, "go", "test", "-race", "-tags", "ducklake", "./internal/porcupine", "-run", "^TestDuckLakeS3", "-count=1", "-timeout=180s", "-v")
	tests.Env = environment(map[string]string{
		"PORCUPINE_TEST_DUCKLAKE_S3_ENDPOINT":          endpoint,
		"PORCUPINE_TEST_DUCKLAKE_S3_BUCKET":            bucket,
		"PORCUPINE_TEST_DUCKLAKE_S3_PREFIX":            "native-tests/" + suffix,
		"PORCUPINE_TEST_DUCKLAKE_S3_REGION":            "us-east-1",
		"PORCUPINE_TEST_DUCKLAKE_S3_ACCESS_KEY_ID":     access,
		"PORCUPINE_TEST_DUCKLAKE_S3_SECRET_ACCESS_KEY": secret,
		"AWS_EC2_METADATA_DISABLED":                    "true",
		"AWS_ACCESS_KEY_ID":                            access, "AWS_SECRET_ACCESS_KEY": secret, "AWS_SESSION_TOKEN": "", "AWS_REGION": "us-east-1", "AWS_DEFAULT_REGION": "us-east-1",
		"AWS_CONFIG_FILE": awsConfigFile, "AWS_SHARED_CREDENTIALS_FILE": awsConfigFile, "AWS_PROFILE": "", "AWS_ROLE_ARN": "", "AWS_WEB_IDENTITY_TOKEN_FILE": "",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI": "", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "", "AWS_ENDPOINT_URL": "", "AWS_ENDPOINT_URL_S3": "",

		"PORCUPINE_TEST_DUCKLAKE_S3_CA_CERT_FILE": func() string {
			if useTLS {
				return filepath.Join(certificates, "ca.pem")
			}
			return ""
		}(),
	})
	tests.Stdout, tests.Stderr = os.Stdout, os.Stderr
	if err := tests.Run(); err != nil {
		return errors.New("native DuckLake S3 integration tests failed")
	}
	return nil
}

func randomHex(n int) (string, error) {
	data := make([]byte, n)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return hex.EncodeToString(data), nil
}

func environment(values map[string]string) []string {
	result := make([]string, 0, len(os.Environ())+len(values))
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if _, replace := values[name]; !replace {
			result = append(result, entry)
		}
	}
	for name, value := range values {
		result = append(result, name+"="+value)
	}
	return result
}

func clearBucket(ctx context.Context, client *s3.Client, bucket string) {
	for {
		objects, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), MaxKeys: aws.Int32(1000)})
		if err != nil || len(objects.Contents) == 0 {
			break
		}
		keys := make([]types.ObjectIdentifier, 0, len(objects.Contents))
		for _, object := range objects.Contents {
			keys = append(keys, types.ObjectIdentifier{Key: object.Key})
		}
		if _, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Objects: keys, Quiet: aws.Bool(true)}}); err != nil {
			return
		}
	}
	client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
}
