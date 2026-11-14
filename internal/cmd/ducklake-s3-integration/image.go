package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// runImage exercises only the supplied artifact. The server runs nonroot,
// read-only and without Internet access, so a missing extension or CA fails.
func runImage(ctx context.Context, image, expectedVersion string, credentialChain bool) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if expectedVersion != "" {
		version, err := exec.CommandContext(ctx, "docker", "run", "--rm", "--pull=never", "--network", "none", "--user", "65532:65532", image, "--version").Output()
		if err != nil || strings.TrimSpace(string(version)) != expectedVersion {
			return errors.New("supplied image version does not match the expected release")
		}
	}
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
	bootstrap, err := randomHex(32)
	if err != nil {
		return err
	}
	network := "porcupine-ducklake-offline-" + suffix
	storageName := "porcupine-ducklake-tls-" + suffix
	serverName := "porcupine-ducklake-image-" + suffix
	bucket := "porcupine-ducklake-test-" + suffix
	prefix := "image-tls/" + suffix
	directory, err := os.MkdirTemp("", "porcupine-s3-tls-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	if _, err := createFixtureCertificates(directory, storageName); err != nil {
		return err
	}
	architecture, err := exec.CommandContext(ctx, "docker", "image", "inspect", image, "--format", "{{.Architecture}}").Output()
	if err != nil {
		return errors.New("cannot inspect the supplied image architecture")
	}
	arch := strings.TrimSpace(string(architecture))
	if arch != "amd64" && arch != "arm64" {
		return errors.New("unsupported fixture image architecture")
	}
	probe := filepath.Join(directory, "probe")
	build := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", probe, "./internal/cmd/ducklake-s3-integration")
	build.Env = environment(map[string]string{"CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": arch})
	build.Stdout, build.Stderr = io.Discard, os.Stderr
	if err := build.Run(); err != nil {
		return errors.New("cannot build the isolated image probe")
	}
	if err := docker(ctx, nil, "network", "create", "--internal", "--label", "porcupine.integration=true", network); err != nil {
		return errors.New("cannot create the offline S3 image network")
	}
	storageCreated, serverCreated := false, false
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if serverCreated {
			docker(cleanup, nil, "rm", "-fv", serverName)
		}
		if storageCreated {
			docker(cleanup, nil, "rm", "-fv", storageName)
		}
		docker(cleanup, nil, "network", "rm", network)
	}()
	if err := docker(ctx, environment(map[string]string{"MINIO_ROOT_USER": access, "MINIO_ROOT_PASSWORD": secret}), "run", "-d", "--name", storageName, "--network", network,
		"--label", "porcupine.integration=true", "--memory", "512m", "--cpus", "1", "-e", "MINIO_ROOT_USER", "-e", "MINIO_ROOT_PASSWORD",
		"--mount", "type=bind,src="+directory+",dst=/certs,readonly", siloImage, "--certs-dir", "/certs", "server", "/data", "--address", ":9000"); err != nil {
		return errors.New("cannot start isolated TLS S3 fixture")
	}
	storageCreated = true
	variables := map[string]string{
		"PORCUPINE_BOOTSTRAP_TOKEN": bootstrap, "PORCUPINE_DUCKLAKE_MEMORY_MB": "256", "PORCUPINE_DUCKLAKE_THREADS": "1",
		"PORCUPINE_DUCKLAKE_S3_BUCKET": bucket, "PORCUPINE_DUCKLAKE_S3_PREFIX": prefix, "PORCUPINE_DUCKLAKE_S3_REGION": "us-east-1", "PORCUPINE_DUCKLAKE_S3_PATH_STYLE": "true",
		"PORCUPINE_DUCKLAKE_S3_ENDPOINT": "https://" + storageName + ":9000", "PORCUPINE_DUCKLAKE_S3_ACCESS_KEY_ID": access, "PORCUPINE_DUCKLAKE_S3_SECRET_ACCESS_KEY": secret,
		"PORCUPINE_DUCKLAKE_S3_CA_CERT_FILE": "/etc/porcupine-test-ca.pem",
	}
	if credentialChain {
		variables["PORCUPINE_DUCKLAKE_S3_ACCESS_KEY_ID"] = ""
		variables["PORCUPINE_DUCKLAKE_S3_SECRET_ACCESS_KEY"] = ""
		variables["AWS_ACCESS_KEY_ID"] = access
		variables["AWS_SECRET_ACCESS_KEY"] = secret
		variables["AWS_EC2_METADATA_DISABLED"] = "true"
		variables["AWS_REGION"] = "us-east-1"
	}
	env := environment(variables)

	arguments := []string{"run", "-d", "--pull=never", "--name", serverName, "--network", network, "--label", "porcupine.integration=true", "--user", "65532:65532", "--read-only", "--memory", "1g", "--cpus", "2",
		"--tmpfs", "/data:rw,nosuid,nodev,size=268435456,uid=65532,gid=65532,mode=0700", "--tmpfs", "/tmp:rw,nosuid,nodev,size=67108864",
		"--mount", "type=bind,src=" + filepath.Join(directory, "ca.pem") + ",dst=/etc/porcupine-test-ca.pem,readonly",
		"--mount", "type=bind,src=" + probe + ",dst=/porcupine-fixture-probe,readonly"}
	for name := range variables {
		arguments = append(arguments, "-e", name)
	}
	arguments = append(arguments, image)
	if err := docker(ctx, env, arguments...); err != nil {
		return errors.New("cannot start the supplied nonroot image")
	}
	serverCreated = true
	command := exec.CommandContext(ctx, "docker", "exec", "--user", "65532:65532", serverName, "/porcupine-fixture-probe", "-probe", "-expected-version", expectedVersion)
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("offline TLS image probe failed: %s", redactedContainerLogs(ctx, serverName, access, secret, bootstrap))
	}
	return nil
}

func runImageProbe(ctx context.Context, expectedVersion string) error {
	certificate, err := os.ReadFile(os.Getenv("PORCUPINE_DUCKLAKE_S3_CA_CERT_FILE"))
	if err != nil {
		return errors.New("nonroot probe cannot read mounted CA")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate) {
		return errors.New("invalid mounted fixture CA")
	}
	secure := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer secure.CloseIdleConnections()
	endpoint := os.Getenv("PORCUPINE_DUCKLAKE_S3_ENDPOINT")
	if err := pollImage(ctx, func() (bool, error) {
		request, _ := http.NewRequestWithContext(ctx, "GET", endpoint+"/minio/health/live", nil)
		response, err := secure.Do(request)
		if err != nil {
			return false, nil
		}
		response.Body.Close()
		return response.StatusCode == 200, nil
	}); err != nil {
		return errors.New("isolated TLS S3 fixture did not become ready")
	}
	access, secret := os.Getenv("PORCUPINE_DUCKLAKE_S3_ACCESS_KEY_ID"), os.Getenv("PORCUPINE_DUCKLAKE_S3_SECRET_ACCESS_KEY")
	if access == "" {
		access = os.Getenv("AWS_ACCESS_KEY_ID")
		secret = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	storage := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(endpoint), UsePathStyle: true,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(access, secret, "")), HTTPClient: secure, RetryMaxAttempts: 2})
	bucket, prefix := os.Getenv("PORCUPINE_DUCKLAKE_S3_BUCKET"), os.Getenv("PORCUPINE_DUCKLAKE_S3_PREFIX")
	if _, err := storage.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return errors.New("cannot create TLS image fixture bucket")
	}
	api := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer api.CloseIdleConnections()
	origin := "http://127.0.0.1:8080"
	if err := pollImage(ctx, func() (bool, error) {
		response, err := api.Get(origin + "/readyz")
		if err != nil {
			return false, nil
		}
		response.Body.Close()
		return response.StatusCode == 200, nil
	}); err != nil {
		return errors.New("supplied nonroot image did not become ready offline")
	}
	bootstrap := os.Getenv("PORCUPINE_BOOTSTRAP_TOKEN")
	if expectedVersion != "" {
		var status struct {
			Version string `json:"version"`
		}
		if err := imageCall(ctx, api, origin, "GET", "/api/v1/status", bootstrap, nil, 200, &status); err != nil {
			return err
		}
		if status.Version != expectedVersion {
			return errors.New("running image API version does not match expected release")
		}
	}
	return verifyImageAPI(ctx, api, origin, storage, bucket, prefix, bootstrap)
}

func verifyImageAPI(ctx context.Context, api *http.Client, origin string, storage *s3.Client, bucket, prefix, bootstrap string) error {
	type projectReply struct {
		Project struct {
			ID string `json:"id"`
		} `json:"project"`
		Ingest string `json:"ingest_token"`
	}
	var a, b projectReply
	for index, reply := range []*projectReply{&a, &b} {
		if err := imageCall(ctx, api, origin, "POST", "/api/v1/projects", bootstrap, map[string]any{"name": "tls-image-test"}, 201, reply); err != nil {
			return err
		}
		payload := map[string]any{"type": "track", "message_id": "image-event", "actor_id": "synthetic", "name": "TLS image event", "properties": map[string]any{"project": index}}
		var receipt struct {
			Receipts []struct {
				EventID string `json:"event_id"`
			} `json:"receipts"`
		}
		if err := imageCall(ctx, api, origin, "POST", "/ingest/v1/events", reply.Ingest, payload, 200, &receipt); err != nil {
			return err
		}
		if len(receipt.Receipts) != 1 || receipt.Receipts[0].EventID == "" {
			return errors.New("nonroot image did not durably accept its test event")
		}
	}
	for _, reply := range []projectReply{a, b} {
		if err := pollImage(ctx, func() (bool, error) {
			var result struct {
				Result struct {
					Data []map[string]any `json:"data"`
				} `json:"result"`
			}
			err := imageCall(ctx, api, origin, "POST", "/api/v1/projects/"+reply.Project.ID+"/query", bootstrap,
				map[string]any{"sql": "SELECT event_id,event FROM events", "use_cache": false}, 200, &result)
			return err == nil && len(result.Result.Data) == 1 && result.Result.Data[0]["event"] == "TLS image event", nil
		}); err != nil {
			return errors.New("nonroot image could not query its native TLS-backed events")
		}
	}
	projectHash := func(project string) string {
		digest := sha256.Sum256([]byte(project))
		return "/" + hex.EncodeToString(digest[:]) + "/"
	}
	list := func() ([]string, error) {
		page, err := storage.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix + "/")})
		if err != nil {
			return nil, errors.New("cannot inspect the TLS-backed analytical objects")
		}
		var keys []string
		for _, object := range page.Contents {
			keys = append(keys, aws.ToString(object.Key))
		}
		return keys, nil
	}
	if err := pollImage(ctx, func() (bool, error) {
		keys, err := list()
		if err != nil {
			return false, err
		}
		var foundA, foundB bool
		for _, key := range keys {
			foundA = foundA || strings.Contains(key, projectHash(a.Project.ID))
			foundB = foundB || strings.Contains(key, projectHash(b.Project.ID))
		}
		return foundA && foundB, nil
	}); err != nil {
		return errors.New("nonroot image did not flush separate project data to TLS S3")
	}
	// Force a read after Parquet flush, proving HTTPS GET after engine locking.
	var queried any
	if err := imageCall(ctx, api, origin, "POST", "/api/v1/projects/"+a.Project.ID+"/query", bootstrap,
		map[string]any{"sql": "SELECT * FROM events", "use_cache": false}, 200, &queried); err != nil {
		return errors.New("nonroot TLS S3 query failed after native flush")
	}
	if err := pollImage(ctx, func() (bool, error) {
		err := imageCall(ctx, api, origin, "DELETE", "/api/v1/projects/"+a.Project.ID, bootstrap, nil, 204, nil)
		if err == nil {
			return true, nil
		}
		var status *imageStatusError
		if errors.As(err, &status) && status.status == 503 {
			return false, nil
		}
		return false, err
	}); err != nil {
		return err
	}
	keys, err := list()
	if err != nil {
		return err
	}
	foundB := false
	for _, key := range keys {
		if strings.Contains(key, projectHash(a.Project.ID)) {
			return errors.New("nonroot image retained deleted project TLS S3 objects")
		}
		foundB = foundB || strings.Contains(key, projectHash(b.Project.ID))
	}
	if !foundB {
		return errors.New("nonroot image removed the other project's TLS S3 data")
	}
	fmt.Println("ok: supplied image runs nonroot/read-only/offline and queries/deletes isolated TLS S3 data")
	return nil
}

func docker(ctx context.Context, environment []string, arguments ...string) error {
	command := exec.CommandContext(ctx, "docker", arguments...)
	command.Env, command.Stdout, command.Stderr = environment, io.Discard, os.Stderr
	return command.Run()
}

func pollImage(ctx context.Context, check func() (bool, error)) error {
	deadline := time.Now().Add(45 * time.Second)
	for {
		ok, err := check()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("image verification timed out")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func imageCall(ctx context.Context, client *http.Client, origin, method, route, token string, body any, expected int, out any) error {
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, origin+route, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		if strings.HasPrefix(route, "/ingest/") {
			request.Header.Set("X-Porcupine-Key", token)
		} else {
			request.Header.Set("Authorization", "Bearer "+token)
		}
	}
	response, err := client.Do(request)
	if err != nil {
		return errors.New("isolated image API request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != expected {
		return &imageStatusError{method: method, route: route, status: response.StatusCode}
	}
	if out != nil && json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(out) != nil {
		return errors.New("isolated image API returned invalid JSON")
	}
	return nil
}

func createFixtureCertificates(directory, hostname string) ([]byte, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	start := time.Now().Add(-time.Hour)
	root := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Porcupine isolated test CA"}, NotBefore: start, NotAfter: start.Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	rootDER, err := x509.CreateCertificate(rand.Reader, root, root, caKey.Public(), caKey)
	if err != nil {
		return nil, err
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: hostname}, NotBefore: start, NotAfter: root.NotAfter,
		DNSNames: []string{hostname, "localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, root, serverKey.Public(), caKey)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		return nil, err
	}
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootDER})
	for name, data := range map[string][]byte{
		"ca.pem":      ca,
		"public.crt":  append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), ca...),
		"private.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	} {
		mode := os.FileMode(0600)
		if name == "ca.pem" {
			mode = 0644 // Public trust anchor must be readable by the nonroot image.
		}
		if err := os.WriteFile(path.Join(directory, name), data, mode); err != nil {
			return nil, err
		}
	}
	return ca, nil
}

func redactedContainerLogs(ctx context.Context, name string, secrets ...string) string {
	data, _ := exec.CommandContext(ctx, "docker", "logs", "--tail", "12", name).CombinedOutput()
	text := string(data)
	for _, secret := range secrets {
		if secret != "" {
			text = strings.ReplaceAll(text, secret, "[redacted]")
		}
	}
	if len(text) > 4096 {
		text = text[:4096]
	}
	return text
}

type imageStatusError struct {
	method, route string
	status        int
}

func (e *imageStatusError) Error() string {
	return fmt.Sprintf("isolated image API %s %s returned status %d", e.method, e.route, e.status)
}
