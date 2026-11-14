package porcupine

import (
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/olup/porcupine/internal/exportstore"
)

// DuckLakeS3Config is operator-owned analytical storage, independent of result
// exports and pipeline destinations. Credentials never appear in API schemas.
type DuckLakeS3Config struct {
	Bucket                string
	Prefix                string
	Region                string
	Endpoint              string
	UsePathStyle          bool
	RequestTimeoutSeconds int
	CACertFile            string
	AccessKeyID           string `json:"-"`
	SecretAccessKey       string `json:"-"`
	SessionToken          string `json:"-"`
}

func (c DuckLakeS3Config) String() string {
	return fmt.Sprintf("DuckLakeS3{bucket:%q,prefix:%q,region:%q,endpoint:%q,credentials:redacted}", c.Bucket, c.Prefix, c.Region, c.Endpoint)
}

func (c DuckLakeS3Config) GoString() string { return c.String() }

var lakeBucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
var lakeRegionPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)
var lakePrefixSegmentPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

func normalizeDuckLakeS3(config *DuckLakeS3Config) (*DuckLakeS3Config, error) {
	if config == nil {
		return nil, nil
	}
	c := *config
	if c.RequestTimeoutSeconds == 0 {
		c.RequestTimeoutSeconds = 5
	}
	if c.RequestTimeoutSeconds < 1 || c.RequestTimeoutSeconds > 30 {
		return nil, errors.New("analytical S3 request timeout must be between 1 and 30 seconds")
	}
	c.Prefix = strings.TrimSuffix(c.Prefix, "/")
	if !lakeBucketPattern.MatchString(c.Bucket) || strings.Contains(c.Bucket, "..") || net.ParseIP(c.Bucket) != nil || !lakeRegionPattern.MatchString(c.Region) || c.Prefix == "" || len(c.Prefix) > 512 {
		return nil, errors.New("analytical S3 requires a valid bucket, prefix and region")
	}
	for _, part := range strings.Split(c.Prefix, "/") {
		if !lakePrefixSegmentPattern.MatchString(part) || part == "." || part == ".." {
			return nil, errors.New("invalid analytical S3 prefix")
		}
	}
	if (c.AccessKeyID == "") != (c.SecretAccessKey == "") || c.SessionToken != "" && c.AccessKeyID == "" {
		return nil, errors.New("analytical S3 credentials must include both access key ID and secret")
	}
	if c.Endpoint != "" {
		if !exportstore.ValidEndpoint(c.Endpoint) {
			return nil, errors.New("invalid analytical S3 endpoint")
		}
		endpoint, err := url.Parse(c.Endpoint)
		if err != nil {
			return nil, errors.New("invalid analytical S3 endpoint")
		}
		endpoint.Path = ""
		c.Endpoint = endpoint.String()
	}
	if c.CACertFile != "" {
		path, err := filepath.Abs(c.CACertFile)
		if err != nil {
			return nil, errors.New("invalid analytical S3 CA certificate file")
		}
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			return nil, errors.New("analytical S3 CA certificate file is unavailable")
		}
		pem, err := os.ReadFile(path)
		if err != nil || !x509.NewCertPool().AppendCertsFromPEM(pem) {
			return nil, errors.New("analytical S3 CA certificate file is unavailable")
		}
		c.CACertFile = path
	}
	return &c, nil
}

func (c DuckLakeS3Config) tlsEnabled() bool { return !strings.HasPrefix(c.Endpoint, "http://") }

func (c DuckLakeS3Config) secretEndpoint() string {
	if c.Endpoint != "" {
		u, _ := url.Parse(c.Endpoint)
		return u.Host
	}
	suffix := "amazonaws.com"
	if strings.HasPrefix(c.Region, "cn-") {
		suffix = "amazonaws.com.cn"
	}
	return "s3." + c.Region + "." + suffix
}
