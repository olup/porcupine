package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
)

// FileConfig is the operator configuration. Environment tags are also the
// allowlist, so a newly supported setting cannot be forgotten by validation.
type FileConfig struct {
	Profile    string `json:"profile,omitempty" env:"PORCUPINE_PROFILE"`
	Acceptance struct {
		Mode string `json:"mode,omitempty" env:"PORCUPINE_ACCEPTANCE_MODE"`
	} `json:"acceptance"`
	Listen         string   `json:"listen,omitempty" env:"PORCUPINE_ADDR"`
	BootstrapToken string   `json:"bootstrap_token,omitempty" env:"PORCUPINE_BOOTSTRAP_TOKEN" secret:"true"`
	AllowedOrigins []string `json:"allowed_origins,omitempty" env:"PORCUPINE_ALLOWED_ORIGINS"`
	TrustedProxies []string `json:"trusted_proxies,omitempty" env:"PORCUPINE_TRUSTED_PROXIES"`
	Storage        struct {
		Journal  string `json:"journal,omitempty" env:"PORCUPINE_DB"`
		Control  string `json:"control,omitempty" env:"PORCUPINE_CONTROL_DB"`
		DuckLake string `json:"ducklake,omitempty" env:"PORCUPINE_DUCKLAKE_DIR"`
	} `json:"storage"`
	Queue struct {
		MaxEvents        *int   `json:"max_events,omitempty" env:"PORCUPINE_QUEUE_MAX_EVENTS"`
		MaxBytes         *int64 `json:"max_bytes,omitempty" env:"PORCUPINE_QUEUE_MAX_BYTES"`
		ReceiptRetention string `json:"receipt_retention,omitempty" env:"PORCUPINE_RECEIPT_RETENTION"`
	} `json:"queue"`
	Query struct {
		MaxConcurrent  *int `json:"max_concurrent,omitempty" env:"PORCUPINE_QUERY_MAX_CONCURRENT"`
		TimeoutMS      *int `json:"timeout_ms,omitempty" env:"PORCUPINE_QUERY_TIMEOUT_MS"`
		MaxRows        *int `json:"max_rows,omitempty" env:"PORCUPINE_QUERY_MAX_ROWS"`
		MaxResultBytes *int `json:"max_result_bytes,omitempty" env:"PORCUPINE_QUERY_MAX_RESULT_BYTES"`
	} `json:"query"`
	Usage struct {
		RetentionDays    *int   `json:"retention_days,omitempty" env:"PORCUPINE_USAGE_RETENTION_DAYS"`
		MaxSnapshots     *int   `json:"max_snapshots,omitempty" env:"PORCUPINE_USAGE_MAX_SNAPSHOTS"`
		MaxSnapshotBytes *int64 `json:"max_snapshot_bytes,omitempty" env:"PORCUPINE_USAGE_MAX_SNAPSHOT_BYTES"`
	} `json:"usage"`
	DuckLake struct {
		MemoryMB               *int         `json:"memory_mb,omitempty" env:"PORCUPINE_DUCKLAKE_MEMORY_MB"`
		Threads                *int         `json:"threads,omitempty" env:"PORCUPINE_DUCKLAKE_THREADS"`
		MaxOpenProjects        *int         `json:"max_open_projects,omitempty" env:"PORCUPINE_DUCKLAKE_MAX_OPEN_PROJECTS"`
		MetadataTimeoutSeconds *int         `json:"metadata_timeout_seconds,omitempty" env:"PORCUPINE_DUCKLAKE_METADATA_TIMEOUT_SECONDS"`
		PostgresURL            string       `json:"postgres_url,omitempty" env:"PORCUPINE_DUCKLAKE_POSTGRES_URL" secret:"true"`
		PostgresURLFile        string       `json:"postgres_url_file,omitempty" env:"PORCUPINE_DUCKLAKE_POSTGRES_URL_FILE" secret:"true"`
		MaintenanceInterval    string       `json:"maintenance_interval,omitempty" env:"PORCUPINE_DUCKLAKE_MAINTENANCE_INTERVAL"`
		CleanupEnabled         *bool        `json:"cleanup_enabled,omitempty" env:"PORCUPINE_DUCKLAKE_CLEANUP_ENABLED"`
		SnapshotRetention      string       `json:"snapshot_retention,omitempty" env:"PORCUPINE_DUCKLAKE_SNAPSHOT_RETENTION"`
		CleanupGrace           string       `json:"cleanup_grace,omitempty" env:"PORCUPINE_DUCKLAKE_CLEANUP_GRACE"`
		CleanupInterval        string       `json:"cleanup_interval,omitempty" env:"PORCUPINE_DUCKLAKE_CLEANUP_INTERVAL"`
		RestoreWindow          string       `json:"restore_window,omitempty" env:"PORCUPINE_DUCKLAKE_RESTORE_WINDOW"`
		RestoreMaxReaderTime   string       `json:"restore_max_reader_time,omitempty" env:"PORCUPINE_DUCKLAKE_RESTORE_MAX_READER_TIME"`
		RestoreSafetyMargin    string       `json:"restore_safety_margin,omitempty" env:"PORCUPINE_DUCKLAKE_RESTORE_SAFETY_MARGIN"`
		S3                     S3FileConfig `json:"s3" envprefix:"PORCUPINE_DUCKLAKE_S3_"`
	} `json:"ducklake"`
	OAuth struct {
		Issuer                   string `json:"issuer,omitempty" env:"PORCUPINE_OAUTH_ISSUER"`
		TokenLifetimeSeconds     *int   `json:"token_lifetime_seconds,omitempty" env:"PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS"`
		AllowLegacyKeys          *bool  `json:"allow_legacy_keys,omitempty" env:"PORCUPINE_OAUTH_ALLOW_LEGACY_KEYS"`
		RestoreIdentityBootstrap *bool  `json:"restore_identity_bootstrap,omitempty" env:"PORCUPINE_RESTORE_IDENTITY_BOOTSTRAP"`
	} `json:"oauth"`
	Auth struct {
		Mode      string `json:"mode,omitempty" env:"PORCUPINE_AUTH_MODE"`
		PublicURL string `json:"public_url,omitempty" env:"PORCUPINE_AUTH_PUBLIC_URL"`
		External  struct {
			Issuer           string   `json:"issuer,omitempty" env:"PORCUPINE_AUTH_EXTERNAL_ISSUER"`
			JWKSURL          string   `json:"jwks_url,omitempty" env:"PORCUPINE_AUTH_EXTERNAL_JWKS_URL"`
			APIAudience      string   `json:"api_audience,omitempty" env:"PORCUPINE_AUTH_EXTERNAL_API_AUDIENCE"`
			MCPAudience      string   `json:"mcp_audience,omitempty" env:"PORCUPINE_AUTH_EXTERNAL_MCP_AUDIENCE"`
			Algorithms       []string `json:"algorithms,omitempty" env:"PORCUPINE_AUTH_EXTERNAL_ALGORITHMS"`
			TokenTypes       []string `json:"token_types,omitempty" env:"PORCUPINE_AUTH_EXTERNAL_TOKEN_TYPES"`
			ClockSkew        string   `json:"clock_skew,omitempty" env:"PORCUPINE_AUTH_EXTERNAL_CLOCK_SKEW"`
			MaxTokenLifetime string   `json:"max_token_lifetime,omitempty" env:"PORCUPINE_AUTH_EXTERNAL_MAX_TOKEN_LIFETIME"`
		} `json:"external"`
	} `json:"auth"`
	OIDC struct {
		Issuer           string `json:"issuer,omitempty" env:"PORCUPINE_OIDC_ISSUER"`
		ClientID         string `json:"client_id,omitempty" env:"PORCUPINE_OIDC_CLIENT_ID"`
		ClientSecret     string `json:"client_secret,omitempty" env:"PORCUPINE_OIDC_CLIENT_SECRET" secret:"true"`
		ClientSecretFile string `json:"client_secret_file,omitempty" env:"PORCUPINE_OIDC_CLIENT_SECRET_FILE" secret:"true"`
	} `json:"oidc"`
	Email struct {
		AdminEmail   string `json:"admin_email,omitempty" env:"PORCUPINE_ADMIN_EMAIL"`
		Host         string `json:"host,omitempty" env:"PORCUPINE_SMTP_HOST"`
		Port         *int   `json:"port,omitempty" env:"PORCUPINE_SMTP_PORT"`
		TLS          string `json:"tls,omitempty" env:"PORCUPINE_SMTP_TLS"`
		From         string `json:"from,omitempty" env:"PORCUPINE_SMTP_FROM"`
		Username     string `json:"username,omitempty" env:"PORCUPINE_SMTP_USERNAME" secret:"true"`
		PasswordFile string `json:"password_file,omitempty" env:"PORCUPINE_SMTP_PASSWORD_FILE" secret:"true"`
	} `json:"email"`
	Pipeline struct {
		Key         string   `json:"key,omitempty" env:"PORCUPINE_PIPELINE_KEY" secret:"true"`
		KeyFile     string   `json:"key_file,omitempty" env:"PORCUPINE_PIPELINE_KEY_FILE" secret:"true"`
		S3Endpoints []string `json:"s3_endpoints,omitempty" env:"PORCUPINE_PIPELINE_S3_ENDPOINTS"`
	} `json:"pipeline"`
	Export ExportFileConfig `json:"export"`
	Backup struct {
		Directory string       `json:"directory,omitempty" env:"PORCUPINE_BACKUP_DIR"`
		Interval  string       `json:"interval,omitempty" env:"PORCUPINE_BACKUP_INTERVAL"`
		Timeout   string       `json:"timeout,omitempty" env:"PORCUPINE_BACKUP_TIMEOUT"`
		Retain    *int         `json:"retain,omitempty" env:"PORCUPINE_BACKUP_RETAIN"`
		S3        S3FileConfig `json:"s3" envprefix:"PORCUPINE_BACKUP_S3_"`
	} `json:"backup"`
	Shared struct {
		ControlDatabaseURL     string `json:"control_database_url,omitempty" env:"PORCUPINE_CONTROL_DATABASE_URL" secret:"true"`
		ControlDatabaseURLFile string `json:"control_database_url_file,omitempty" env:"PORCUPINE_CONTROL_DATABASE_URL_FILE" secret:"true"`
		WorkerID               string `json:"worker_id,omitempty" env:"PORCUPINE_WORKER_ID"`
		WorkerURL              string `json:"worker_url,omitempty" env:"PORCUPINE_WORKER_URL"`
		RoutingSecret          string `json:"routing_secret,omitempty" env:"PORCUPINE_SHARED_ROUTING_SECRET" secret:"true"`
		RoutingSecretFile      string `json:"routing_secret_file,omitempty" env:"PORCUPINE_SHARED_ROUTING_SECRET_FILE" secret:"true"`
	} `json:"shared"`
}

type S3FileConfig struct {
	Bucket                string `json:"bucket,omitempty" suffix:"BUCKET"`
	Prefix                string `json:"prefix,omitempty" suffix:"PREFIX"`
	Region                string `json:"region,omitempty" suffix:"REGION"`
	Endpoint              string `json:"endpoint,omitempty" suffix:"ENDPOINT"`
	AccessKeyID           string `json:"access_key_id,omitempty" suffix:"ACCESS_KEY_ID" secret:"true"`
	SecretAccessKey       string `json:"secret_access_key,omitempty" suffix:"SECRET_ACCESS_KEY" secret:"true"`
	SessionToken          string `json:"session_token,omitempty" suffix:"SESSION_TOKEN" secret:"true"`
	PathStyle             *bool  `json:"path_style,omitempty" suffix:"PATH_STYLE"`
	CACertFile            string `json:"ca_cert_file,omitempty" suffix:"CA_CERT_FILE"`
	RequestTimeoutSeconds *int   `json:"request_timeout_seconds,omitempty" suffix:"REQUEST_TIMEOUT_SECONDS"`
}

type ExportS3FileConfig struct {
	Bucket          string `json:"bucket,omitempty" suffix:"BUCKET"`
	Prefix          string `json:"prefix,omitempty" suffix:"PREFIX"`
	Region          string `json:"region,omitempty" suffix:"REGION"`
	Endpoint        string `json:"endpoint,omitempty" suffix:"ENDPOINT"`
	AccessKeyID     string `json:"access_key_id,omitempty" suffix:"ACCESS_KEY_ID" secret:"true"`
	SecretAccessKey string `json:"secret_access_key,omitempty" suffix:"SECRET_ACCESS_KEY" secret:"true"`
	SessionToken    string `json:"session_token,omitempty" suffix:"SESSION_TOKEN" secret:"true"`
	PathStyle       *bool  `json:"path_style,omitempty" suffix:"PATH_STYLE"`
}

type ExportFileConfig struct {
	S3                      ExportS3FileConfig `json:"s3" envprefix:"PORCUPINE_EXPORT_S3_"`
	MaxExecutionSeconds     *int64             `json:"max_execution_seconds,omitempty" env:"PORCUPINE_EXPORT_MAX_EXECUTION_SECONDS"`
	MaxOutputBytes          *int64             `json:"max_output_bytes,omitempty" env:"PORCUPINE_EXPORT_MAX_OUTPUT_BYTES"`
	FileBytes               *int64             `json:"file_bytes,omitempty" env:"PORCUPINE_EXPORT_FILE_BYTES"`
	MaxRowBytes             *int64             `json:"max_row_bytes,omitempty" env:"PORCUPINE_EXPORT_MAX_ROW_BYTES"`
	BandwidthBytesPerSecond *int64             `json:"bandwidth_bytes_per_second,omitempty" env:"PORCUPINE_EXPORT_BANDWIDTH_BYTES_PER_SECOND"`
	QueueThreshold          *int64             `json:"queue_threshold,omitempty" env:"PORCUPINE_EXPORT_QUEUE_THRESHOLD"`
	MaxQueuedJobs           *int64             `json:"max_queued_jobs,omitempty" env:"PORCUPINE_EXPORT_MAX_QUEUED_JOBS"`
	RetentionHours          *int64             `json:"retention_hours,omitempty" env:"PORCUPINE_EXPORT_RETENTION_HOURS"`
}

type settings map[string]string

func (s settings) get(name string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return s[name]
}

func configFields(v reflect.Value, prefix string, visit func(string, reflect.Value, bool)) {
	for i := 0; i < v.NumField(); i++ {
		f, value := v.Type().Field(i), v.Field(i)
		if value.Kind() == reflect.Struct {
			configFields(value, f.Tag.Get("envprefix"), visit)
			continue
		}
		name := f.Tag.Get("env")
		if suffix := f.Tag.Get("suffix"); suffix != "" {
			name = prefix + suffix
		}
		visit(name, value, f.Tag.Get("secret") == "true")
	}
}

func fileSettings(c FileConfig) settings {
	result := settings{}
	configFields(reflect.ValueOf(c), "", func(name string, v reflect.Value, _ bool) {
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return
			}
			v = v.Elem()
		} else if v.IsZero() {
			return
		}
		switch v.Kind() {
		case reflect.String:
			result[name] = v.String()
		case reflect.Int, reflect.Int64:
			result[name] = strconv.FormatInt(v.Int(), 10)
		case reflect.Bool:
			result[name] = strconv.FormatBool(v.Bool())
		case reflect.Slice:
			result[name] = strings.Join(v.Interface().([]string), ",")
		}
	})
	return result
}

func validateEnvironment() error {
	known := map[string]bool{"PORCUPINE_CONFIG": true}
	configFields(reflect.ValueOf(FileConfig{}), "", func(name string, _ reflect.Value, _ bool) { known[name] = true })
	for _, item := range os.Environ() {
		name, _, _ := strings.Cut(item, "=")
		if strings.HasPrefix(name, "PORCUPINE_") && !known[name] {
			return fmt.Errorf("unknown environment variable %s", name)
		}
	}
	return nil
}

func loadSettings(path string, explicit bool) (settings, error) {
	if err := validateEnvironment(); err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) && !explicit {
		return settings{}, nil
	}
	if err != nil {
		return nil, errors.New("cannot open configuration file")
	}
	defer f.Close()
	return readSettings(f)
}

func readSettings(input io.Reader) (settings, error) {
	body, err := io.ReadAll(io.LimitReader(input, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return nil, errors.New("configuration file must be at most 1 MiB")
	}
	if body = bytes.TrimSpace(body); len(body) == 0 || body[0] != '{' {
		return nil, errors.New("configuration must be a JSON object")
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var config FileConfig
	if err := dec.Decode(&config); err != nil {
		var syntax *json.SyntaxError
		if errors.As(err, &syntax) {
			return nil, fmt.Errorf("invalid configuration JSON at byte %d", syntax.Offset)
		}
		var valueType *json.UnmarshalTypeError
		if errors.As(err, &valueType) {
			return nil, fmt.Errorf("invalid configuration type for %s at byte %d", valueType.Field, valueType.Offset)
		}
		// Other decoder errors can quote secret input. Do not return their text.
		return nil, errors.New("invalid configuration: use the documented JSON keys and value types")
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		return nil, errors.New("configuration must contain exactly one JSON object")
	}
	return fileSettings(config), nil
}

func (s settings) redacted() map[string]string {
	result := map[string]string{}
	configFields(reflect.ValueOf(FileConfig{}), "", func(name string, _ reflect.Value, secret bool) {
		if value := s.get(name); value != "" {
			if secret || strings.Contains(value, "://") && (strings.Contains(value, "@") || strings.Contains(value, "?")) {
				value = "[redacted]"
			}
			result[name] = value
		}
	})
	return result
}
