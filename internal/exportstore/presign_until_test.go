package exportstore

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type advancedSigningTime struct{ by time.Duration }

func (s advancedSigningTime) PresignHTTP(ctx context.Context, credentials aws.Credentials, request *http.Request, payloadHash, service, region string, signingTime time.Time, options ...func(*v4.SignerOptions)) (string, http.Header, error) {
	return v4.NewSigner().PresignHTTP(ctx, credentials, request, payloadHash, service, region, signingTime.Add(s.by), options...)
}

func TestPresignUntilUsesActualSignatureDeadline(t *testing.T) {
	t.Parallel()
	store := testStore(t, &fakeS3{})
	deadline := time.Now().Add(30 * time.Second)
	signed, expires, err := store.PresignUntil(t.Context(), "job/events.ndjson", deadline, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(signed)
	date, err := time.Parse("20060102T150405Z", u.Query().Get("X-Amz-Date"))
	seconds, durationErr := strconv.Atoi(u.Query().Get("X-Amz-Expires"))
	if err != nil || durationErr != nil || !expires.Equal(date.Add(time.Duration(seconds)*time.Second)) || expires.After(deadline) {
		t.Fatal("actual signed expiry exceeds absolute authority", expires, deadline)
	}
	// Deterministically advance the SDK signing clock, as if credential resolution
	// took time after the caller computed its relative TTL. The real signer still
	// produces the URL; the store must reject it rather than return a stale bound.
	store.presigner = s3.NewPresignClient(store.client, func(o *s3.PresignOptions) {
		o.Presigner = advancedSigningTime{by: 5 * time.Second}
	})
	if signed, _, err := store.PresignUntil(t.Context(), "job/events.ndjson", deadline, time.Minute); !errors.Is(err, ErrExpired) || signed != "" {
		t.Fatal("signature outlived caller deadline", err)
	}
	if _, _, err := store.PresignUntil(t.Context(), "job/events.ndjson", time.Now().Add(-time.Second), time.Minute); !errors.Is(err, ErrExpired) {
		t.Fatal("expired authority produced a URL", err)
	}
}
