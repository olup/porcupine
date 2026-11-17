package delivery

import (
	"context"
	"io"
	"net/http"
	"strings"
)

type WebhookMessage struct {
	Endpoint, DeliveryID, Payload string
	Headers                       map[string]string
}

func SendWebhook(ctx context.Context, client *http.Client, message WebhookMessage) (int, string) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, message.Endpoint, strings.NewReader(message.Payload))
	if err != nil {
		return 0, "invalid_destination"
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "Porcupine-Webhooks/1")
	request.Header.Set("X-Porcupine-Delivery-ID", message.DeliveryID)
	request.Header.Set("Idempotency-Key", message.DeliveryID)
	for key, value := range message.Headers {
		request.Header.Set(key, value)
	}
	if client == nil {
		client = NewWebhookHTTPClient()
		defer client.CloseIdleConnections()
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, "request_failed"
	}
	defer response.Body.Close()
	n, err := io.Copy(io.Discard, io.LimitReader(response.Body, (64<<10)+1))
	if err != nil || n > 64<<10 {
		return response.StatusCode, "invalid_response"
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return response.StatusCode, "http_rejected"
	}
	return response.StatusCode, ""
}
