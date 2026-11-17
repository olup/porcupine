package delivery

import (
	"bytes"
	"errors"
)

// WebhookBatchPrefix shares the exact batch envelope with journal and hosted
// delivery. Encoded events and the closing ]} follow this prefix.
func WebhookBatchPrefix(id, pipeline, project string, start, end int64) ([]byte, error) {
	base, err := EncodeJSON(WebhookPayload{DeliveryID: id, PipelineID: pipeline, ProjectID: project, WindowStartMS: &start, WindowEndMS: &end})
	if err != nil {
		return nil, err
	}
	return append(base[:len(base)-1], []byte(`,"events":[`)...), nil
}

// WebhookBatchCount chooses one chunk using encoded lengths before any event
// bodies are loaded. An oversized first event may stand alone up to maxBytes.
func WebhookBatchCount(prefixBytes int, lengths []int64, maxBytes int64) (int, error) {
	if prefixBytes <= 0 || maxBytes <= 0 || maxBytes < int64(prefixBytes)+2 || len(lengths) == 0 {
		return 0, errors.New("invalid webhook chunk bounds")
	}
	size := int64(prefixBytes) + 2
	count := 0
	for _, length := range lengths {
		if count == WebhookChunkEvents {
			break
		}
		if length <= 0 || length > maxBytes {
			return 0, errors.New("invalid webhook event length")
		}
		delta := length
		if count > 0 {
			delta++
		}
		if count > 0 && delta > min(int64(WebhookChunkBytes), maxBytes)-size {
			break
		}
		if delta > maxBytes-size {
			return 0, errors.New("webhook singleton exceeds body limit")
		}
		size += delta
		count++
	}
	return count, nil
}

// EncodeWebhookBatch preserves the canonical event bytes selected by the
// assembler. The caller validates each event against its frozen fragment.
func EncodeWebhookBatch(prefix []byte, events [][]byte, maxBytes int64) ([]byte, error) {
	lengths := make([]int64, len(events))
	for i, event := range events {
		lengths[i] = int64(len(event))
	}
	count, err := WebhookBatchCount(len(prefix), lengths, maxBytes)
	if err != nil {
		return nil, err
	}
	if count != len(events) {
		return nil, errors.New("webhook events exceed one chunk")
	}
	var body bytes.Buffer
	body.Write(prefix)
	for i, event := range events {
		if i > 0 {
			body.WriteByte(',')
		}
		body.Write(event)
	}
	body.WriteString("]}")
	return body.Bytes(), nil
}
