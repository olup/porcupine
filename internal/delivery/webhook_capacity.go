package delivery

import "errors"

var ErrWebhookCapacity = errors.New("webhook queue is full")
