package porcupine

// SharedConfig configures the hosted profile's PostgreSQL control database.
type SharedConfig struct {
	DatabaseURL string
}

type sharedRuntime struct {
	workerID string
}
