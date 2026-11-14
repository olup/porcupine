package porcupine

import (
	"context"
	"embed"
	"encoding/json"
	"net/http"
)

const nativeContractVersion = "1"

//go:embed contracts/*.schema.json
var nativeContracts embed.FS

type NativeInputContract struct {
	Version             string         `json:"version"`
	LiveEndpoint        string         `json:"live_endpoint"`
	LiveSchema          map[string]any `json:"live_schema"`
	HistoricalImport    bool           `json:"native_historical_import_available" doc:"Historical files use the resumable import endpoint with format=native; live capture is not a substitute."`
	HistoricalStatus    string         `json:"historical_schema_status"`
	HistoricalSchema    map[string]any `json:"historical_event_schema"`
	ServerManagedFields []string       `json:"server_managed_fields"`
	Documentation       string         `json:"documentation"`
}

type NativeInputContractOutput struct{ Body NativeInputContract }

func nativeContractSchema(name string) (map[string]any, error) {
	raw, err := nativeContracts.ReadFile("contracts/" + name)
	if err != nil {
		return nil, err
	}
	var schema map[string]any
	err = json.Unmarshal(raw, &schema)
	return schema, err
}

func (s *Service) getEventInputContract(_ context.Context, _ *ProjectIDInput) (*NativeInputContractOutput, error) {
	live, err := nativeContractSchema("native-message-v1.schema.json")
	if err != nil {
		return nil, apiError(500, "native input contract unavailable")
	}
	history, err := nativeContractSchema("native-history-message-v1.schema.json")
	if err != nil {
		return nil, apiError(500, "native history contract unavailable")
	}
	return &NativeInputContractOutput{Body: NativeInputContract{
		Version: nativeContractVersion, LiveEndpoint: "/ingest/v1/events", LiveSchema: live,
		HistoricalImport: true, HistoricalStatus: "stable", HistoricalSchema: history,
		ServerManagedFields: []string{"project_id", "source_id", "event_id", "uuid", "received_at", "revision", "is_imported"},
		Documentation:       "https://olup.github.io/porcupine/docs/native-event-contract.md",
	}}, nil
}

func serveNativeInputSchema(w http.ResponseWriter, _ *http.Request) {
	raw, err := nativeContracts.ReadFile("contracts/native-message-v1.schema.json")
	if err != nil {
		problem(w, 500, "native input schema unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/schema+json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	_, _ = w.Write(raw)
}
