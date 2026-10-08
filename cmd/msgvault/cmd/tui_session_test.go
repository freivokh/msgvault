package cmd

import (
	"context"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/daemonclient"
)

func TestTUISessionUsesSelectedDaemonAfterCancellation(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		var body map[string]any
		assert.NoError(t, json.UnmarshalRead(r.Body, &body))
		assert.Equal(t, map[string]any{"event": "session_ended", "properties": map[string]any{"surface": "tui", "duration_bucket": "1_to_5m"}}, body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"queued"}`))
	}))
	defer server.Close()
	client, err := daemonclient.New(daemonclient.Config{URL: server.URL, APIKey: "selected-daemon-key", AllowInsecure: true})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	reportTUISession(ctx, client, 2*time.Minute)
	assert.Equal(t, 1, requests)
}

func TestTUISessionDeliveryTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer server.Close()
	client, err := daemonclient.New(daemonclient.Config{URL: server.URL, APIKey: "selected-daemon-key", AllowInsecure: true})
	require.NoError(t, err)
	started := time.Now()
	reportTUISession(t.Context(), client, time.Minute)
	assert.Less(t, time.Since(started), 5*time.Second)
}
