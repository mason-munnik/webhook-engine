package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func setupTestDB(t *testing.T) {
	t.Helper()
	var err error
	db, err = sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS subscriptions (
			id          TEXT PRIMARY KEY,
			consumer    TEXT NOT NULL,
			target_url  TEXT NOT NULL,
			event_types TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS events (
			id         TEXT PRIMARY KEY,
			type       TEXT NOT NULL,
			payload    TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("failed to create tables: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
	})
}

func TestCreateAndGetSubscriptions(t *testing.T) {
	setupTestDB(t)

	// 1. Create a subscription via POST /subscription
	body := `{"consumer":"notify-service","target_url":"http://localhost:8083/webhook","event_types":["budget.exceeded","transaction.created"]}`
	req := httptest.NewRequest(http.MethodPost, "/subscription", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()

	createSubscription(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var created Subscription
	if err := json.NewDecoder(rec.Body).Decode(&created); err != nil {
		t.Fatalf("failed to decode created subscription: %v", err)
	}
	if created.ID == "" {
		t.Errorf("expected generated UUID, got empty ID")
	}

	// 2. Fetch subscriptions via GET /subscriptions
	getReq := httptest.NewRequest(http.MethodGet, "/subscriptions", nil)
	getRec := httptest.NewRecorder()

	getSubs(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", getRec.Code)
	}

	var subs []Subscription
	if err := json.NewDecoder(getRec.Body).Decode(&subs); err != nil {
		t.Fatalf("failed to decode subscriptions list: %v", err)
	}
	if len(subs) != 1 {
		t.Fatalf("expected 1 subscription, got %d", len(subs))
	}
	if subs[0].Consumer != "notify-service" || len(subs[0].EventTypes) != 2 {
		t.Errorf("unexpected subscription returned: %+v", subs[0])
	}
}

func TestCreateEventAndDeliverNestedJSONPayload(t *testing.T) {
	setupTestDB(t)

	// 1. Spin up a mock receiver server to capture the delivered webhook
	receivedCh := make(chan Event, 1)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var evt Event
		_ = json.Unmarshal(raw, &evt)
		receivedCh <- evt
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	// 2. Register a subscription pointing to our mock receiver
	subBody, _ := json.Marshal(Subscription{
		Consumer:   "budgit-sheets",
		TargetURL:  receiver.URL,
		EventTypes: []string{"transaction.created"},
	})
	subReq := httptest.NewRequest(http.MethodPost, "/subscription", bytes.NewReader(subBody))
	subRec := httptest.NewRecorder()
	createSubscription(subRec, subReq)

	// 3. Publish an event with a nested JSON object payload
	evtJSON := `{"type":"transaction.created","payload":{"category":"Groceries","amount_cents":-8431}}`
	evtReq := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(evtJSON))
	evtRec := httptest.NewRecorder()
	createEvent(evtRec, evtReq)

	if evtRec.Code != http.StatusCreated {
		t.Fatalf("expected status 201 from createEvent, got %d: %s", evtRec.Code, evtRec.Body.String())
	}

	// 4. Verify the background goroutine delivered the exact nested JSON payload
	select {
	case delivered := <-receivedCh:
		if delivered.Type != "transaction.created" {
			t.Errorf("expected type transaction.created, got %s", delivered.Type)
		}
		var payload struct {
			Category    string `json:"category"`
			AmountCents int64  `json:"amount_cents"`
		}
		if err := json.Unmarshal(delivered.Payload, &payload); err != nil {
			t.Fatalf("failed to unmarshal nested payload: %v", err)
		}
		if payload.Category != "Groceries" || payload.AmountCents != -8431 {
			t.Errorf("unexpected payload values: %+v", payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for webhook delivery")
	}
}
