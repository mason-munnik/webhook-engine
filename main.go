package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

type Subscription struct {
	ID         string   `json:"id"`
	Consumer   string   `json:"consumer"`
	TargetURL  string   `json:"target_url"`
	EventTypes []string `json:"event_types"`
}

type Event struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func readJSON(w http.ResponseWriter, r *http.Request, i any) error {
	wrappedBody := http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(wrappedBody)
	return dec.Decode(i)
}

var httpClient = &http.Client{
	Timeout: 5 * time.Second,
}
var db *sql.DB

func createSubscription(w http.ResponseWriter, r *http.Request) {
	var sub Subscription
	err := readJSON(w, r, &sub)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sub.ID = uuid.NewString()
	eventTypesJSON, err := json.Marshal(sub.EventTypes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = db.Exec(
		"INSERT INTO subscriptions (id, consumer, target_url, event_types) VALUES (?, ?, ?, ?)",
		sub.ID, sub.Consumer, sub.TargetURL, string(eventTypesJSON),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(sub)
}

func createEvent(w http.ResponseWriter, r *http.Request) {
	var event Event
	err := readJSON(w, r, &event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	event.ID = uuid.NewString()
	_, err = db.Exec(
		"INSERT INTO events (id, type, payload) VALUES (?, ?, ?)",
		event.ID, event.Type, string(event.Payload),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rows, err := db.Query("SELECT id, consumer, target_url, event_types FROM subscriptions")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var matches []Subscription
	for rows.Next() {
		var sub Subscription
		var eventTypesJSON string
		if err := rows.Scan(&sub.ID, &sub.Consumer, &sub.TargetURL, &eventTypesJSON); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := json.Unmarshal([]byte(eventTypesJSON), &sub.EventTypes); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, eventType := range sub.EventTypes {
			if eventType == event.Type {
				matches = append(matches, sub)
			}
		}
	}

	for _, sub := range matches {
		go sendWebhook(sub, event)
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(event)
}

func sendWebhook(sub Subscription, event Event) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("error marshaling event: %v", err)
		return
	}

	for i := 0; i < 3; i++ {
		resp, err := httpClient.Post(sub.TargetURL, "application/json", bytes.NewReader(data))
		if err != nil {
			log.Printf("attempt %d failed to %s: %v", i+1, sub.TargetURL, err)
			time.Sleep(time.Second)
			continue
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			log.Printf("webhook delivered to %s (status %d)", sub.TargetURL, resp.StatusCode)
			resp.Body.Close()
			break
		}

		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			log.Printf("client error (%d) from %s, not retrying", resp.StatusCode, sub.TargetURL)
			resp.Body.Close()
			return
		}

		log.Printf("server error (%d) from %s, retrying...", resp.StatusCode, sub.TargetURL)
		resp.Body.Close()
		time.Sleep(time.Second)
	}
}

func getSubs(w http.ResponseWriter, r *http.Request) {
	// sql call to SELECT all subscriptions in the sub table in webhook.db
	rows, err := db.Query("SELECT id, consumer, target_url, event_types FROM subscriptions")
	// if errors do interal service error
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	subs := []Subscription{}
	for rows.Next() {
		var sub Subscription
		var eventTypesJSON string
		if err := rows.Scan(&sub.ID, &sub.Consumer, &sub.TargetURL, &eventTypesJSON); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := json.Unmarshal([]byte(eventTypesJSON), &sub.EventTypes); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		subs = append(subs, sub)
	}

	// encode the sql data into JSON and then return that as the body
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(subs)
}

func webhookTest(w http.ResponseWriter, r *http.Request) {
	reqBody, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(reqBody)
}

func main() {
	var err error
	db, err = sql.Open("sqlite", "webhooks.db")
	if err != nil {
		log.Fatal(err)
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
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook-test", webhookTest)
	mux.HandleFunc("POST /subscription", createSubscription)
	mux.HandleFunc("POST /events", createEvent)
	mux.HandleFunc("GET /subscriptions", getSubs)
	log.Fatal(http.ListenAndServe(":8080", mux))
}
