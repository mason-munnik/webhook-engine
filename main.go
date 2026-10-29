package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/google/uuid"
)

type Subscription struct {
	ID         string   `json:"id"`
	Consumer   string   `json:"consumer"`
	TargetURL  string   `json:"target_url"`
	EventTypes []string `json:"event_types"`
}

type Event struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

func readJSON(w http.ResponseWriter, r *http.Request, i any) error {
	wrappedBody := http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(wrappedBody)
	return dec.Decode(i)
}

var subscriptions = map[string]Subscription{}
var mu sync.Mutex
var events = map[string]Event{}

func createSubscription(w http.ResponseWriter, r *http.Request) {
	var sub Subscription
	err := readJSON(w, r, &sub)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sub.ID = uuid.NewString()
	mu.Lock()
	subscriptions[sub.ID] = sub
	mu.Unlock()
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
	mu.Lock()
	events[event.ID] = event
	mu.Unlock()
	var matches []Subscription
	for _, sub := range subscriptions {
		for _, eventType := range sub.EventTypes {
			if eventType == event.Type {
				matches = append(matches, sub)
			}
		}
	}
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(event)
}

func webhookTest(w http.ResponseWriter, r *http.Request) {
	reqBody, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// responds with status okay here
	w.WriteHeader(http.StatusOK)
	// makes response body the reqBody
	w.Write(reqBody)
}

func main() {
	// Step 1: Set up an HTTP server and your first route
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook-test", webhookTest)
	mux.HandleFunc("POST /subscription", createSubscription)
	mux.HandleFunc("POST /events", createEvent)
	log.Fatal(http.ListenAndServe(":8080", mux))
}
