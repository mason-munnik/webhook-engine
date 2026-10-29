package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

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
var httpClient = &http.Client{
	Timeout: 5 * time.Second,
}

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
	var matches []Subscription
	for _, sub := range subscriptions {
		for _, eventType := range sub.EventTypes {
			if eventType == event.Type {
				matches = append(matches, sub)
			}
		}
	}
	mu.Unlock()

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
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook-test", webhookTest)
	mux.HandleFunc("POST /subscription", createSubscription)
	mux.HandleFunc("POST /events", createEvent)
	log.Fatal(http.ListenAndServe(":8080", mux))
}
