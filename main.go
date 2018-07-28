package main

import (
	"bytes"
	"fmt"
	"net/http"
	"time"
)

type TopicBroker struct {
	Topics map[string]Topic
}

type Topic struct {
	Listeners map[chan []byte]bool
	Messages  chan []byte
}

type Message struct {
	data  []byte
	event string
}

func EventString(message []byte, event string) string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("data: %s\n", message))
	buffer.WriteString(fmt.Sprintf("event: %s\n", event))
	return buffer.String()
}

func (broker *TopicBroker) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	rw.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	rw.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

	flusher, ok := rw.(http.Flusher)

	if !ok {
		http.Error(rw, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	if req.Method == "POST" {
		topicName := getPath(req)
		buf := new(bytes.Buffer)
		buf.ReadFrom(req.Body)
		s := buf.Bytes()
		if topic, ok := broker.Topics[topicName]; ok {
			topic.Messages <- s
		}
		rw.WriteHeader(204)
		return
	}

	messageChannel := make(chan []byte)
	topicName := getPath(req)
	if topic, ok := broker.Topics[topicName]; ok {
		topic.Listeners[messageChannel] = true
	} else {
		CreateTopic(broker, topicName)
		broker.Topics[topicName].Listeners[messageChannel] = true
	}
	connectionTime := time.Now()

	for {
		select {
		case m := <-messageChannel:
			fmt.Fprintf(rw, "%s\n", EventString(m, "msg"))
		default:
			if TimeoutCheck(connectionTime) {
				delete(broker.Topics[topicName].Listeners, messageChannel)
				fmt.Fprintf(rw, "%s\n", EventString([]byte("Timeout"), "timeout"))
				return
			}
		}

		flusher.Flush()
	}

}

func TimeoutCheck(connectionTime time.Time) bool {
	currentTime := time.Now()
	delta := currentTime.Sub(connectionTime)
	if delta.Seconds() >= 10 {
		return true
	}
	return false
}

func getPath(req *http.Request) string {
	return req.RequestURI[1:len(req.RequestURI)]
}

func CreateTopic(broker *TopicBroker, topic string) {
	newTopic := &Topic{
		Listeners: make(map[chan []byte]bool),
		Messages:  make(chan []byte),
	}

	broker.Topics[topic] = (*newTopic)
	go newTopic.listen()
}

func (topic *Topic) listen() {
	for {
		select {
		case message := <-topic.Messages:
			for listener := range topic.Listeners {
				listener <- message
			}
		}
	}
}

func NewBroker() (broker *TopicBroker) {
	broker = &TopicBroker{
		Topics: make(map[string]Topic),
	}

	return
}

func main() {
	broker := NewBroker()

	http.ListenAndServe(":2666", broker)
}
