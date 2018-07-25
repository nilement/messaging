package main

import (
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

func (broker *TopicBroker) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	fmt.Println("Connected!")
	flusher, ok := rw.(http.Flusher)

	if !ok {
		http.Error(rw, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	rw.Header().Set("Access-Control-Allow-Origin", "*")

	messageChannel := make(chan []byte)
	topic := req.RequestURI[1:len(req.RequestURI)]
	if topic, ok := broker.Topics[topic]; ok {
		topic.Listeners[messageChannel] = true
	}

	for {
		fmt.Fprintf(rw, "data: %s\n\n", <-broker.Topics["info"].Messages)
		time.Sleep(time.Second * 3)
		flusher.Flush()
	}

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
	newTopic := &Topic{
		Listeners: make(map[chan []byte]bool),
		Messages:  make(chan []byte),
	}

	broker.Topics["info"] = (*newTopic)
	go newTopic.listen()

	return
}

func main() {
	broker := NewBroker()
	go func(){
		broker.Topics["info"].Messages <- []byte("Hello")
		time.Sleep(time.Second * 5)
	}
	http.ListenAndServe(":2666", broker)
}
