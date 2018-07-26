package main

import (
	"bytes"
	"fmt"
	"net/http"
)

type TopicBroker struct {
	Topics map[string]Topic
}

type Topic struct {
	Listeners map[chan []byte]bool
	Messages  chan []byte
}

func (broker *TopicBroker) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	//fmt.Println("Connected!")
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
	}

	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	rw.Header().Set("Access-Control-Allow-Origin", "*")

	messageChannel := make(chan []byte)
	topic := getPath(req)
	if topic, ok := broker.Topics[topic]; ok {
		topic.Listeners[messageChannel] = true
	}

	for {
		//fmt.Println("Looking for message!")
		fmt.Fprintf(rw, "data: %s\n\n", <-messageChannel)

		flusher.Flush()
	}

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
}

func (topic *Topic) listen() {
	for {
		select {
		case message := <-topic.Messages:
			//fmt.Println("putting message to listeners!")
			for listener := range topic.Listeners {
				//fmt.Println("Put message to listener!")
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

	/*go func() {
		for {
			//fmt.Println("Putting message!")
			broker.Topics["info"].Messages <- []byte("Hello")
			//fmt.Println("Message put!")
			time.Sleep(time.Second * 3)
		}
	}()*/

	http.ListenAndServe(":2666", broker)
}
