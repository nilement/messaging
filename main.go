package main

import (
	"bytes"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type TopicBroker struct {
	Topics           map[string]Topic
	GlobalMessagesID chan int
	DeleteLock       *sync.Mutex
}

type Topic struct {
	Listeners map[chan Message]bool
	Messages  chan Message
	Quit      chan bool
	lock      *sync.Mutex
}

type Message struct {
	ID   int
	Data []byte
}

func EventStringWithID(message Message, event string) string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("data: %s\n", message.Data))
	buffer.WriteString(fmt.Sprintf("event: %s\n", event))
	buffer.WriteString(fmt.Sprintf("id: %d\n", message.ID))
	return buffer.String()
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
		globalID := <-broker.GlobalMessagesID
		message := &Message{
			Data: buf.Bytes(),
			ID:   globalID,
		}
		globalID++
		broker.GlobalMessagesID <- globalID
		broker.DeleteLock.Lock()
		if topic, ok := broker.Topics[topicName]; ok {
			topic.Messages <- *message
		}
		broker.DeleteLock.Unlock()
		rw.WriteHeader(204)
		return
	}

	messageChannel := make(chan Message)
	topicName := getPath(req)
	broker.DeleteLock.Lock()
	if topic, ok := broker.Topics[topicName]; ok {
		broker.Topics[topicName].lock.Lock()
		topic.Listeners[messageChannel] = true
		broker.Topics[topicName].lock.Unlock()
	} else {
		CreateTopic(broker, topicName)
		broker.Topics[topicName].Listeners[messageChannel] = true
	}
	broker.DeleteLock.Unlock()
	connectionTime := time.Now()

	for {
		select {
		case m := <-messageChannel:
			fmt.Fprintf(rw, "%s\n", EventStringWithID(m, "msg"))
		default:
			if TimeoutCheck(connectionTime) {
				broker.DeleteLock.Lock()
				broker.Topics[topicName].lock.Lock()
				delete(broker.Topics[topicName].Listeners, messageChannel)
				if (len(broker.Topics[topicName].Listeners)) == 0 {
					broker.Topics[topicName].Quit <- true
					broker.Topics[topicName].lock.Unlock()
					delete(broker.Topics, topicName)
				} else {
					broker.Topics[topicName].lock.Unlock()
				}
				fmt.Fprintf(rw, "%s\n", EventString([]byte("Timeout"), "timeout"))
				broker.DeleteLock.Unlock()
				return
			}
		}

		flusher.Flush()
	}

}

func TimeoutCheck(connectionTime time.Time) bool {
	currentTime := time.Now()
	delta := currentTime.Sub(connectionTime)
	if delta.Seconds() >= 5 {
		return true
	}
	return false
}

func getPath(req *http.Request) string {
	return req.RequestURI[1:len(req.RequestURI)]
}

func CreateTopic(broker *TopicBroker, topic string) {
	newTopic := &Topic{
		Listeners: make(map[chan Message]bool),
		Messages:  make(chan Message, 100),
		Quit:      make(chan bool, 1),
		lock:      new(sync.Mutex),
	}

	broker.Topics[topic] = (*newTopic)
	go newTopic.listen()
}

func (topic *Topic) listen() {
	for {
		topic.lock.Lock()
		select {
		case <-topic.Quit:
			defer topic.lock.Unlock()
			return
		case message := <-topic.Messages:
			for listener := range topic.Listeners {
				listener <- message
			}
		default:
		}
		topic.lock.Unlock()
	}
}

func NewBroker() (broker *TopicBroker) {
	broker = &TopicBroker{
		Topics:           make(map[string]Topic),
		DeleteLock:       new(sync.Mutex),
		GlobalMessagesID: make(chan int, 1),
	}

	broker.GlobalMessagesID <- 0

	return
}

func main() {
	broker := NewBroker()

	http.ListenAndServe(":2666", broker)
}
