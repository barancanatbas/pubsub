package pubsub

import (
	"crypto/rand"
	"fmt"
	"log"
	"sync"
)

type Subscriber struct {
	Id       string          // Id of subscriber
	Messages chan *Message   // messages channel
	Topics   map[string]bool // topics it is subscribed to.
	Active   bool            // if given subscriber is Active
	Mutex    sync.RWMutex    // lock
}

func CreateNewSubscriber() (string, *Subscriber) {
	// returns a new subscriber.
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}
	Id := fmt.Sprintf("%X-%X", b[0:4], b[4:8])
	return Id, &Subscriber{
		Id:       Id,
		Messages: make(chan *Message),
		Topics:   map[string]bool{},
		Active:   true,
	}
}

func (s *Subscriber) AddTopic(topic string) {
	// add topic to the subscriber
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	s.Topics[topic] = true
}

func (s *Subscriber) RemoveTopic(topic string) {
	// remove topic to the subscriber
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	delete(s.Topics, topic)
}

func (s *Subscriber) GetTopics() []string {
	// Get all topic of the subscriber
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	topics := []string{}
	for topic, _ := range s.Topics {
		topics = append(topics, topic)
	}
	return topics
}

func (s *Subscriber) Destruct() {
	// destructor for subscriber.
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	s.Active = false
	close(s.Messages)
}

func (s *Subscriber) Signal(msg *Message) {
	// Gets the message from the channel
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	if s.Active {
		s.Messages <- msg
	}
}

func (s *Subscriber) Listen() {
	// Listens to the message channel, prints once received.
	for {
		if msg, ok := <-s.Messages; ok {
			fmt.Println(msg.GetMessageBody())
		}
	}
}
