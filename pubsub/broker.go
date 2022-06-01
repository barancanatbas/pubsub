package pubsub

import (
	"fmt"
	"sync"
)

// broker

type Subscribers map[string]*Subscriber

type Broker struct {
	subscribers Subscribers            // map of subscribers Id:Subscriber
	topics      map[string]Subscribers // map of topic to subscribers
	mut         sync.RWMutex           // mutex lock
}

func NewBroker() *Broker {
	// yeni bir broker oluşturduk.
	return &Broker{
		subscribers: Subscribers{},
		topics:      map[string]Subscribers{},
	}
}

func (b *Broker) AddSubscriber() *Subscriber {
	// Add subscriber to the broker.
	b.mut.Lock()
	defer b.mut.Unlock()
	Id, s := CreateNewSubscriber()
	b.subscribers[Id] = s
	return s
}

func (b *Broker) RemoveSubscriber(s *Subscriber) {
	// sub kısmındaki topics değerlerinden ilk olarak siliyoruz
	for topic := range s.Topics {
		b.Unsubscribe(s, topic)
	}
	b.mut.Lock()
	// son olarak broker kısmından siliyoruz.
	delete(b.subscribers, s.Id)
	b.mut.Unlock()
	s.Destruct()
}

func (b *Broker) Broadcast(msg string, topics []string) {
	// broadcast message to all topics.
	for _, topic := range topics {
		for _, s := range b.topics[topic] {
			m := NewMessage(msg, topic)
			go (func(s *Subscriber) {
				s.Signal(m)
			})(s)
		}
	}
}

func (b *Broker) GetSubscribers(topic string) int {
	// verilen topic değerine göre subscriber sayısını döndürür.
	b.mut.RLock()
	defer b.mut.RUnlock()
	return len(b.topics[topic])
}

func (b *Broker) Subscribe(s *Subscriber, topic string) {
	// subscribe to given topic
	b.mut.Lock()
	defer b.mut.Unlock()
	if b.topics[topic] == nil {
		b.topics[topic] = Subscribers{}
	}
	s.AddTopic(topic)
	b.topics[topic][s.Id] = s
	fmt.Printf("%s Subscribed for topic: %s\n", s.Id, topic)
}

func (b *Broker) Unsubscribe(s *Subscriber, topic string) {
	// unsubscribe to given topic
	b.mut.RLock()
	defer b.mut.RUnlock()
	delete(b.topics[topic], s.Id)
	s.RemoveTopic(topic)
	fmt.Printf("%s Unsubscribed for topic: %s\n", s.Id, topic)
}

func (b *Broker) Publish(topic string, msg string) {
	// verilen topic değerine göre mesajı yayınlar.
	b.mut.RLock()
	bTopics := b.topics[topic]
	b.mut.RUnlock()
	for _, s := range bTopics {
		m := NewMessage(msg, topic)
		if !s.Active {
			return
		}
		go (func(s *Subscriber) {
			s.Signal(m)
		})(s)
	}
}
