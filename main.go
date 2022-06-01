package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/barancanatbas/pubsub/pubsub"
)

func publish(broker *pubsub.Broker) {
	i := 0
	for {
		msg := fmt.Sprintf("%d", i)
		i++
		// fmt.Printf("Publishing %s to %s topic\n", msg, randKey)
		go broker.Publish("hello", msg)
		// Uncomment if you want to broadcast to all topics.
		// go broker.Broadcast(msg, topicValues)
		r := rand.Intn(4)
		time.Sleep(time.Duration(r) * time.Second) //sleep for random secs.
	}
}

func main() {
	// yeni bir borker oluşturduk.
	broker := pubsub.NewBroker()
	// yeni bir subscriber oluşturduk.
	s1 := broker.AddSubscriber()
	s2 := broker.AddSubscriber()

	// sub değerlerini brokera bildirdik. ve hangi topiclere ait olduğunu belirttik.
	broker.Subscribe(s1, "hello")
	broker.Subscribe(s2, "hello")

	// Concurrent olarak publish yapıyoruz.
	go publish(broker)
	// s1 sub ve s2 sub değerleri gelen mesajları dinliyor.
	//go s1.Listen()

	go func() {
		for {
			if msg, ok := <-s1.Messages; ok {
				fmt.Println("bu sekilde de kullanabilirsin gelen deger : ", msg.GetMessageBody())
			}
		}
	}()

	go s2.Listen()
	// to prevent terminate
	fmt.Scanln()
	fmt.Println("Done!")
}
