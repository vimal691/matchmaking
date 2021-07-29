package main

import (
	"fmt"
	"sync"
	"time"
)

// example:
// channel here is equal to subscriber
// topic1 can speak to ch1 ch2; 
// topic2 can speak to ch3 ch2; 
type MessageBus struct {
	mu sync.Mutex
	// multiple people can subscribe to same topic
	// mapping from topic to subscriber channel
	subscribers map[string][]chan string
	subscriptionList map[string]Subscription
	// if we want to close the complete Bus
	closed bool `default: false`
}

// structure to save all subscriptions
type Subscription struct {
	subscriptionID string
	topics []string
	channel chan string
}

// Initialize the Message bus which will create structure of MessageBus
func InitMessageBus() *MessageBus {
	mb := &MessageBus{}
	mb.subscribers = make(map[string][]chan string)
	mb.subscriptionList = make(map[string]Subscription)
	mb.closed = false
	return mb
}

//This function will subscribe the user/subscription to the topic provided
//params [string] Topic name
//params [Subscription] for which subscription
//return [nil]
func (mb *MessageBus) Subscribe(topic string, sub Subscription) {
	_, ok := mb.subscribers[topic]
	if ok {
		ch := sub.channel
		id := sub.subscriptionID
		mb.mu.Lock()
		mb.subscribers[topic] = append(mb.subscribers[topic], ch)
		mb.mu.Unlock()
		fmt.Printf("$$$$$ SubscriptionId %v have Subscribed to Topic : %s\n", id, topic)
	}else {
		fmt.Printf("Please create Topic : %s ; first.\n", topic)
	}
}

//This function will Unsubscribe the user/subscription from the topic provided
//params [string] Topic name
//params [Subscription] for which subscription
//return [nil]
func (mb *MessageBus) UnSubscribe(topic string, sub Subscription) {
	_, ok := mb.subscribers[topic]
	if !ok {
		fmt.Printf("There is no Topic : %s ; in topic list. So can not unsubscribe.\n", topic)
		return
	}
	index := -1
	ch := sub.channel
	id := sub.subscriptionID
	for idx,channel := range(mb.subscribers[topic]) {
		if channel == ch {
			index = idx
		}
	}
	mb.mu.Lock()
	if index != -1 {
		mb.subscribers[topic] = append(mb.subscribers[topic][:index], mb.subscribers[topic][index+1:]...)
	}
	mb.mu.Unlock()
	fmt.Printf("***** SubscriptionId %v have Unsubscribed to Topic : %s\n", id, topic)
}
// Given Subscription details for a user this function will subscribe to all the topic provided
// will create a channel for that user and start goroutine to ack if some message received.
//params [Subscription] user info
//return [channel] channel
func (mb *MessageBus) AddSubscription(sub Subscription) chan string{
	topics := sub.topics
	ch := sub.channel
	id := sub.subscriptionID
	_, ok := mb.subscriptionList[id]
	if ok {
		fmt.Printf("subscriptionID : %s ; already exist.\n", id)
		return mb.subscriptionList[id].channel
	}
	for _, topic := range(topics) {
		if ch != nil{
			mb.Subscribe(topic, sub)
		}else{
			ch = make(chan string, 1)
			sub.channel = ch
			mb.Subscribe(topic, sub)
		}
	}
	mb.mu.Lock()
	mb.subscriptionList[id] = sub
	go ack(id, ch)  // through goroutine it will start listening and ack when message received to the channel
	mb.mu.Unlock()
	return ch
}

//given subscriptionID, this function will delete that subscription
//params [string] subscription id of user
//return [nil]
func (mb *MessageBus) DeleteSubscription(subscriptionID string) {
	_, ok := mb.subscriptionList[subscriptionID]
	if !ok {
		fmt.Printf("subscriptionID : %s ; does not exist.\n", subscriptionID)
		return
	}
	topics := mb.subscriptionList[subscriptionID].topics
	sub := mb.subscriptionList[subscriptionID]
	ch := sub.channel
	for _, topic := range(topics) {
		mb.UnSubscribe(topic, sub)
	}
	mb.mu.Lock()
	delete(mb.subscriptionList, subscriptionID)
	close(ch)
	mb.mu.Unlock()
}

//given topic and message it will publish that message into that topic bus
//params [string] topic name
//params [string] message to be published in topic
//return [nil]
func (mb *MessageBus) Publish(topic string, msg string) {
	// for every subscriber of topic message is pushed and
	// as it will be waiting until channel receive the message, akn is assured
	_, ok := mb.subscribers[topic]
	if ok {
		fmt.Printf("+++++ Publishing Message in topic : %s, Msg : %s\n", topic, msg)
		mb.mu.Lock()
		for _, ch := range(mb.subscribers[topic]) {
			if ch != nil {
				ch <- msg
			}
		}
		mb.mu.Unlock()
		time.Sleep(1 * time.Millisecond)
	} else {
		fmt.Printf("Topic : %s ; does not exist so cannot publish message in it.\n", topic)
	}
}

//given topic name this function will create that topic, if already there then do nothing
//params [string] topic name
//return [nil]
func (mb *MessageBus) CreateTopic(topic string) {
	mb.mu.Lock()
	_, ok := mb.subscribers[topic]
	if !ok {
		mb.subscribers[topic] = make([]chan string,0,100)
	}else {
		fmt.Printf("Topic : %s ; already exist in topic list. So can not reinitializing.\n", topic)
	}
	mb.mu.Unlock()
}

//given topic name this function will delete that topic.
// first it will Unsubscribe all users subscribed to the topic then delete the topic.
//params [string] topic name
//return [nil]
func (mb *MessageBus) DeleteTopic(topic string){
	mb.mu.Lock()
	// remove topic from all subscribers topic list
	_, ok := mb.subscribers[topic]
	if ok {
		for subID, subs := range(mb.subscriptionList){
			index := -1
			for idx, topic_ := range(subs.topics) {
				if topic == topic_ {
					index = idx
				}
			}
			if index != -1 {
				subs.topics = append(subs.topics[:index], subs.topics[index+1:]...)
				fmt.Printf(">>>>>>>>>> Topic: %v, removed from subscription list of: %v \n", topic, subID)
				mb.subscriptionList[subID] = subs
			}
		}
		delete(mb.subscribers, topic) //delete topic
		fmt.Printf(">>>>>>>>>> Topic %v Deleted.\n", topic)
	}else {
		fmt.Printf("There is no Topic : %s ; in topic list. So can not Delete it.\n", topic)
	}
	mb.mu.Unlock()
}

//when the channel receives a message it will acknowledge it.
//params [string] subscription ID
//params [channel] channel reference/address
//return [nil]
func ack(subId string, ch chan string) {
	for msg := range ch {
		fmt.Printf("----- subscriptionID:[%s] got message: %s\n", subId, msg)
	}
}

func main() {
	mbus := InitMessageBus()

	//creating topics
	mbus.CreateTopic("ml")
	mbus.CreateTopic("python")
	mbus.CreateTopic("football")
	mbus.CreateTopic("cricket")
	mbus.CreateTopic("golang")
	mbus.CreateTopic("systemdesign")

	//adding Subscriptions
	sub1 := Subscription{
		subscriptionID: "1",
		topics: []string{"ml","python", "cricket"},
		channel: make(chan string, 1),
	}
	mbus.AddSubscription(sub1)

	sub2 := Subscription{
		subscriptionID: "2",
		topics: []string{"golang", "systemdesign"},
		channel: make(chan string, 1),
	}
	mbus.AddSubscription(sub2)

	sub3 := Subscription{
		subscriptionID: "3",
		topics: []string{"cricket", "football", "python"},
		channel: make(chan string, 1),
	}
	mbus.AddSubscription(sub3)

	sub4 := Subscription{
		subscriptionID: "4",
		topics: []string{"football", "golang"},
		channel: make(chan string, 1),
	}
	mbus.AddSubscription(sub4)

	time.Sleep(50 * time.Millisecond)

	mbus.Publish("golang", "system design language")
	mbus.Publish("football", "Semi-Finals, Jul 8 2021, Thu - 00:30 (IST) , Wembley Stadium, London. Vs. Denmark. European Championship")
	mbus.Publish("ml", "need more data")
	mbus.Publish("cricket", "ICC Champions Trophy")
	mbus.Publish("systemdesign", "haproxy")
	mbus.Publish("python", "scripting")
	mbus.UnSubscribe("golang",sub4) // example of Unsubscribe
	mbus.Publish("golang", "concurrent will be recieved by only sub2")
	mbus.Subscribe("golang",sub4)  // example of subscribing to additional channel in middle
	mbus.Publish("golang", "concurrent will be recieved by both sub2 and sub4")
	mbus.Publish("systemdesign", "youtube live")
	mbus.DeleteSubscription(sub1.subscriptionID)  // example of deleting subscription itself
	mbus.Publish("python", "fuzzymatch")
	mbus.Subscribe("ml",sub2)  // example of subscribing to additional channel in middle
	mbus.Publish("ml", "generate blistm")
	mbus.Publish("cricket", "T20 coming up!")
	mbus.DeleteTopic("football")  // example of deleting a Topic
	mbus.Publish("football", "Semi-Finals, Jul 7 2021, Wed - 00:30 (IST) , Wembley Stadium, London. Italy. Vs. Spain")

	time.Sleep(50 * time.Millisecond)
}
