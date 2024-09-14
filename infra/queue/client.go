package queue

import (
	"fmt"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Client struct {
	ClientID string
	client   mqtt.Client
}

// NewClient creates a new MQTT client with the provided broker and client ID.
// It establishes a connection to the broker and returns the created client.
// If an error occurs during the connection, it returns nil and the error.
func NewClient(broker string, clientID string) (*Client, error) {
	fmt.Println("NewClient connecting to broker: ", broker, " with ID: ", clientID)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(clientID)
	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	return &Client{
		ClientID: clientID,
		client:   client,
	}, nil
}

// Consume subscribes to multiple topics and starts consuming messages using the provided messageHandler.
// It returns a boolean indicating whether the consumption started successfully and an error if any.
func (c Client) Consume(topics map[string]byte, messageHandler mqtt.MessageHandler) (bool, error) {
	fmt.Println("Client ", c.ClientID, " subscribing to topics: ", topics)

	token := c.client.SubscribeMultiple(topics, messageHandler)
	token.Wait()

	if token.Error() != nil {
		return false, token.Error()
	}

	fmt.Println("Consumer running...")
	return true, nil
}

// Publish publishes a message to the specified topic with the given payload, quality of service (QoS),
// and retained flag. It returns a boolean indicating whether the message was successfully published
// and an error if any occurred.
func (c Client) Publish(topic string, payload []byte, qos byte, retained bool) (bool, error) {
	fmt.Println("Client ", c.ClientID, " publishing to topic: ", topic)

	token := c.client.Publish(topic, qos, retained, payload)
	token.Wait()

	if token.Error() != nil {
		return false, token.Error()
	}

	return true, nil
}

func (c Client) Disconnect() {
	c.client.Disconnect(10)
}
