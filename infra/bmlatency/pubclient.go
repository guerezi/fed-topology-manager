package mqttbmlatency

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/GaryBoone/GoStats/stats"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type PubClient struct {
	ID         int
	BrokerURL  string
	BrokerUser string
	BrokerPass string
	PubTopic   string
	MsgSize    int
	MsgCount   int
	PubQoS     byte
	KeepAlive  int
	Quiet      bool
}

// run is a method of the PubClient struct that starts the message generator and publisher,
// and collects the results of the publishing process.
// It takes a channel `res` to send the publishing results.
// The function runs indefinitely until the publishing is done.
// It calculates various statistics about the publishing process and sends the results through the `res` channel.
func (c *PubClient) run(res chan *PubResults) {
	newMsgs := make(chan *Message)
	pubMsgs := make(chan *Message)
	doneGen := make(chan bool)
	donePub := make(chan bool)
	runResults := new(PubResults)

	started := time.Now()
	// start generator
	go c.genMessages(newMsgs, doneGen)
	// start publisher
	go c.pubMessages(newMsgs, pubMsgs, doneGen, donePub)

	runResults.ID = c.ID
	times := []float64{}
	for {
		select {
		case m := <-pubMsgs:
			if m.Error {
				fmt.Println("PUBLISHER ", c.ID, " ERROR publishing message: ", m.Topic, " at ", m.Sent.Unix())
				runResults.Failures++
			} else {
				fmt.Println("Message Sent by: ", c.ID, " on Topic:", m.Topic, " at: ", m.Sent)
				runResults.Successes++
				times = append(times, m.Delivered.Sub(m.Sent).Seconds()*1000) // in milliseconds
			}
		case <-donePub:
			// calculate results
			duration := time.Since(started)
			runResults.PubTimeMin = stats.StatsMin(times)
			runResults.PubTimeMax = stats.StatsMax(times)
			runResults.PubTimeMean = stats.StatsMean(times)
			runResults.PubTimeStd = stats.StatsSampleStandardDeviation(times)
			runResults.RunTime = duration.Seconds()
			runResults.PubsPerSec = float64(runResults.Successes) / duration.Seconds()

			// report results and exit
			res <- runResults
			return
		}
	}
}

// genMessages is a method of the PubClient struct that generates messages to be published.
// It takes an input channel for messages and a channel to signal when message generation is done.
// The function generates the specified number of messages with the specified size and sends them to the input channel.
// After generating all messages, it sends a signal that message generation is done and returns.
func (c *PubClient) genMessages(ch chan *Message, done chan bool) {
	for i := 0; i < c.MsgCount; i++ {
		ch <- &Message{
			Topic: c.PubTopic,
			QoS:   c.PubQoS,
		}
	}
	done <- true
	if !c.Quiet {
		fmt.Println("PUBLISHER ", c.ID, " is done generating messages")
	}
}

// pubMessages is a method of the PubClient struct that handles publishing messages to the MQTT broker.
// It takes input and output channels for messages, as well as channels to signal when message generation and publishing are done.
// The function establishes a connection to the MQTT broker, and continuously listens for messages from the input channel.
// When a message is received, it sets the sent time, generates the payload, and publishes the message to the broker.
// If there is an error sending the message, it sets the error flag in the message.
// After publishing the message, it sends the message to the output channel and increments a counter.
// If the doneGen channel is closed, indicating that message generation is done, it logs a message and signals that publishing is done.
// Finally, it disconnects from the broker and returns.
func (c *PubClient) pubMessages(in, out chan *Message, doneGen, donePub chan bool) {
	onConnected := func(client mqtt.Client) {
		ctr := 0
		for {
			select {
			case m := <-in:
				m.Sent = time.Now()
				m.Payload = bytes.Join([][]byte{[]byte(strconv.FormatInt(m.Sent.UnixNano(), 10)), make([]byte, c.MsgSize)}, []byte("#@#"))
				token := client.Publish(m.Topic, m.QoS, false, m.Payload)
				token.Wait()
				if token.Error() != nil {
					fmt.Println("PUBLISHER ", c.ID, " Error sending message: ", token.Error())
					m.Error = true
				} else {
					fmt.Println("PUBLISHER ", c.ID, " on Topic:", m.Topic, " at: ", m.Sent)
					m.Delivered = time.Now()
					m.Error = false
				}
				out <- m
				ctr++
			case <-doneGen:
				if !c.Quiet {
					fmt.Println("PUBLISHER ", c.ID, " had connected to the broker ", c.BrokerURL, " and done publishing for topic: ", c.PubTopic)
				}
				donePub <- true
				client.Disconnect(250)
				return
			}
		}
	}

	ka, _ := time.ParseDuration(strconv.Itoa(c.KeepAlive) + "s")

	opts := mqtt.NewClientOptions().
		AddBroker(c.BrokerURL).
		SetClientID(fmt.Sprintf("mqtt-benchmark-%v-%v", time.Now(), c.ID)).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetOnConnectHandler(onConnected).
		SetKeepAlive(ka).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			fmt.Println("PUBLISHER ", c.ID, " lost connection to the broker: ", reason.Error(), ". Will reconnect...")
		})
	if c.BrokerUser != "" && c.BrokerPass != "" {
		opts.SetUsername(c.BrokerUser)
		opts.SetPassword(c.BrokerPass)
	}
	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()

	if token.Error() != nil {
		fmt.Println("PUBLISHER ", c.ID, " had error connecting to the broker: ", token.Error())
	}
}
