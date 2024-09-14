package mqttbmlatency

import (
	"fmt"
	"strconv"
	"time"

	"github.com/GaryBoone/GoStats/stats"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type SubClient struct {
	ID         int
	BrokerURL  string
	BrokerUser string
	BrokerPass string
	SubTopic   string
	SubQoS     byte
	KeepAlive  int
	Quiet      bool
}

// run is a method of the SubClient struct that starts the subscriber client and handles the subscription process.
// It receives three channels: res, subDone, and jobDone. <\br>
// [res] is a channel used to send the results of the subscription process.
// [subDone] is a channel used to signal the completion of the subscription process.
// [jobDone] is a channel used to signal the completion of the job.
// The function connects to the MQTT broker, subscribes to the specified topic, and listens for incoming messages.
// It calculates the forward latency for each received message and stores the results in the runResults struct.
// Once the jobDone channel receives a signal, the function disconnects from the broker, calculates the statistics for the forward latency,
// sends the runResults to the res channel, and returns.
func (c *SubClient) run(res chan *SubResults, subDone chan string, jobDone chan bool) {
	runResults := new(SubResults)
	runResults.ID = c.ID

	forwardLatency := []float64{}

	ka, _ := time.ParseDuration(strconv.Itoa(c.KeepAlive) + "s")

	opts := mqtt.NewClientOptions().
		AddBroker(c.BrokerURL).
		SetClientID(fmt.Sprintf("mqtt-benchmark-%v-%v", time.Now(), c.ID)).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetKeepAlive(ka).
		SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
			recvTime := time.Now().UnixNano()
			payload := msg.Payload()
			i := 0
			for ; i < len(payload)-3; i++ {
				// Find the sent time in the payload by looking for the pattern "#@#"
				if payload[i] == '#' && payload[i+1] == '@' && payload[i+2] == '#' {
					sendTime, _ := strconv.ParseInt(string(payload[:i]), 10, 64)
					forwardLatency = append(forwardLatency, float64(recvTime-sendTime)/1000000) // in milliseconds
					break
				}
			}
			runResults.Received++
		}).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			fmt.Println("SUBSCRIBER ", c.ID, " lost connection to the broker: ", reason.Error(), ". Will reconnect...")
		})

	if c.BrokerUser != "" && c.BrokerPass != "" {
		opts.SetUsername(c.BrokerUser)
		opts.SetPassword(c.BrokerPass)
	}
	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Println("SUBSCRIBER ", c.ID, " had error connecting to the broker: ", token.Error())
		subDone <- "error"
		return
	}

	if token := client.Subscribe(c.SubTopic, c.SubQoS, nil); token.Wait() && token.Error() != nil {
		fmt.Println("SUBSCRIBER ", c.ID, " had error subscribe with topic: ", token.Error())
		subDone <- "error"
		return
	}

	if !c.Quiet {
		fmt.Println("SUBSCRIBER ", c.ID, " had connected to the broker: ", c.BrokerURL, " and subscribed with topic: ", c.SubTopic)
	}

	subDone <- "success"
	//加各项统计
	// why is this comment in chinese?
	for {
		select {
		case <-jobDone:
			client.Disconnect(250)
			runResults.FwdLatencyMin = stats.StatsMin(forwardLatency)
			runResults.FwdLatencyMax = stats.StatsMax(forwardLatency)
			runResults.FwdLatencyMean = stats.StatsMean(forwardLatency)
			runResults.FwdLatencyStd = stats.StatsSampleStandardDeviation(forwardLatency)
			res <- runResults
			if !c.Quiet {
				fmt.Println("SUBSCRIBER ", c.ID, " is done subscribe")
			}
			return
		}
	}
}
