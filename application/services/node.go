package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	keys "topology/application/crypto"
	"topology/application/database"
	"topology/application/database/models"
	"topology/application/utils"
	mqttbmlatency "topology/infra/bmlatency"
	paho "topology/infra/queue"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/go-co-op/gocron"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type NodeService struct {
	Collection *mongo.Collection
	Topics     *mongo.Collection
	Client     *paho.Client
}

// NewNodeService creates a new NodeService instance.
// It connects to the database and returns a NodeService instance with the collection of nodes.
// Returns nil if there was a problem connecting to the database.
func NewNodeService() *NodeService {
	db := database.NewDb("root")
	_, err := db.Connect()

	if err != nil {
		return nil
	}

	collection := db.GetCollection("nodes")
	topics := db.GetCollection("topics")

	return &NodeService{
		Collection: collection,
		Topics:     topics,
	}
}

// getNodeId retrieves the last inserted node id from the database and returns the next id.
// Returns 0 if there was a problem retrieving the last inserted node id.
func (n *NodeService) getNodeId() int64 {
	filter := bson.D{{}}
	opts := options.FindOne().SetProjection(bson.D{{Key: "id", Value: 1}}).SetSort(bson.D{{Key: "id", Value: -1}})

	var lastInsertedNode models.Node
	err := n.Collection.FindOne(context.Background(), filter, opts).Decode(&lastInsertedNode)

	fmt.Println("Last inserted node: ", lastInsertedNode, err)
	if err != nil {
		return 0
	}

	return lastInsertedNode.Id + 1
}

// getNodeLatency retrieves the latency of a node by sending a benchmark message to the broker.
// Returns the latency of the node and an error if there was a problem retrieving the latency.
func (n *NodeService) getNodeLatency(broker string) (float64, error) {
	// Start the benchmark latency test
	// The parameters are the broker address, the topic to publish the benchmark message,
	// the number of messages to send, the message size, the QoS, the number of publishers,
	// the number of subscribers, and if the benchmark is bidirectional.
	bm, err := mqttbmlatency.Start(broker, "federator/topology_bm", 1, 64, 1, 1, true) // ,false) MORE LOGS

	if err != nil {
		return 10000, err
	}

	return bm.SubTotals.FwdLatencyMeanAvg, nil
}

// getNeighborsForNode retrieves the neighbors for a node.
// It retrieves the strong neighbor and the neighbor to redundancy.
// Returns a list of neighbors for the node.
func (n *NodeService) getNeighborsForNode(id int64) []models.Node {
	var neighbors []models.Node
	var neighbor models.Node
	var neighborRedundancy models.Node

	// Get the maximum redundancy allowed for the topology
	maxRedundancy, _ := strconv.ParseInt(os.Getenv("TOP_MAX_REDUNDANCY"), 10, 64)

	//Find the strong neighbor
	filter := bson.D{
		{Key: "id", Value: bson.D{{Key: "$ne", Value: id}}},
		{Key: "neighborsAmount", Value: bson.D{{Key: "$lt", Value: maxRedundancy}}},
	}
	// Sort by latency to get the neighbor with the lowest latency
	opts := options.FindOne().SetSort(bson.D{{Key: "latency", Value: 1}})
	// Find the neighbor with the lowest latency that has less than the maximum redundancy allowed
	err := n.Collection.FindOne(context.Background(), filter, opts).Decode(&neighbor)

	// err will be nil if a neighbor was found
	if err == nil {
		neighbors = append(neighbors, neighbor)
	}

	//Find the neighbor to redundancy
	filter = bson.D{
		{Key: "id", Value: bson.D{{Key: "$ne", Value: id}}},
		{Key: "id", Value: bson.D{{Key: "$ne", Value: neighbor.Id}}},
	}
	// Sort by the number of neighbors to get the neighbor with the lowest number of neighbors
	opts = options.FindOne().SetSort(bson.D{{Key: "neighborsAmount", Value: 1}})
	// Find the neighbor with the lowest number of neighbors
	err = n.Collection.FindOne(context.Background(), filter, opts).Decode(&neighborRedundancy)

	// err will be nil if a neighbor was found
	if err == nil {
		neighbors = append(neighbors, neighborRedundancy)
	}

	fmt.Println("Database neighbors for node", id, ":", neighbors)

	return neighbors
}

// updateNeighbors updates the neighbors of a given node with the provided list of neighbors and a new node.
// It appends the new node to the neighbors list of each existing neighbor, updates the neighborsAmount field of each existing neighbor,
// and sends a topology announcement to each existing neighbor to inform them about the addition of the new node.
// [neighbors]: A slice of models.Node representing the existing neighbors.
// [newNode]: A pointer to models.Node representing the new node to be added.
func (n *NodeService) updateNeighbors(neighbors []models.Node, newNode *models.Node) {
	fmt.Println("Updating neighbors for node ", newNode.Id)
	for _, neighbor := range neighbors {
		//update new node neighbors
		newNode.Neighbors = append(newNode.Neighbors, utils.NeighborConfig{Id: neighbor.Id, Ip: neighbor.Ip})
		newNode.NeighborsAmount += 1

		//adds the new node to neighbor already belonging to the topology
		filter := bson.D{{Key: "id", Value: neighbor.Id}}
		update := bson.M{
			"$set": bson.M{
				"neighborsAmount": neighbor.NeighborsAmount + 1,
			},
			"$push": bson.M{
				"neighbors": utils.NeighborConfig{Id: newNode.Id, Ip: newNode.Ip},
			},
		}

		// Update the neighbor with the new node information
		_, _ = n.Collection.UpdateOne(context.Background(), filter, update)

		// Create a new MQTT client (that will die) to send the topology announcement to the neighbor
		mqttClient, err := paho.NewClient(neighbor.Ip, "microservice")

		// If the connection to the neighbor was successful, send the topology announcement
		if err == nil {
			fmt.Println("Sending topology announcement from ", newNode.Id, "to neighbor NEW CALL", neighbor.Id)
			payload, _ := json.Marshal(utils.TopologyAnn{
				Action:   "NEW",
				Neighbor: utils.NeighborConfig{Id: newNode.Id, Ip: newNode.Ip},
			})

			fmt.Println("Encripting whit shared key", string(neighbor.SharedKey))
			encrypted, _ := keys.Encrypt(payload, neighbor.SharedKey)

			_, _ = mqttClient.Publish("federator/topology_ann", encrypted, 2, false)

			mqttClient.Disconnect()
		} else {
			fmt.Println("Error connecting to neighbor", neighbor.Ip)
			fmt.Println(err)
		}
	}
}

// NewNode creates a new node based on the provided data.
// It decodes the data into a Node struct, sets additional fields such as latency and latest health check,
// assigns a unique ID to the node, updates its neighbors if any, inserts the node into the collection,
// and returns a FederatorConfig containing the node's ID and neighbors.
// The function expects an io.ReadCloser containing the data to be decoded.
// It returns a pointer to a FederatorConfig and an error if any occurred during the process.
func (n *NodeService) NewNode(data io.ReadCloser) (*utils.FederatorConfig, error) {
	newNode := models.Node{
		NeighborsAmount: 0,
		Neighbors:       []utils.NeighborConfig{},
	}

	err := json.NewDecoder(data).Decode(&newNode)

	if err != nil {
		return nil, err
	}

	whitelist := strings.Split(os.Getenv("WHITELISTED_NODES"), ",")

	found := func(slice []string, value string) bool {
		for _, v := range slice {
			if v == value {
				return true
			}
		}
		return false
	}(whitelist, newNode.HardwareId)

	if !found {
		return nil, fmt.Errorf("node not whitelisted")
	}

	// Get the latency of the new node by sending a benchmark message to the broker
	newNode.Latency, _ = n.getNodeLatency(newNode.Ip)
	newNode.LatestHealthCheck = time.Now()

	newNode.Id = n.getNodeId()

	// Generate the ECDH key pair for the new node and the shared key with the existing node
	privateKey, publicKey, _ := keys.GenerateECDHKeyPair()
	otherKey, _ := keys.ConvertBytesToECDSAPublicKey(privateKey, newNode.PublicKey)

	newNode.SharedKey, _ = keys.GenerateSharedSecret(privateKey, otherKey)

	// Get the neighbors for the new node and update the neighbors of the existing neighbors
	neighbors := n.getNeighborsForNode(newNode.Id)

	// If the new node has neighbors, update the neighbors of the existing neighbors
	if len(neighbors) > 0 {
		n.updateNeighbors(neighbors, &newNode)
	}

	// Insert the new node into the collection
	_, err = n.Collection.InsertOne(context.Background(), newNode)

	if err != nil {
		return nil, err
	}

	client, err := paho.NewClient("tcp://localhost:1883", "topology"+strconv.FormatInt(newNode.Id, 10))

	if err != nil {
		return nil, err
	}

	fmt.Println("Created a Client for Node", newNode.Id)
	n.Client = client

	federatorConfig := utils.FederatorConfig{
		Id:        newNode.Id,
		Neighbors: newNode.Neighbors,
	}

	// Return the FederatorConfig containing the new node's ID and neighbors
	// This information will be used to configure the federator microservice
	federatorConfig.CoreAnnInterval, _ = time.ParseDuration(os.Getenv("CORE_ANN_INTERVAL"))
	federatorConfig.BeaconInterval, _ = time.ParseDuration(os.Getenv("BEACON_INTERVAL"))
	federatorConfig.Redundancy, _ = strconv.Atoi(os.Getenv("FED_REDUNDANCY"))
	federatorConfig.PublicKey = keys.ConvertECDSAPublicKeyToBytes(publicKey)

	n.ListenToNodeAnn(newNode)

	fmt.Println("NewNode with FederatorConfig: ", federatorConfig)

	return &federatorConfig, nil
}

func (n *NodeService) ListenToNodeAnn(node models.Node) {

	topics := map[string]byte{
		"federated/node_ann/" + strconv.FormatInt(node.Id, 10): 2,
	}

	messageHandler := func(client mqtt.Client, msg mqtt.Message) {
		payload, _ := keys.Decrypt(msg.Payload(), node.SharedKey)

		nodeAnn := utils.NodeAnn{}
		_ = json.Unmarshal(payload, &nodeAnn)

		if nodeAnn.Id != -1 {
			switch nodeAnn.Action {
			case "JOIN":
				err := n.AdmitNodeInTopic(nodeAnn)
				if err != nil {
					fmt.Println("Error admitting node", nodeAnn.Id, "in topic", nodeAnn.Topic)
					fmt.Println(err)
				}
			case "LEAVE":
				err := n.RemoveNodeInTopic(nodeAnn)
				if err != nil {
					fmt.Println("Error removing node", nodeAnn.Id, "from topic", nodeAnn.Topic)
					fmt.Println(err)
				}
			case "UPDATE_CORE":
				err := n.UpdateTopicCore(nodeAnn)
				if err != nil {
					fmt.Println("Error updating core for topic", nodeAnn.Topic)
					fmt.Println(err)
				}
			}
		}
	}

	_, err := n.Client.Consume(topics, messageHandler)

	if err != nil {
		fmt.Println("Error consuming messages for node", node.Id)
		fmt.Println(err)
	}
}

// AdmitNodeInTopic admits a node in a topic, updating the topic and sending the new password to the nodes.
// It retrieves the node from the collection and the topic from the topics collection.
// If the topic does not exist, it creates a new topic with the node as the core.
// It updates the topic with the new node and sends the new password to the nodes in the topic.
func (n *NodeService) AdmitNodeInTopic(msg utils.NodeAnn) error {
	node := models.Node{}
	err := n.Collection.FindOne(context.Background(), bson.D{{Key: "id", Value: bson.D{{Key: "$eq", Value: msg.Id}}}}).Decode(&node)

	if err != nil {
		return err
	}

	topic := models.Topic{}
	err = n.Topics.FindOne(context.Background(), bson.D{{Key: "topic", Value: bson.D{{Key: "$eq", Value: msg.Topic}}}}).Decode(&topic)

	// Isso não deve acontecer
	if err != nil {
		topic = models.Topic{
			Topic:    msg.Topic,
			Password: nil,
			Nodes:    []models.Node{node},
			Core:     node,
		}

		_, err := n.Topics.InsertOne(context.Background(), topic)

		if err != nil {
			return err
		}
	} else {
		n.Topics.FindOneAndUpdate(context.Background(), bson.D{{Key: "topic", Value: bson.D{{Key: "$eq", Value: msg.Topic}}}}, bson.M{
			"$push": bson.M{
				"nodes": node,
			},
		}).Decode(&topic)
	}

	// Se já tiver um topico, e ele já tem uma senha associada, posso enviar a senha de volta, se não, não envio
	if topic.Password != nil {
		decryptedPassword, _ := keys.Decrypt(topic.Password, topic.Core.SharedKey)
		payload, _ := json.Marshal(utils.NodeAnn{
			Id:       -1,
			Topic:    msg.Topic,
			Action:   "UPDATE_PASSWORD",
			Password: decryptedPassword,
		})

		encrypted, _ := keys.Encrypt(payload, node.SharedKey)
		topic := fmt.Sprintf("federated/node_ann/%d", node.Id)

		fmt.Println("NODES:", node)

		client, clientErr := paho.NewClient(node.Ip, "NodeService"+strconv.FormatInt(node.Id, 10))

		if clientErr != nil {
			return clientErr
		}

		_, clientErr = client.Publish(topic, encrypted, 2, false)

		if clientErr != nil {
			return clientErr
		}

		client.Disconnect()
	}

	if err != nil {
		return err
	}

	return nil
}

// Removes a node from a topic, updating the topic and sending the new password to the nodes
func (n *NodeService) RemoveNodeInTopic(msg utils.NodeAnn) error {
	node := models.Node{}
	err := n.Collection.FindOne(context.Background(), bson.D{{Key: "id", Value: bson.D{{Key: "$eq", Value: msg.Id}}}}).Decode(&node)

	if err != nil {
		return err
	}

	topic := models.Topic{}
	err = n.Topics.FindOne(context.Background(), bson.D{{Key: "topic", Value: bson.D{{Key: "$eq", Value: msg.Topic}}}}).Decode(&topic)
	if err != nil {
		return err
	}

	n.Topics.FindOneAndUpdate(context.Background(), bson.D{{Key: "topic", Value: bson.D{{Key: "$eq", Value: msg.Topic}}}}, bson.M{
		"$pull": bson.M{
			"nodes": bson.M{"id": node.Id},
		},
	}).Decode(&topic)

	password := keys.GenerateSessionKey(msg.Topic)
	encryptedPassword, _ := keys.Encrypt(password, topic.Core.SharedKey)
	// Devo criptografar com o que essa senha? Não sei quem é o core ainda (talvez)
	// Acho que não tem problema porque se criptografar com qualquer merda, vou receber uma atualização de core e atualizar a senha
	// Seguindo o caminho certo

	n.Topics.FindOneAndUpdate(context.Background(), bson.D{{Key: "topic", Value: bson.D{{Key: "$eq", Value: msg.Topic}}}}, bson.M{
		"$set": bson.M{
			"password": encryptedPassword,
		},
	}).Decode(&topic)

	decryptedPassword, _ := keys.Decrypt(topic.Password, topic.Core.SharedKey)
	payload, _ := json.Marshal(utils.NodeAnn{
		Id:       -1,
		Topic:    msg.Topic,
		Action:   "UPDATE_PASSWORD",
		Password: decryptedPassword,
	})

	for _, topicNode := range topic.Nodes {
		encrypted, _ := keys.Encrypt(payload, topicNode.SharedKey)
		topic := fmt.Sprintf("federated/node_ann/%d", topicNode.Id)

		client, clientErr := paho.NewClient(topicNode.Ip, "NodeService")

		if clientErr != nil {
			return clientErr
		}

		_, clientErr = client.Publish(topic, encrypted, 2, false)

		if clientErr != nil {
			return clientErr
		}

		client.Disconnect()
	}

	return nil
}

func (n *NodeService) UpdateTopicCore(msg utils.NodeAnn) error {
	topicCore := models.Node{}
	err := n.Collection.FindOne(context.Background(), bson.D{{Key: "id", Value: bson.D{{Key: "$eq", Value: msg.Id}}}}).Decode(&topicCore)

	if err != nil {
		return err
	}

	topic := models.Topic{}
	err = n.Topics.FindOne(context.Background(), bson.D{{Key: "topic", Value: bson.D{{Key: "$eq", Value: msg.Topic}}}}).Decode(&topic)

	password := keys.GenerateSessionKey(msg.Topic)
	encryptedPassword, _ := keys.Encrypt(password, topicCore.SharedKey)

	if err != nil {
		fmt.Println("Topic not found, creating new topic", err, topic)

		topic = models.Topic{
			Topic:    msg.Topic,
			Password: encryptedPassword,
			Nodes:    []models.Node{topicCore},
			Core:     topicCore,
		}

		_, err := n.Topics.InsertOne(context.Background(), topic)

		if err != nil {
			return err
		}
	} else {
		n.Topics.FindOneAndUpdate(context.Background(), bson.D{{Key: "topic", Value: bson.D{{Key: "$eq", Value: msg.Topic}}}}, bson.M{
			"$set": bson.M{
				"password": encryptedPassword,
				"core":     topicCore,
			},
			"$push": bson.M{
				"nodes": topicCore,
			},
		}).Decode(&topic)
	}

	decryptedPassword, _ := keys.Decrypt(topic.Password, topic.Core.SharedKey)
	payload, _ := json.Marshal(utils.NodeAnn{
		Id:       -1,
		Topic:    msg.Topic,
		Action:   "UPDATE_PASSWORD",
		Password: decryptedPassword,
	})

	for _, topicNode := range topic.Nodes {
		encrypted, _ := keys.Encrypt(payload, topicNode.SharedKey)
		topic := fmt.Sprintf("federated/node_ann/%d", topicNode.Id)

		client, clientErr := paho.NewClient(topicNode.Ip, "NodeService")

		if clientErr != nil {
			return clientErr
		}

		_, clientErr = client.Publish(topic, encrypted, 2, false)

		if clientErr != nil {
			return clientErr
		}

		client.Disconnect()
	}

	if err != nil {
		return err
	}

	return nil
}

// MonitorTopologyHealth periodically checks the health of the topology nodes.
// It updates the latest health check timestamp for all nodes and removes offline nodes.
// If a node is considered offline (no health check for 10 seconds), it is removed from the collection.
// Additionally, it updates the neighbors of the offline node and sends topology announcements to the neighbors.
// If a neighbor has no more neighbors, it retrieves new neighbors and updates the neighbor's information.
// This function runs as a cron job every 5 seconds.
//
// Returns an error if there was a problem executing the monitoring process.
func (n *NodeService) MonitorTopologyHealth() error {
	//update all nodes to avoid removing nodes in case the microservice goes offline
	update := bson.M{
		"$set": bson.M{
			"latestHealthCheck": time.Now(),
		},
	}

	// Update the latest health check timestamp for all nodes in the collection
	_, _ = n.Collection.UpdateMany(context.Background(), bson.D{{}}, update)

	//cron function that execute every 5 seconds
	s := gocron.NewScheduler(time.UTC)

	// Schedule the monitoring process to run every 5 seconds using the gocron library
	// The monitoring process checks the health of the topology nodes and removes offline nodes
	_, err := s.Every(5).Seconds().Do(func() {
		// Find all nodes in the collection to check their health
		cur, _ := n.Collection.Find(context.Background(), bson.D{{}})

		var nodes []models.Node

		// Iterate over all nodes in the collection to check their health
		for cur.Next(context.TODO()) {
			var node models.Node
			err := cur.Decode(&node)
			if err != nil {
				log.Fatal(err)
			}

			//
			nodes = append(nodes, node)
		}

		// Iterate over all nodes to check their health and remove offline nodes
		for _, node := range nodes {
			// Run a goroutine to check the health of each node in *parallel* to speed up the process
			go func(node models.Node) {
				latency, err := n.getNodeLatency(node.Ip)

				if err == nil {
					filter := bson.D{{Key: "_id", Value: node.ObjectId}}
					update := bson.M{
						"$set": bson.M{
							"latestHealthCheck": time.Now(),
							"latency":           latency,
						},
					}
					// Update the latest health check timestamp and latency of the node
					_, _ = n.Collection.UpdateOne(context.Background(), filter, update)
				} else {
					// If there was an error retrieving the latency, consider the node offline
					elapsed := time.Since(node.LatestHealthCheck)

					// if 10 seconds pass since the last health check, the federator is considered offline and will be removed
					if elapsed > time.Second*10 {
						fmt.Println("Federator", node.Id, "offline, will be removed!")

						// Remove the offline node from the collection using its ObjectId
						_, _ = n.Collection.DeleteOne(context.Background(), bson.D{{Key: "_id", Value: node.ObjectId}})

						// Remove offline node from the topics
						_, _ = n.Topics.UpdateMany(
							context.Background(),
							bson.D{{
								Key: "nodes._id", Value: node.ObjectId,
							}},
							bson.M{
								"$pull": bson.M{
									"nodes": bson.M{"_id": node.ObjectId},
								},
								"$set": bson.M{
									"password": nil,
								},
							},
						)

						// Remove offline nodes from the topics core
						_, _ = n.Topics.UpdateMany(context.Background(), bson.D{{Key: "core.id", Value: bson.D{{Key: "$eq", Value: node.Id}}}}, bson.M{
							"$set": bson.M{
								"core": bson.M{},
							},
						})

						// If the offline node has neighbors, update the neighbors of the offline node
						for _, neighbor := range node.Neighbors {

							//remove offline node from neighbors
							filter := bson.D{{Key: "id", Value: neighbor.Id}}
							update := bson.M{
								"$inc": bson.M{
									"neighborsAmount": -1,
								},
								"$pull": bson.M{
									"neighbors": bson.M{"id": node.Id},
								},
							}
							opts := options.FindOneAndUpdate().SetReturnDocument(options.After)

							var updatedNeighbor models.Node

							// Update the neighbors of the offline node and get the updated neighbor information
							_ = n.Collection.FindOneAndUpdate(context.Background(), filter, update, opts).Decode(&updatedNeighbor)

							// If the neighbor of the offline node has neighbors, send topology announcements to inform them about the removal of the offline node
							mqttClient, err := paho.NewClient(neighbor.Ip, "microservice")

							if err == nil {
								//send topology ann to neighbor informing the remove of offline node
								fmt.Println("Sending topology announcement to neighbor REMOVE CALL", node.Ip)
								payload, _ := json.Marshal(utils.TopologyAnn{
									Action:   "REMOVE",
									Neighbor: utils.NeighborConfig{Id: node.Id, Ip: node.Ip},
								})

								fmt.Println("Encripting whit shared key", string(updatedNeighbor.SharedKey))
								encrypted, _ := keys.Encrypt(payload, updatedNeighbor.SharedKey)

								_, _ = mqttClient.Publish("federator/topology_ann", encrypted, 2, false)

								// If the neighbor of offline node has no more neighbors, get new neighbors
								if updatedNeighbor.NeighborsAmount <= 0 {
									updatedNeighbor.NeighborsAmount = 0
									newNeighbors := n.getNeighborsForNode(updatedNeighbor.Id)

									// If the neighbor has new neighbors, update the neighbor's information and send topology announcements
									if len(newNeighbors) > 0 {
										n.updateNeighbors(newNeighbors, &updatedNeighbor)

										filter := bson.D{{Key: "id", Value: updatedNeighbor.Id}}
										update := bson.M{
											"$set": bson.M{
												"neighborsAmount": updatedNeighbor.NeighborsAmount,
												"neighbors":       updatedNeighbor.Neighbors,
											},
										}
										_, _ = n.Collection.UpdateOne(context.Background(), filter, update)

										for _, newNeighbor := range newNeighbors {
											fmt.Println("New neighbor", newNeighbor.Id, "added to neighbor", updatedNeighbor.Id)

											payload, _ := json.Marshal(utils.TopologyAnn{
												Action:   "NEW",
												Neighbor: utils.NeighborConfig{Id: newNeighbor.Id, Ip: newNeighbor.Ip},
											})

											fmt.Println("Encripting whit shared key", string(updatedNeighbor.SharedKey))
											encrypted, _ := keys.Encrypt(payload, updatedNeighbor.SharedKey)

											_, _ = mqttClient.Publish("federator/topology_ann", encrypted, 2, false)
										}
									}
								} else {
									fmt.Println("Neighbor", neighbor.Id, "has no more neighbors")
								}

								mqttClient.Disconnect()
							} else {
								fmt.Println("Error connecting to neighbor", neighbor.Id)
							}
						}
					}
				}
			}(node)
		}
	})

	if err != nil {
		return err
	}

	s.StartAsync()

	return nil
}
