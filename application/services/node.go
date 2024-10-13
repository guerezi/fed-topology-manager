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

	"github.com/go-co-op/gocron"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type NodeService struct {
	Collection *mongo.Collection
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

	return &NodeService{
		Collection: collection,
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

	// TODO: theses keys would problably be stored in a secure place
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

	federatorConfig := utils.FederatorConfig{
		Id:        newNode.Id,
		Neighbors: newNode.Neighbors,
	}

	// Return the FederatorConfig containing the new node's ID and neighbors
	// This information will be used to configure the federator microservice
	federatorConfig.CoreAnnInterval, _ = time.ParseDuration(os.Getenv("CORE_ANN_INTERVAL"))
	federatorConfig.BeaconInterval, _ = time.ParseDuration(os.Getenv("BEACON_INTERVAL"))
	federatorConfig.Redundancy, _ = strconv.Atoi(os.Getenv("FED_REDUNDANCY"))
	federatorConfig.SharedKey = newNode.SharedKey // TODO: Should not send the shared key in the response!!
	federatorConfig.PublicKey = keys.ConvertECDSAPublicKeyToBytes(publicKey)

	fmt.Println("NewNode with FederatorConfig: ", federatorConfig)

	return &federatorConfig, nil
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
