package models

import (
	"time"
	"topology/application/utils"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Node struct
// This struct represents a node in the network
// It has an ObjectId, an Id, an Ip, a list of Neighbors, the amount of Neighbors, the Latency and the LatestHealthCheck
// The [ObjectId] is the unique identifier of the node in the database
// The [Id] is the identifier of the node in the network
// The [Ip] is the IP address of the node
// The [Neighbors] is a list of NeighborConfig structs, which contains the Id and the Latency of the neighbor
// The [NeighborsAmount] is the amount of neighbors the node has
// The [Latency] is the latency of the node
// The [LatestHealthCheck] is the time of the last health check of the node
type Node struct {
	ObjectId          primitive.ObjectID     `bson:"_id,omitempty" json:"_id"`
	Id                int64                  `bson:"id" json:"id"`
	Ip                string                 `bson:"ip" json:"ip"`
	Neighbors         []utils.NeighborConfig `bson:"neighbors" json:"neighbors"`
	NeighborsAmount   int                    `bson:"neighborsAmount" json:"neighborsAmount"`
	Latency           float64                `bson:"latency" json:"latency"`
	LatestHealthCheck time.Time              `bson:"latestHealthCheck" json:"latestHealthCheck"`
	PublicKey         []byte                 `bson:"publicKey" json:"publicKey"`
	SharedKey         []byte                 `bson:"sharedKey" json:"sharedKey"`
	HardwareId        string                 `bson:"hardwareId" json:"hardwareId"`
}

//  {
// 	ObjectID: '5f8b3b3b1c9d440000f1b3b3',
// 	Id: 1,
// 	Ip: 'tcp://mqtt-fed-0:1883',
// 	Neighbors: [
// 		{
// 			Id: 2,
// 			Ip: 'tcp://mqtt-fed-1:1883'
// 		},
// 	],
// 	NeighborsAmount: 1,
// 	Latency: 0.0,
// 	LatestHealthCheck: '2020-10-17T00:00:00Z',
// 	PublicKey: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
// 	SharedKey: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
// 	HardwareId: '1234567890'
//  }
