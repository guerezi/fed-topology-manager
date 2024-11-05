package models

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Topic struct
// This struct represents a topic in the network
// It has an ObjectId, a Topic, a Password, a list of Nodes and a Core
// The [ObjectId] is the unique identifier of the topic in the database
// The [Topic] is the name of the topic
// The [Password] is the password of the topic
// The [Nodes] is a list of Node structs, which contains the Id, the Ip, the Neighbors, the NeighborsAmount, the Latency and the LatestHealthCheck of the node
// The [Core] is a Node struct for the core of the topic, which contains the Id, the Ip, the Neighbors, the NeighborsAmount, the Latency and the LatestHealthCheck of the core node
type Topic struct {
	ObjectId primitive.ObjectID `bson:"_id,omitempty" json:"_id"`
	Topic    string             `bson:"topic" json:"topic"`
	Password []byte             `bson:"password" json:"password"`
	Nodes    []Node             `bson:"nodes" json:"nodes"`
	Core     Node               `bson:"core" json:"core"`
}

// {
// 	"ObjectId": "5f3b0b3b9b3b3b3b3b3b3b3b",
// 	"topic": "topic1",
// 	"password": "password1", (cryptographically stored)
// 	"core": {
// 		"Id": 1,
// 		"Ip": "",
// 		"Neighbors": [...],
// 		...
// 	}
// 	"nodes": [
// 		{
// 			"Id": 1,
// 			"Ip": "",
// 			"Neighbors": [...],
// 			"NeighborsAmount": 0,
// 			"Latency": 0,
// 			"LatestHealthCheck": "2020-08-18T00:00:00Z"
// 		},
// 		...
// 	],
// }
