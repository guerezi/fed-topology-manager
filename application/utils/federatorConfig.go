package utils

import "time"

type NodeAnn struct {
	Id       int64  `json:"id"`
	Topic    string `json:"topic"`
	Password []byte `json:"password"`
	Action   string `json:"action"`
}

type NeighborConfig struct {
	Id int64  `json:"id"`
	Ip string `json:"ip"`
}

type FederatorConfig struct {
	Id              int64            `json:"id"`
	Host            string           `json:"ip"`
	Neighbors       []NeighborConfig `json:"neighbors"`
	Redundancy      int              `json:"redundancy"`
	CoreAnnInterval time.Duration    `json:"coreAnnInterval"`
	BeaconInterval  time.Duration    `json:"beaconInterval"`
	PublicKey       []byte           `json:"publicKey"`
}
