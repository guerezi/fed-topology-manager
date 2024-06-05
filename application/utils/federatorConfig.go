package utils

import "time"

type NeighborConfig struct {
	Id int64
	Ip string
}

type FederatorConfig struct {
	Id              int64            `json:"id"`
	Host            string           `json:"ip"`
	Neighbors       []NeighborConfig `json:"neighbors"`
	Redundancy      int              `json:"redundancy"`
	CoreAnnInterval time.Duration    `json:"coreAnnInterval"`
	BeaconInterval  time.Duration    `json:"beaconInterval"`
}
