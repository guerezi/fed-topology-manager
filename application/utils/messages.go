package utils

type TopologyAnn struct {
	Neighbor  NeighborConfig `json:"neighbor"`
	Action    string         `json:"action"`
	PublicKey string         `json:"publicKey"`
}
