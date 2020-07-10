package marina

import (
	rpc "github.com/TheSmallBoat/carlo/streaming_rpc"
)

type twinTunnel struct {
	tw       *twin
	provider *rpc.Provider
}

func newTwinTunnel(tw *twin, prd *rpc.Provider) *twinTunnel {
	return &twinTunnel{}
}
