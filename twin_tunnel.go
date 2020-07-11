package marina

import (
	rpc "github.com/TheSmallBoat/carlo/streaming_rpc"
)

type twinTunnel struct {
	tw       *twin
	provider *rpc.Provider
}

func newTwinTunnel(tw *twin, pvd *rpc.Provider) *twinTunnel {
	if tw.kadId == pvd.KadID() {
		return &twinTunnel{tw, pvd}
	}
	return nil
}
