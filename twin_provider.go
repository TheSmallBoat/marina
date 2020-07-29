package marina

import "github.com/lithdew/kademlia"

// The remote service provider for the twin.
type TwinServiceProvider interface {
	KadID() *kademlia.ID
	Push(data []byte) error
}
