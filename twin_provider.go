package marina

import "github.com/lithdew/kademlia"

// The remote service provider for the twin.
type twinServiceProvider interface {
	KadID() *kademlia.ID
}
