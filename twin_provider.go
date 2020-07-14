package marina

import "github.com/lithdew/kademlia"

type twinServiceProvider interface {
	KadID() *kademlia.ID
}
