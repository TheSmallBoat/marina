package marina

import (
	"testing"

	"github.com/TheSmallBoat/cabinet"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestSubscribeWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	tt := cabinet.NewTopicTree()

	twp := newTwinsPool()
	require.Equal(t, int(0), twp.length())
	require.Empty(t, twp.mp)

	sw := NewSubscribeWorker(twp, tt)
	defer sw.Close()

	kid1, err1 := generateKadId()
	require.NoError(t, err1)
	_ = twp.acquire(kid1)

	kid2, err2 := generateKadId()
	require.NoError(t, err2)

	kid3, err3 := generateKadId()
	require.NoError(t, err3)

	kid4, err4 := generateKadId()
	require.NoError(t, err4)

	sw.peerNodeSubscribe(kid1, byte(0), []byte("/finance/tom"))
	sw.peerNodeSubscribe(kid2, byte(0), []byte("/finance/jack"))
	sw.peerNodeSubscribe(kid3, byte(1), []byte("/finance/#"))

	sw.Wait()
	require.Equal(t, uint32(3), sw.subSucNum)

	entities := make([]interface{}, 0)
	err := sw.tt.LinkedEntities([]byte("/finance/#"), &entities)
	require.NoError(t, err)
	require.Equal(t, 1, len(entities))
	twe := entities[0].(*twin)
	require.Equal(t, twp.acquire(kid3), twe)

	entities = entities[0:0]
	err = sw.tt.LinkedEntities([]byte("/finance/tom"), &entities)
	require.NoError(t, err)
	require.Equal(t, 2, len(entities))

	i := 0
	for _, e := range entities {
		te := e.(*twin)
		if te == twp.acquire(kid3) || te == twp.acquire(kid1) {
			i++
		}
	}
	require.Equal(t, 2, i)

	entities = entities[0:0]
	err = sw.tt.LinkedEntities([]byte("/finance/jack"), &entities)
	require.NoError(t, err)
	require.Equal(t, 2, len(entities))

	i = 0
	for _, e := range entities {
		te := e.(*twin)
		if te == twp.acquire(kid3) || te == twp.acquire(kid2) {
			i++
		}
	}
	require.Equal(t, 2, i)

	sw.peerNodeUnSubscribe(kid3, byte(1), []byte("/finance/#"))
	sw.Wait()
	require.Equal(t, uint32(1), sw.unSubSucNum)

	entities = entities[0:0]
	err = sw.tt.LinkedEntities([]byte("/finance/tom"), &entities)
	require.NoError(t, err)
	require.Equal(t, 1, len(entities))
	twe = entities[0].(*twin)
	require.Equal(t, twp.acquire(kid1), twe)

	sw.peerNodeUnSubscribe(kid1, byte(1), []byte("/finance/tom/jack"))
	sw.peerNodeUnSubscribe(kid4, byte(0), []byte("/finance/tom/jack"))
	sw.Wait()
	require.Equal(t, uint32(2), sw.unSubErrNum)

	sw.peerNodeUnSubscribe(kid1, byte(1), []byte("/finance/tom"))
	sw.Wait()
	require.Equal(t, uint32(2), sw.unSubSucNum)

	entities = entities[0:0]
	err = sw.tt.LinkedEntities([]byte("/finance/tom"), &entities)
	require.NoError(t, err)
	require.Equal(t, 0, len(entities))

	sw.peerNodeSubscribe(kid1, byte(0), []byte("/finance/1/tom"))
	sw.peerNodeSubscribe(kid2, byte(0), []byte("/finance/1/jack"))
	sw.peerNodeSubscribe(kid3, byte(1), []byte("/finance/1/#"))
	sw.peerNodeSubscribe(kid1, byte(0), []byte("/finance/2/tom"))
	sw.peerNodeSubscribe(kid2, byte(0), []byte("/finance/2/jack"))
	sw.peerNodeSubscribe(kid3, byte(1), []byte("/finance/2/#"))
	sw.Wait()
	require.Equal(t, uint32(9), sw.subSucNum)

	entities = entities[0:0]
	err = sw.tt.LinkedEntities([]byte("/finance/#/tom"), &entities)
	require.Error(t, err)

	entities = entities[0:0]
	err = sw.tt.LinkedEntities([]byte("/finance/1/tom"), &entities)
	require.NoError(t, err)
	require.Equal(t, 2, len(entities))

	entities = entities[0:0]
	err = sw.tt.LinkedEntities([]byte("/finance/2/+"), &entities)
	require.NoError(t, err)
	require.Equal(t, 1, len(entities))
	twe = entities[0].(*twin)
	require.Equal(t, twp.acquire(kid3), twe)

	sw.peerNodeSubscribe(kid1, byte(0), []byte("/finance/#/tom"))
	sw.peerNodeSubscribe(kid2, byte(0), []byte("/finance/+/jack"))
	sw.Wait()
	require.Equal(t, uint32(10), sw.subSucNum)
	require.Equal(t, uint32(1), sw.subErrNum)

	sw.peerNodeUnSubscribe(kid1, byte(0), []byte("/finance/#/tom"))
	sw.peerNodeUnSubscribe(kid2, byte(0), []byte("/finance/+/jack"))
	sw.peerNodeUnSubscribe(kid3, byte(0), []byte("/finance/+/jack"))
	sw.Wait()
	require.Equal(t, uint32(3), sw.unSubSucNum)
	require.Equal(t, uint32(4), sw.unSubErrNum)
}
