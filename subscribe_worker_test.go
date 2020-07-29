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
	defer func() {
		err := tt.Close()
		require.NoError(t, err)
	}()

	twp := NewTwinsPool()
	defer twp.Close()

	twn, pdn := twp.length()
	require.Equal(t, 0, twn)
	require.Equal(t, 0, len(twp.mpt))
	require.Equal(t, 0, pdn)
	require.Equal(t, 0, len(twp.mpp))
	require.Empty(t, twp.mpt)

	sw := NewSubscribeWorker(twp, tt)
	defer sw.Close()

	kid1, err1 := generateKadId()
	require.NoError(t, err1)

	var prd1 TwinServiceProvider = &provider{kadId: kid1}
	require.Equal(t, kid1, prd1.KadID())

	_ = twp.acquire(&prd1)

	kid2, err2 := generateKadId()
	require.NoError(t, err2)

	var prd2 TwinServiceProvider = &provider{kadId: kid2}
	require.Equal(t, kid2, prd2.KadID())

	kid3, err3 := generateKadId()
	require.NoError(t, err3)

	var prd3 TwinServiceProvider = &provider{kadId: kid3}
	require.Equal(t, kid3, prd3.KadID())

	kid4, err4 := generateKadId()
	require.NoError(t, err4)

	var prd4 TwinServiceProvider = &provider{kadId: kid4}
	require.Equal(t, kid4, prd4.KadID())

	sw.PeerNodeSubscribe(&prd1, byte(0), []byte("/finance/tom"))
	sw.PeerNodeSubscribe(&prd2, byte(0), []byte("/finance/jack"))
	sw.PeerNodeSubscribe(&prd3, byte(1), []byte("/finance/#"))

	sw.Wait()
	require.Equal(t, uint32(3), sw.subSucNum)

	entities := make([]interface{}, 0)
	err := sw.tt.LinkedEntities([]byte("/finance/#"), &entities)
	require.NoError(t, err)
	require.Equal(t, 1, len(entities))
	twe := entities[0].(*twin)
	require.Equal(t, twp.acquire(&prd3), twe)

	entities = entities[0:0]
	err = sw.tt.LinkedEntities([]byte("/finance/tom"), &entities)
	require.NoError(t, err)
	require.Equal(t, 2, len(entities))

	i := 0
	for _, e := range entities {
		te := e.(*twin)
		if te == twp.acquire(&prd3) || te == twp.acquire(&prd1) {
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
		if te == twp.acquire(&prd3) || te == twp.acquire(&prd2) {
			i++
		}
	}
	require.Equal(t, 2, i)

	sw.PeerNodeUnSubscribe(kid3.Pub, byte(1), []byte("/finance/#"))
	sw.Wait()
	require.Equal(t, uint32(1), sw.unSubSucNum)

	entities = entities[0:0]
	err = sw.tt.LinkedEntities([]byte("/finance/tom"), &entities)
	require.NoError(t, err)
	require.Equal(t, 1, len(entities))
	twe = entities[0].(*twin)
	require.Equal(t, twp.acquire(&prd1), twe)

	sw.PeerNodeUnSubscribe(kid1.Pub, byte(1), []byte("/finance/tom/jack"))
	sw.PeerNodeUnSubscribe(kid4.Pub, byte(0), []byte("/finance/tom/jack"))
	sw.Wait()
	require.Equal(t, uint32(2), sw.unSubErrNum)

	sw.PeerNodeUnSubscribe(kid1.Pub, byte(1), []byte("/finance/tom"))
	sw.Wait()
	require.Equal(t, uint32(2), sw.unSubSucNum)

	entities = entities[0:0]
	err = sw.tt.LinkedEntities([]byte("/finance/tom"), &entities)
	require.NoError(t, err)
	require.Equal(t, 0, len(entities))

	sw.PeerNodeSubscribe(&prd1, byte(0), []byte("/finance/1/tom"))
	sw.PeerNodeSubscribe(&prd2, byte(0), []byte("/finance/1/jack"))
	sw.PeerNodeSubscribe(&prd3, byte(1), []byte("/finance/1/#"))
	sw.PeerNodeSubscribe(&prd1, byte(0), []byte("/finance/2/tom"))
	sw.PeerNodeSubscribe(&prd2, byte(0), []byte("/finance/2/jack"))
	sw.PeerNodeSubscribe(&prd3, byte(1), []byte("/finance/2/#"))
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
	require.Equal(t, twp.acquire(&prd3), twe)

	sw.PeerNodeSubscribe(&prd1, byte(0), []byte("/finance/#/tom"))
	sw.PeerNodeSubscribe(&prd2, byte(0), []byte("/finance/+/jack"))
	sw.Wait()
	require.Equal(t, uint32(10), sw.subSucNum)
	require.Equal(t, uint32(1), sw.subErrNum)

	sw.PeerNodeUnSubscribe(kid1.Pub, byte(0), []byte("/finance/#/tom"))
	sw.PeerNodeUnSubscribe(kid2.Pub, byte(0), []byte("/finance/+/jack"))
	sw.PeerNodeUnSubscribe(kid3.Pub, byte(0), []byte("/finance/+/jack"))
	sw.Wait()
	require.Equal(t, uint32(3), sw.unSubSucNum)
	require.Equal(t, uint32(4), sw.unSubErrNum)
}
