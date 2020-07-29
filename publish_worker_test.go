package marina

import (
	"testing"
	"time"

	"github.com/TheSmallBoat/cabinet"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestPublishWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	tt := cabinet.NewTopicTree()
	defer func() {
		err := tt.Close()
		require.NoError(t, err)
	}()

	pKid, err1 := generateKadId()
	require.NoError(t, err1)
	bKid, err2 := generateKadId()
	require.NoError(t, err2)
	sKid, err3 := generateKadId()
	require.NoError(t, err3)

	twp := NewTwinsPool()
	defer twp.close()

	twn, pdn := twp.length()
	require.Equal(t, 0, twn)
	require.Equal(t, 0, len(twp.mpt))
	require.Equal(t, 0, pdn)
	require.Equal(t, 0, len(twp.mpp))
	require.Empty(t, twp.mpt)

	var prd TwinServiceProvider = &provider{kadId: sKid}
	require.Equal(t, sKid, prd.KadID())

	tw := twp.acquire(&prd)
	err4 := tt.EntityLink([]byte("/finance/tom"), tw)
	require.NoError(t, err4)
	require.Equal(t, sKid, (*tw.prd).KadID())
	require.Equal(t, true, tw.online)

	pw := NewPublishWorker(bKid, tt)
	defer pw.Close()

	entities := make([]interface{}, 0)
	entities = pw.EntitiesFor([]byte("/finance/tom"))
	require.Equal(t, 1, len(entities))
	twee := entities[0].(*twin)
	require.Equal(t, tw, twee)

	entities = pw.EntitiesFor([]byte("/finance/jack"))
	require.Equal(t, true, entities == nil)

	num := pw.EntitiesNumFor([]byte("/finance/tom"))
	require.Equal(t, 1, num)

	num = pw.EntitiesNumFor([]byte("/finance/jack"))
	require.Equal(t, 0, num)

	pkt := NewMessagePacket(pKid, uint32(88), byte(0), []byte("/finance/tom"), []byte("xyz123456abc"))
	pw.WorkFor(pkt)
	pw.Wait()

	require.Equal(t, uint32(1), twp.acquire(&prd).pushSucNum)

	require.Equal(t, uint32(1), pw.pubSucNum)
	require.Equal(t, uint32(1), pw.fwdSucNum)
	require.Equal(t, uint32(0), pw.pubErrNum)
	require.Equal(t, uint32(0), pw.fwdErrNum)

	require.Equal(t, pKid, pkt.pubKadId)
	require.Equal(t, bKid, pkt.brkKadId)
	require.Equal(t, sKid, pkt.subKadId)

	time.Sleep(5 * time.Millisecond)

	require.Equal(t, uint32(1), twp.acquire(&prd).transSucNum+twp.acquire(&prd).transErrNum)

	dst := make([]byte, 0)
	pkt.SetSubscriberKadId(sKid)
	require.Equal(t, uint64(len(pkt.AppendTo(dst))), twp.acquire(&prd).transSucSize+twp.acquire(&prd).transErrSize)

	pkt = NewMessagePacket(pKid, uint32(89), byte(0), []byte("/finance/jack"), []byte("xyz123456abc"))
	pw.WorkFor(pkt)
	pw.Wait()

	require.Equal(t, uint32(1), pw.pubSucNum)
	require.Equal(t, uint32(1), pw.fwdSucNum)
	require.Equal(t, uint32(1), pw.pubErrNum)
	require.Equal(t, uint32(0), pw.fwdErrNum)

	pkt = NewMessagePacket(pKid, uint32(90), byte(1), []byte("/finance/tom"), []byte("xyz123456abc.."))
	pw.WorkFor(pkt)
	pw.Wait()

	require.Equal(t, uint32(2), twp.acquire(&prd).pushSucNum)

	require.Equal(t, uint32(2), pw.pubSucNum)
	require.Equal(t, uint32(2), pw.fwdSucNum)
	require.Equal(t, uint32(1), pw.pubErrNum)
	require.Equal(t, uint32(0), pw.fwdErrNum)

	require.Equal(t, pKid, pkt.pubKadId)
	require.Equal(t, bKid, pkt.brkKadId)
	require.Equal(t, sKid, pkt.subKadId)

	err6 := tt.LinkedEntities([]byte("/finance/tom"), &entities)
	require.NoError(t, err6)
	require.Equal(t, 1, len(entities))
	twe := entities[0].(*twin)
	require.Equal(t, tw, twe)
	require.Equal(t, twp.acquire(&prd), twe)
	require.Equal(t, true, tw.online)
	require.Equal(t, true, twe.online)

	twe.turnToOffline()
	require.Equal(t, false, twe.online)
	require.Equal(t, false, tw.online)

	pkt = NewMessagePacket(pKid, uint32(91), byte(0), []byte("/finance/tom"), []byte("x123456abc..."))
	pw.WorkFor(pkt)
	pw.Wait()

	require.Equal(t, uint32(3), pw.pubSucNum)
	require.Equal(t, uint32(2), pw.fwdSucNum)
	require.Equal(t, uint32(1), pw.pubErrNum)
	require.Equal(t, uint32(1), pw.fwdErrNum)

	err7 := tt.EntityUnLink([]byte("/finance/tom"), twp.acquire(&prd))
	require.NoError(t, err7)

	pkt = NewMessagePacket(pKid, uint32(92), byte(1), []byte("/finance/tom"), []byte("123456abc"))
	pw.WorkFor(pkt)
	pw.Wait()

	require.Equal(t, uint32(3), pw.pubSucNum)
	require.Equal(t, uint32(2), pw.fwdSucNum)
	require.Equal(t, uint32(2), pw.pubErrNum)
	require.Equal(t, uint32(1), pw.fwdErrNum)

	// Still equal 2 due to the failure of pushing
	require.Equal(t, uint32(2), twp.acquire(&prd).pushSucNum)
}

func TestPublishWorkerForMultipleSubscribe(t *testing.T) {
	defer goleak.VerifyNone(t)

	tt := cabinet.NewTopicTree()
	defer func() {
		err := tt.Close()
		require.NoError(t, err)
	}()

	pKid, err1 := generateKadId()
	require.NoError(t, err1)
	bKid, err2 := generateKadId()
	require.NoError(t, err2)

	sKidA, err3 := generateKadId()
	require.NoError(t, err3)
	sKidB, err4 := generateKadId()
	require.NoError(t, err4)
	sKidC, err5 := generateKadId()
	require.NoError(t, err5)

	twp := NewTwinsPool()
	defer twp.close()

	twn, pdn := twp.length()
	require.Equal(t, 0, twn)
	require.Equal(t, 0, len(twp.mpt))
	require.Equal(t, 0, pdn)
	require.Equal(t, 0, len(twp.mpp))
	require.Empty(t, twp.mpt)
	require.Empty(t, twp.mpp)

	// section A:
	var prdA TwinServiceProvider = &provider{kadId: sKidA}
	require.Equal(t, sKidA, prdA.KadID())

	twA := twp.acquire(&prdA)
	err6 := tt.EntityLink([]byte("/finance/tom"), twA)
	require.NoError(t, err6)
	require.Equal(t, sKidA, (*twA.prd).KadID())
	require.Equal(t, true, twA.online)

	// section B:
	var prdB TwinServiceProvider = &provider{kadId: sKidB}
	require.Equal(t, sKidB, prdB.KadID())

	twB := twp.acquire(&prdB)
	err7 := tt.EntityLink([]byte("/finance/tom"), twB)
	require.NoError(t, err7)
	require.Equal(t, sKidB, (*twB.prd).KadID())
	require.Equal(t, true, twB.online)

	// section C:
	var prdC TwinServiceProvider = &provider{kadId: sKidC}
	require.Equal(t, sKidC, prdC.KadID())

	twC := twp.acquire(&prdC)
	err8 := tt.EntityLink([]byte("/finance/tom"), twC)
	require.NoError(t, err8)
	require.Equal(t, sKidC, (*twC.prd).KadID())
	require.Equal(t, true, twC.online)

	pw := NewPublishWorker(bKid, tt)
	defer pw.Close()

	entities := make([]interface{}, 0)
	entities = pw.EntitiesFor([]byte("/finance/tom"))
	require.Equal(t, 3, len(entities))

	num := 0
	for i := range entities {
		tw := entities[i].(*twin)
		if tw == twA {
			num++
		}
		if tw == twB {
			num++
		}
		if tw == twC {
			num++
		}
	}
	require.Equal(t, 3, num)

	pkt := NewMessagePacket(pKid, uint32(888), byte(0), []byte("/finance/tom"), []byte("xyz123456abc"))
	pw.WorkFor(pkt)
	pw.Wait()

	require.Equal(t, uint32(1), twp.acquire(&prdA).pushSucNum)
	require.Equal(t, uint32(1), twp.acquire(&prdB).pushSucNum)
	require.Equal(t, uint32(1), twp.acquire(&prdC).pushSucNum)

	require.Equal(t, uint32(1), pw.pubSucNum)
	require.Equal(t, uint32(3), pw.fwdSucNum)
	require.Equal(t, uint32(0), pw.pubErrNum)
	require.Equal(t, uint32(0), pw.fwdErrNum)

	require.Equal(t, pKid, pkt.pubKadId)
	require.Equal(t, bKid, pkt.brkKadId)

	flag := pkt.subKadId == sKidA || pkt.subKadId == sKidB || pkt.subKadId == sKidC
	require.Equal(t, true, flag)

	time.Sleep(5 * time.Millisecond)

	require.Equal(t, uint32(1), twp.acquire(&prdA).transSucNum+twp.acquire(&prdA).transErrNum)
	require.Equal(t, uint32(1), twp.acquire(&prdB).transSucNum+twp.acquire(&prdB).transErrNum)
	require.Equal(t, uint32(1), twp.acquire(&prdC).transSucNum+twp.acquire(&prdB).transErrNum)

	// section A:
	dst := make([]byte, 0)
	pkt.SetSubscriberKadId(sKidA)
	require.Equal(t, uint64(len(pkt.AppendTo(dst))), twp.acquire(&prdA).transSucSize+twp.acquire(&prdA).transErrSize)

	// section B:
	dst = make([]byte, 0)
	pkt.SetSubscriberKadId(sKidB)
	require.Equal(t, uint64(len(pkt.AppendTo(dst))), twp.acquire(&prdB).transSucSize+twp.acquire(&prdB).transErrSize)

	// section C:
	dst = make([]byte, 0)
	pkt.SetSubscriberKadId(sKidC)
	require.Equal(t, uint64(len(pkt.AppendTo(dst))), twp.acquire(&prdC).transSucSize+twp.acquire(&prdC).transErrSize)

}
