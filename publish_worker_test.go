package marina

import (
	"testing"

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

	twp := newTwinsPool()
	require.Equal(t, 0, twp.length())
	require.Empty(t, twp.mp)

	tw := twp.acquire(sKid)
	err4 := tt.EntityLink([]byte("/finance/tom"), tw)
	require.NoError(t, err4)
	require.Equal(t, sKid, tw.kadId)
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

	require.Equal(t, uint32(1), twp.acquire(sKid).counter)

	require.Equal(t, uint32(1), pw.pubSucNum)
	require.Equal(t, uint32(1), pw.fwdSucNum)
	require.Equal(t, uint32(0), pw.pubErrNum)
	require.Equal(t, uint32(0), pw.fwdErrNum)

	require.Equal(t, pKid, pkt.pubKadId)
	require.Equal(t, bKid, pkt.brkKadId)
	require.Equal(t, sKid, pkt.subKadId)

	dst := make([]byte, 0)
	pktByte, ok := twp.acquire(sKid).PullMessagePacket()
	require.Equal(t, true, ok)
	require.Equal(t, pkt.AppendTo(dst), pktByte)

	pkt_, err5 := UnmarshalMessagePacket(pktByte)
	require.NoError(t, err5)
	// the pointer is not equal
	require.Equal(t, pkt.mid, pkt_.mid)
	require.Equal(t, pkt.qos, pkt_.qos)
	require.Equal(t, pkt.topic, pkt_.topic)
	require.Equal(t, pkt.payLoad, pkt_.payLoad)

	dst = make([]byte, 0)
	require.Equal(t, pkt.pubKadId.AppendTo(dst), pkt_.pubKadId.AppendTo(dst))
	require.Equal(t, pkt.brkKadId.AppendTo(dst), pkt_.brkKadId.AppendTo(dst))
	require.Equal(t, pkt.subKadId.AppendTo(dst), pkt_.subKadId.AppendTo(dst))

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

	require.Equal(t, uint32(2), twp.acquire(sKid).counter)

	require.Equal(t, uint32(2), pw.pubSucNum)
	require.Equal(t, uint32(2), pw.fwdSucNum)
	require.Equal(t, uint32(1), pw.pubErrNum)
	require.Equal(t, uint32(0), pw.fwdErrNum)

	require.Equal(t, pKid, pkt.pubKadId)
	require.Equal(t, bKid, pkt.brkKadId)
	require.Equal(t, sKid, pkt.subKadId)

	dst = make([]byte, 0)
	pktByte, ok = twp.acquire(sKid).PullMessagePacket()
	require.Equal(t, true, ok)
	require.Equal(t, pkt.AppendTo(dst), pktByte)

	err6 := tt.LinkedEntities([]byte("/finance/tom"), &entities)
	require.NoError(t, err6)
	require.Equal(t, 1, len(entities))
	twe := entities[0].(*twin)
	require.Equal(t, tw, twe)
	require.Equal(t, twp.acquire(sKid), twe)
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

	err7 := tt.EntityUnLink([]byte("/finance/tom"), twp.acquire(sKid))
	require.NoError(t, err7)

	pkt = NewMessagePacket(pKid, uint32(92), byte(1), []byte("/finance/tom"), []byte("123456abc"))
	pw.WorkFor(pkt)
	pw.Wait()

	require.Equal(t, uint32(3), pw.pubSucNum)
	require.Equal(t, uint32(2), pw.fwdSucNum)
	require.Equal(t, uint32(2), pw.pubErrNum)
	require.Equal(t, uint32(1), pw.fwdErrNum)

	// Still equal 2 due to the failure of pushing
	require.Equal(t, uint32(2), twp.acquire(sKid).counter)
}
