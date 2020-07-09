package marina

import (
	"testing"

	"github.com/lithdew/kademlia"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestPacket(t *testing.T) {
	defer goleak.VerifyNone(t)

	pKid, err1 := generateKadId()
	require.NoError(t, err1)
	bKid, err2 := generateKadId()
	require.NoError(t, err2)
	sKid, err3 := generateKadId()
	require.NoError(t, err3)

	pkt := NewMessagePacket(pKid, uint32(88), byte(0), []byte("/finance/tom"), []byte("xyz123456abc"))

	require.Equal(t, pKid, pkt.pubKadId)

	pkt.SetBrokerKadId(bKid)
	require.Equal(t, bKid, pkt.brkKadId)
	pkt.SetSubscriberKadId(sKid)
	require.Equal(t, sKid, pkt.subKadId)

	require.Equal(t, uint32(88), pkt.mid)
	require.Equal(t, byte(0), pkt.qos)
	require.Equal(t, []byte("/finance/tom"), pkt.topic)
	require.Equal(t, []byte("xyz123456abc"), pkt.payLoad)

	dst := make([]byte, 0)
	pktByte := pkt.AppendTo(dst)

	pkt_, err4 := UnmarshalMessagePacket(pktByte)
	require.NoError(t, err4)
	// the pointer is not equal
	require.Equal(t, pkt.mid, pkt_.mid)
	require.Equal(t, pkt.qos, pkt_.qos)
	require.Equal(t, pkt.topic, pkt_.topic)
	require.Equal(t, pkt.payLoad, pkt_.payLoad)

	dst = make([]byte, 0)
	require.Equal(t, pkt.pubKadId.AppendTo(dst), pkt_.pubKadId.AppendTo(dst))
	require.Equal(t, pkt.brkKadId.AppendTo(dst), pkt_.brkKadId.AppendTo(dst))
	require.Equal(t, pkt.subKadId.AppendTo(dst), pkt_.subKadId.AppendTo(dst))

	require.Equal(t, pkt.pubKadId.Pub, pkt_.pubKadId.Pub)
	require.Equal(t, pkt.brkKadId.Pub, pkt_.brkKadId.Pub)
	require.Equal(t, pkt.subKadId.Pub, pkt_.subKadId.Pub)

	require.Equal(t, pkt.pubKadId.Port, pkt_.pubKadId.Port)
	require.Equal(t, pkt.brkKadId.Port, pkt_.brkKadId.Port)
	require.Equal(t, pkt.subKadId.Port, pkt_.subKadId.Port)

	require.Equal(t, pktByte, pkt_.AppendTo(dst))

	_, err := UnmarshalMessagePacket(pktByte[:4])
	require.Error(t, err)
	_, err = UnmarshalMessagePacket(pktByte[:6])
	require.Error(t, err)
	_, err = UnmarshalMessagePacket(pktByte[:8])
	require.Error(t, err)
	_, err = UnmarshalMessagePacket(pktByte[:20])
	require.Error(t, err)
	_, err = UnmarshalMessagePacket(pktByte[:32])
	require.Error(t, err)
	_, err = UnmarshalMessagePacket(pktByte[:64])
	require.Error(t, err)
	_, err = UnmarshalMessagePacket(pktByte[:96])
	require.Error(t, err)
	_, err = UnmarshalMessagePacket(pktByte[:128])
	require.Error(t, err)

	// test the packet_pool after release and renew one
	pkt.Release()
	require.Equal(t, (*kademlia.ID)(nil), pkt.pubKadId)
	require.Equal(t, (*kademlia.ID)(nil), pkt.brkKadId)
	require.Equal(t, (*kademlia.ID)(nil), pkt.subKadId)
	require.Equal(t, zeroMid, pkt.mid)
	require.Equal(t, zeroQos, pkt.qos)
	require.Equal(t, []byte(nil), pkt.topic)
	require.Equal(t, []byte(nil), pkt.payLoad)

	pkt = NewMessagePacket(pKid, uint32(888), byte(1), []byte("/finance/tom/#"), []byte("....xyz123456abc...."))
	defer pkt.Release()

	require.Equal(t, pKid, pkt.pubKadId)

	pkt.SetBrokerKadId(bKid)
	require.Equal(t, bKid, pkt.brkKadId)
	pkt.SetSubscriberKadId(sKid)
	require.Equal(t, sKid, pkt.subKadId)

	require.Equal(t, uint32(888), pkt.mid)
	require.Equal(t, byte(1), pkt.qos)
	require.Equal(t, []byte("/finance/tom/#"), pkt.topic)
	require.Equal(t, []byte("....xyz123456abc...."), pkt.payLoad)

}
