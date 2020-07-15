package marina

import (
	"math/rand"
	"net"
	"testing"

	sr "github.com/TheSmallBoat/carlo/streaming_rpc"
	"github.com/lithdew/kademlia"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

type provider struct {
	kadId *kademlia.ID
}

func (p *provider) KadID() *kademlia.ID {
	return p.kadId
}

func generateKadId() (*kademlia.ID, error) {
	sk := sr.GenerateSecretKey()
	addr, err := net.ResolveTCPAddr("tcp", ":9000")
	if err != nil {
		return nil, err
	}

	kid := &kademlia.ID{
		Pub:  sk.Public(),
		Host: addr.IP,
		Port: uint16(addr.Port),
	}
	return kid, err
}

func TestTwinsPool(t *testing.T) {
	defer goleak.VerifyNone(t)

	tp := newTwinsPool()
	twn, pdn := tp.length()
	require.Equal(t, 0, twn)
	require.Equal(t, 0, pdn)
	require.Empty(t, tp.mpt)
	require.Empty(t, tp.mpp)

	// section 1:
	kid1, err1 := generateKadId()
	require.NoError(t, err1)

	var prd1 twinServiceProvider = &provider{kadId: kid1}
	tw1 := tp.acquire(kid1)

	require.Equal(t, true, kid1.Pub == tw1.kadId.Pub)
	require.Equal(t, true, kid1.Pub == prd1.KadID().Pub)

	twn, pdn = tp.length()
	require.Equal(t, 1, twn)
	require.Equal(t, 1, len(tp.mpt))
	require.Equal(t, 0, pdn)
	require.Equal(t, 0, len(tp.mpp))

	require.Equal(t, false, tp.pairStatus(kid1))

	otn, mtn := tp.checkTwinsProvidersPairStatus()
	require.Equal(t, 1, otn)
	require.Equal(t, 0, mtn)
	require.Equal(t, false, tw1.onlineStatus())
	require.Equal(t, false, tp.pairStatus(kid1))

	pn := tp.appendProviders(&prd1)
	require.Equal(t, 1, pn)
	require.Equal(t, 1, len(tp.mpt))
	require.Equal(t, 1, len(tp.mpp))
	require.Equal(t, true, tw1.onlineStatus())
	require.Equal(t, true, tp.pairStatus(kid1))

	// the same provider append again
	pn = tp.appendProviders(&prd1)
	require.Equal(t, 0, pn)
	require.Equal(t, 1, len(tp.mpt))
	require.Equal(t, 1, len(tp.mpp))

	otn, mtn = tp.checkTwinsProvidersPairStatus()
	require.Equal(t, 0, otn)
	require.Equal(t, 0, mtn)
	require.Equal(t, true, tw1.onlineStatus())
	require.Equal(t, true, tp.pairStatus(kid1))

	require.Equal(t, tw1, tp.mpt[kid1.Pub])
	require.Equal(t, uint32(0), tw1.counter)
	require.Equal(t, uint32(0), tw1.offNum)
	require.Equal(t, true, tw1.online)
	require.NoError(t, tw1.PushMessagePacketToChannel([]byte("hello,world")))
	require.Equal(t, uint32(1), tw1.counter)
	require.Equal(t, uint32(0), tw1.offNum)

	b, ok := tw1.PullMessagePacketFromChannel()
	require.Equal(t, true, ok)
	require.Equal(t, []byte("hello,world"), b)

	tw1.turnToOffline()
	require.Equal(t, false, tw1.online)
	require.Error(t, tw1.PushMessagePacketToChannel([]byte("hello,world.")))
	require.Equal(t, uint32(1), tw1.counter)
	require.Equal(t, uint32(1), tw1.offNum)
	require.Equal(t, true, tp.pairStatus(kid1))

	tw1.turnToOnline()
	require.Equal(t, true, tw1.online)
	require.NoError(t, tw1.PushMessagePacketToChannel([]byte("hello,world..")))
	require.Equal(t, uint32(2), tw1.counter)
	require.Equal(t, uint32(1), tw1.offNum)
	require.Equal(t, true, tp.pairStatus(kid1))

	b, ok = tw1.PullMessagePacketFromChannel()
	require.Equal(t, true, ok)
	require.Equal(t, []byte("hello,world.."), b)

	tw1.setOnlineStatus(false)
	require.Equal(t, false, tw1.online)
	require.Error(t, tw1.PushMessagePacketToChannel([]byte("hello,world...")))
	require.Equal(t, uint32(2), tw1.counter)
	require.Equal(t, uint32(2), tw1.offNum)
	require.Equal(t, true, tp.pairStatus(kid1))

	tp.release(tw1)
	twn, pdn = tp.length()
	require.Equal(t, 0, twn)
	require.Equal(t, 0, len(tp.mpt))
	require.Equal(t, 1, pdn)
	require.Equal(t, 1, len(tp.mpp))
	require.Empty(t, tp.mpt)

	require.Equal(t, false, tw1.onlineStatus())
	require.Equal(t, false, tp.pairStatus(kid1))

	otn, mtn = tp.checkTwinsProvidersPairStatus()
	require.Equal(t, 0, otn)
	require.Equal(t, 1, mtn)
	require.Equal(t, true, tw1.onlineStatus())
	require.Equal(t, true, tp.pairStatus(kid1))

	twn, pdn = tp.length()
	require.Equal(t, 1, twn)
	require.Equal(t, 1, len(tp.mpt))
	require.Equal(t, 1, pdn)
	require.Equal(t, 1, len(tp.mpp))

	// section 2:
	kid2, err2 := generateKadId()
	require.NoError(t, err2)
	require.Equal(t, false, kid1.Pub == kid2.Pub)

	tw2 := tp.acquire(kid2)
	require.Equal(t, true, kid2.Pub == tw2.kadId.Pub)

	twn, pdn = tp.length()
	require.Equal(t, 2, twn)
	require.Equal(t, 2, len(tp.mpt))
	require.Equal(t, 1, pdn)
	require.Equal(t, 1, len(tp.mpp))
	require.Equal(t, tw2, tp.mpt[kid2.Pub])
	require.Equal(t, uint32(0), tw2.counter)
	require.Equal(t, uint32(0), tw2.offNum)
	require.Equal(t, true, tw2.online)
	require.Equal(t, true, tp.pairStatus(kid1))
	require.Equal(t, false, tp.pairStatus(kid2))

	require.NoError(t, tw2.PushMessagePacketToChannel([]byte("hello,world....")))
	require.Equal(t, uint32(1), tw2.counter)
	require.Equal(t, uint32(0), tw2.offNum)

	b2, ok2 := tw2.PullMessagePacketFromChannel()
	require.Equal(t, true, ok2)
	require.Equal(t, []byte("hello,world...."), b2)

	tp.checkTwinsProvidersPairStatus()
	require.Equal(t, true, tw1.onlineStatus())
	require.Equal(t, false, tw2.onlineStatus())

	require.Equal(t, false, tw2.online)
	require.Error(t, tw2.PushMessagePacketToChannel([]byte("hello,world......")))
	require.Equal(t, uint32(1), tw2.counter)
	require.Equal(t, uint32(1), tw2.offNum)

	tp.release(tw2)
	twn, pdn = tp.length()
	require.Equal(t, 1, twn)
	require.Equal(t, 1, len(tp.mpt))
	require.Equal(t, 1, pdn)
	require.Equal(t, 1, len(tp.mpp))

	require.Equal(t, true, tp.pairStatus(kid1))
	require.Equal(t, false, tp.pairStatus(kid2))

	// section 3:
	tw1 = tp.acquire(kid1)
	twn, pdn = tp.length()
	require.Equal(t, 1, twn)
	require.Equal(t, 1, len(tp.mpt))
	require.Equal(t, 1, pdn)
	require.Equal(t, 1, len(tp.mpp))
	tw2 = tp.acquire(kid2)
	twn, pdn = tp.length()
	require.Equal(t, 2, twn)
	require.Equal(t, 2, len(tp.mpt))
	require.Equal(t, 1, pdn)
	require.Equal(t, 1, len(tp.mpp))
	tw3 := tp.acquire(kid2)
	twn, pdn = tp.length()
	require.Equal(t, 2, twn)
	require.Equal(t, 2, len(tp.mpt))
	require.Equal(t, 1, pdn)
	require.Equal(t, 1, len(tp.mpp))
	require.Equal(t, tw2.kadId, tw3.kadId)
}

func BenchmarkTwinsPool(b *testing.B) {
	tp := newTwinsPool()
	require.Empty(b, tp.mpt)

	kid, err1 := generateKadId()
	require.NoError(b, err1)
	tw := tp.acquire(kid)

	buf := make([]byte, 1400)
	_, err2 := rand.Read(buf)
	require.NoError(b, err2)

	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := tw.PushMessagePacketToChannel(buf)
		if err != nil {
			b.Fatal(err)
		}
		tw.PullMessagePacketFromChannel()
	}

	tp.release(tw)
	require.Empty(b, tp.mpt)

	tw2 := tp.acquire(kid)
	for i := 0; i < b.N; i++ {
		err := tw2.PushMessagePacketToChannel(buf)
		if err != nil {
			b.Fatal(err)
		}
		tw2.PullMessagePacketFromChannel()
	}
}
