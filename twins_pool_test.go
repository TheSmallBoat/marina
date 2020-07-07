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
	require.Equal(t, int(0), tp.length())
	require.Empty(t, tp.m)

	// section 1:
	kid1, err1 := generateKadId()
	require.NoError(t, err1)
	tw1 := tp.acquire(kid1)
	require.Equal(t, true, kid1.Pub.String() == tw1.kadId.Pub.String())

	require.Equal(t, int(1), tp.length())
	require.Equal(t, int(1), len(tp.m))
	require.Equal(t, tw1, tp.m[kid1.Pub.String()])
	require.Equal(t, uint32(0), tw1.counter)
	require.Equal(t, uint32(0), tw1.offNum)
	require.Equal(t, true, tw1.online)
	require.NoError(t, tw1.Push([]byte("hello,world")))
	require.Equal(t, uint32(1), tw1.counter)
	require.Equal(t, uint32(0), tw1.offNum)

	b := <-tw1.tc
	require.Equal(t, []byte("hello,world"), b)

	tw1.turnToOffline()
	require.Equal(t, false, tw1.online)
	require.Error(t, tw1.Push([]byte("hello,world.")))
	require.Equal(t, uint32(1), tw1.counter)
	require.Equal(t, uint32(1), tw1.offNum)

	tw1.turnToOnline()
	require.Equal(t, true, tw1.online)
	require.NoError(t, tw1.Push([]byte("hello,world..")))
	require.Equal(t, uint32(2), tw1.counter)
	require.Equal(t, uint32(1), tw1.offNum)

	b = <-tw1.tc
	require.Equal(t, []byte("hello,world.."), b)

	tw1.setOnlineStatus(false)
	require.Equal(t, false, tw1.online)
	require.Error(t, tw1.Push([]byte("hello,world...")))
	require.Equal(t, uint32(2), tw1.counter)
	require.Equal(t, uint32(2), tw1.offNum)

	tp.release(tw1)
	require.Equal(t, int(0), tp.length())
	require.Empty(t, tp.m)

	// section 2:
	kid2, err2 := generateKadId()
	require.NoError(t, err2)
	require.Equal(t, false, kid1.Pub.String() == kid2.Pub.String())

	tw2 := tp.acquire(kid2)
	// the same point about the tw,tw2
	require.Equal(t, tw1, tw2)
	require.Equal(t, true, kid2.Pub.String() == tw2.kadId.Pub.String())

	require.Equal(t, int(1), tp.length())
	require.Equal(t, int(1), len(tp.m))
	require.Equal(t, tw2, tp.m[kid2.Pub.String()])
	require.Equal(t, uint32(0), tw2.counter)
	require.Equal(t, uint32(0), tw2.offNum)
	require.Equal(t, true, tw2.online)

	require.NoError(t, tw2.Push([]byte("hello,world....")))
	require.Equal(t, uint32(1), tw2.counter)
	require.Equal(t, uint32(0), tw2.offNum)

	b2 := <-tw2.tc
	require.Equal(t, []byte("hello,world...."), b2)

	tw2.turnToOffline()
	require.Equal(t, false, tw2.online)
	require.Error(t, tw2.Push([]byte("hello,world......")))
	require.Equal(t, uint32(1), tw2.counter)
	require.Equal(t, uint32(1), tw2.offNum)

	tp.release(tw2)
	require.Equal(t, int(0), tp.length())
	require.Empty(t, tp.m)

	// section 3:
	tw1 = tp.acquire(kid1)
	require.Equal(t, int(1), tp.length())
	tw2 = tp.acquire(kid2)
	require.Equal(t, int(2), tp.length())
	tw3 := tp.acquire(kid2)
	require.Equal(t, int(2), tp.length())
	require.Equal(t, tw2.kadId, tw3.kadId)
}

func BenchmarkTwinsPool(b *testing.B) {
	tp := newTwinsPool()
	require.Empty(b, tp.m)

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
		err := tw.Push(buf)
		if err != nil {
			b.Fatal(err)
		}
		<-tw.tc
	}

	tp.release(tw)
	require.Empty(b, tp.m)

	tw2 := tp.acquire(kid)
	for i := 0; i < b.N; i++ {
		err := tw2.Push(buf)
		if err != nil {
			b.Fatal(err)
		}
		<-tw2.tc
	}

}
