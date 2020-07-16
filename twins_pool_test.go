package marina

import (
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/lithdew/kademlia"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

type provider struct {
	kadId   *kademlia.ID
	counter int
}

func (p *provider) KadID() *kademlia.ID {
	return p.kadId
}

func (p *provider) Push(data []byte) error {
	p.counter++
	if p.counter%2 == 1 {
		return fmt.Errorf("counter %d", p.counter)
	}
	return nil
}

func generateKadId() (*kademlia.ID, error) {
	_, sk, err := kademlia.GenerateKeys(nil)
	if err != nil {
		return nil, err
	}

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
	defer tp.close()

	twn, pdn := tp.length()
	require.Equal(t, 0, twn)
	require.Equal(t, 0, pdn)
	require.Empty(t, tp.mpt)
	require.Empty(t, tp.mpp)

	// section 1:
	kid1, err1 := generateKadId()
	require.NoError(t, err1)

	var prd1 twinServiceProvider = &provider{kadId: kid1}
	tw1 := tp.acquire(&prd1)

	require.Equal(t, true, kid1.Pub == (*tw1.prd).KadID().Pub)
	require.Equal(t, true, kid1.Pub == prd1.KadID().Pub)

	twn, pdn = tp.length()
	require.Equal(t, 1, twn)
	require.Equal(t, 1, len(tp.mpt))
	require.Equal(t, 0, pdn)
	require.Equal(t, 0, len(tp.mpp))
	//require.Equal(t, false, tp.pairStatus(kid1.Pub))

	otn, mtn := tp.checkTwinsProvidersPairStatus()
	require.Equal(t, 1, otn)
	require.Equal(t, 0, mtn)
	require.Equal(t, false, tw1.onlineStatus())
	//require.Equal(t, false, tp.pairStatus(kid1.Pub))

	pn := tp.appendProviders(&prd1)
	require.Equal(t, 1, pn)
	require.Equal(t, 1, len(tp.mpt))
	require.Equal(t, 1, len(tp.mpp))
	require.Equal(t, true, tw1.onlineStatus())
	//require.Equal(t, true, tp.pairStatus(kid1.Pub))

	// the same provider append again
	pn = tp.appendProviders(&prd1)
	require.Equal(t, 0, pn)
	require.Equal(t, 1, len(tp.mpt))
	require.Equal(t, 1, len(tp.mpp))

	otn, mtn = tp.checkTwinsProvidersPairStatus()
	require.Equal(t, 0, otn)
	require.Equal(t, 0, mtn)
	require.Equal(t, true, tw1.onlineStatus())
	//require.Equal(t, true, tp.pairStatus(kid1.Pub))

	require.Equal(t, tw1, tp.mpt[kid1.Pub])
	require.Equal(t, uint32(0), tw1.pushSucNum)
	require.Equal(t, uint32(0), tw1.pushErrNum)
	require.Equal(t, true, tw1.online)
	require.NoError(t, tw1.pushMessagePacketToChannel([]byte("hello,world")))
	require.Equal(t, uint32(1), tw1.pushSucNum)
	require.Equal(t, uint32(0), tw1.pushErrNum)

	tw1.turnToOffline()
	// test run twice
	tw1.turnToOffline()
	require.Equal(t, false, tw1.online)
	require.Error(t, tw1.pushMessagePacketToChannel([]byte("hello,world.")))
	require.Equal(t, uint32(1), tw1.pushSucNum)
	require.Equal(t, uint32(1), tw1.pushErrNum)
	//require.Equal(t, true, tp.pairStatus(kid1.Pub))

	tw1.turnToOnline()
	// test run twice
	tw1.turnToOnline()
	require.Equal(t, true, tw1.online)
	require.NoError(t, tw1.pushMessagePacketToChannel([]byte("hello,world..")))
	require.Equal(t, uint32(2), tw1.pushSucNum)
	require.Equal(t, uint32(1), tw1.pushErrNum)
	//require.Equal(t, true, tp.pairStatus(kid1.Pub))

	otn, mtn = tp.checkTwinsProvidersPairStatus()
	require.Equal(t, 0, otn)
	require.Equal(t, 0, mtn)
	require.Equal(t, true, tw1.onlineStatus())

	tw1.turnToOffline()
	require.Equal(t, false, tw1.online)
	require.Error(t, tw1.pushMessagePacketToChannel([]byte("hello,world...")))
	require.Equal(t, uint32(2), tw1.pushSucNum)
	require.Equal(t, uint32(2), tw1.pushErrNum)
	//require.Equal(t, true, tp.pairStatus(kid1.Pub))

	otn, mtn = tp.checkTwinsProvidersPairStatus()
	require.Equal(t, 0, otn)
	require.Equal(t, 0, mtn)
	require.Equal(t, true, tw1.onlineStatus())

	require.Equal(t, true, tw1.online)
	require.NoError(t, tw1.pushMessagePacketToChannel([]byte("hello,world...xyz")))
	require.Equal(t, uint32(3), tw1.pushSucNum)
	require.Equal(t, uint32(2), tw1.pushErrNum)

	tp.release(tw1)
	twn, pdn = tp.length()
	require.Equal(t, 0, twn)
	require.Equal(t, 0, len(tp.mpt))
	require.Equal(t, 1, pdn)
	require.Equal(t, 1, len(tp.mpp))
	require.Empty(t, tp.mpt)

	require.Equal(t, false, tw1.onlineStatus())
	//require.Equal(t, true, tp.pairStatus(kid1.Pub))

	otn, mtn = tp.checkTwinsProvidersPairStatus()
	require.Equal(t, 0, otn)
	require.Equal(t, 1, mtn)
	require.Equal(t, tw1, tp.mpt[prd1.KadID().Pub])

	twn, pdn = tp.length()
	require.Equal(t, 1, twn)
	require.Equal(t, 1, len(tp.mpt))
	require.Equal(t, 1, pdn)
	require.Equal(t, 1, len(tp.mpp))

	// section 2:
	kid2, err2 := generateKadId()
	require.NoError(t, err2)
	require.Equal(t, false, kid1.Pub == kid2.Pub)

	var prd2 twinServiceProvider = &provider{kadId: kid2}
	tw2 := tp.acquire(&prd2)
	require.Equal(t, true, kid2.Pub == (*tw2.prd).KadID().Pub)

	twn, pdn = tp.length()
	require.Equal(t, 2, twn)
	require.Equal(t, 2, len(tp.mpt))
	require.Equal(t, 1, pdn)
	require.Equal(t, 1, len(tp.mpp))
	require.Equal(t, tw2, tp.mpt[kid2.Pub])
	require.Equal(t, uint32(0), tw2.pushSucNum)
	require.Equal(t, uint32(0), tw2.pushErrNum)
	require.Equal(t, true, tw2.online)
	//require.Equal(t, true, tp.pairStatus(kid1.Pub))
	//require.Equal(t, false, tp.pairStatus(kid2.Pub))

	require.NoError(t, tw2.pushMessagePacketToChannel([]byte("hello,world....")))
	require.Equal(t, uint32(1), tw2.pushSucNum)
	require.Equal(t, uint32(0), tw2.pushErrNum)

	require.NoError(t, tw2.pushMessagePacketToChannel([]byte("hello,world.12345")))
	require.Equal(t, uint32(2), tw2.pushSucNum)
	require.Equal(t, uint32(0), tw2.pushErrNum)

	otn, mtn = tp.checkTwinsProvidersPairStatus()
	require.Equal(t, 1, otn)
	require.Equal(t, 0, mtn)
	require.Equal(t, true, tw1.onlineStatus())
	require.Equal(t, false, tw2.onlineStatus())

	require.Equal(t, false, tw2.online)
	require.Error(t, tw2.pushMessagePacketToChannel([]byte("hello,world.a.b.c.d.e")))
	require.Equal(t, uint32(2), tw2.pushSucNum)
	require.Equal(t, uint32(1), tw2.pushErrNum)

	time.Sleep(10 * time.Millisecond)
	tp.maxOfflineTimeDuration = 5 * time.Millisecond
	otn, mtn = tp.checkTwinsProvidersPairStatus()
	require.Equal(t, 1, otn)
	require.Equal(t, 0, mtn)
	require.Equal(t, true, tw1.onlineStatus())
	require.Equal(t, false, tw2.onlineStatus())

	twn, pdn = tp.length()
	require.Equal(t, 1, twn)
	require.Equal(t, 1, len(tp.mpt))
	require.Equal(t, 1, pdn)
	require.Equal(t, 1, len(tp.mpp))

	// tw2 already reset
	tp.release(tw2)

	//require.Equal(t, true, tp.pairStatus(kid1.Pub))
	//require.Equal(t, false, tp.pairStatus(kid2.Pub))

	time.Sleep(5 * time.Millisecond)

	// section 3:
	tw1 = tp.acquire(&prd1)
	twn, pdn = tp.length()
	require.Equal(t, 1, twn)
	require.Equal(t, 1, len(tp.mpt))
	require.Equal(t, 1, pdn)
	require.Equal(t, 1, len(tp.mpp))
	require.Equal(t, true, tw1.onlineStatus())

	tw2 = tp.acquire(&prd2)
	twn, pdn = tp.length()
	require.Equal(t, 2, twn)
	require.Equal(t, 2, len(tp.mpt))
	require.Equal(t, 1, pdn)
	require.Equal(t, 1, len(tp.mpp))
	require.Equal(t, true, tw2.onlineStatus())

	tw2.turnToOffline()
	tw3 := tp.acquire(&prd2)
	twn, pdn = tp.length()
	require.Equal(t, 2, twn)
	require.Equal(t, 2, len(tp.mpt))
	require.Equal(t, 1, pdn)
	require.Equal(t, 1, len(tp.mpp))
	require.Equal(t, (*tw2.prd).KadID(), (*tw3.prd).KadID())
	require.Equal(t, true, tw3.onlineStatus())

	pn = tp.appendProviders(&prd1, &prd2)
	require.Equal(t, 1, pn)
	require.Equal(t, 2, len(tp.mpt))
	require.Equal(t, 2, len(tp.mpp))

	// section 4:
	kid3, err3 := generateKadId()
	require.NoError(t, err3)

	var prd3 twinServiceProvider = &provider{kadId: kid3}
	tw3d := tp.acquire(&prd3)
	require.Equal(t, true, kid3.Pub == (*tw3d.prd).KadID().Pub)
	pn = tp.appendProviders(&prd1, &prd2, &prd3)
	require.Equal(t, 1, pn)
	require.Equal(t, 3, len(tp.mpt))
	require.Equal(t, 3, len(tp.mpp))

	otn, mtn = tp.checkTwinsProvidersPairStatus()
	require.Equal(t, 0, otn)
	require.Equal(t, 0, mtn)

	// section 5:
	tp.release(tw1)
	tp.release(tw1)
	tp.release(tw2)
	tp.release(tw2)
	tp.release(tw3)
	tp.release(tw3)
	tp.release(tw3d)
	tp.release(tw3d)

	var prd4 twinServiceProvider = &provider{kadId: nil}
	tw4 := tp.acquire(&prd4)
	tp.release(tw4)

	otn, mtn = tp.checkTwinsProvidersPairStatus()
	require.Equal(t, 0, otn)
	require.Equal(t, 3, mtn)
}

func BenchmarkTwinsPool(b *testing.B) {
	tp := newTwinsPool()
	defer tp.close()

	require.Empty(b, tp.mpt)

	kid, err1 := generateKadId()
	require.NoError(b, err1)

	var prd twinServiceProvider = &provider{kadId: kid}
	tw := tp.acquire(&prd)

	buf := make([]byte, 1400)
	_, err2 := rand.Read(buf)
	require.NoError(b, err2)

	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := tw.pushMessagePacketToChannel(buf)
		if err != nil {
			b.Fatal(err)
		}
	}

	tp.release(tw)
	require.Empty(b, tp.mpt)

	tw2 := tp.acquire(&prd)
	for i := 0; i < b.N; i++ {
		err := tw2.pushMessagePacketToChannel(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}
