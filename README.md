# marina
An abstract library that implements a topic-based publish/subscribe mechanism, 
and using the concept of the digital twins that include the interfaces to bind remote service providers, 
achieves the decoupling from specific network protocol libraries.

## test coverage
* 100.0% of statements

## low dependence
1. [cabinet](https://github.com/TheSmallBoat/cabinet) (Using the tree-structure topics manager.)
2. [kademlia](https://github.com/lithdew/kademlia) (Used for the twin‘s identity, cause support the distributed system.)
3. [bytesutil](https://github.com/lithdew/bytesutil) (Used for the binary codec.)
4. [testify](https://github.com/stretchr/testify) (Used in testing.)
5. [goleak](https://pkg.go.dev/go.uber.org/goleak?tab=doc) (Used in testing.)

## Source of inspiration
At first, it was to build a stream-based message-hub service over the peer-to-peer network, 
which does not rely on the MQTT protocol. With the introduction of twin concept, 
it can be completely decoupled from the communication module, so that a peer-to-peer network is only an option, 
which can be a traditional cluster or a single-point server, and which can use the TCP or UDP protocol.  
So it has stronger flexibility and versatility. 

## Design Architecture Diagram (download => [pdf](https://github.com/TheSmallBoat/marina/blob/master/docs/DesignArchitectureDiagram.pdf) // [keynote](https://github.com/TheSmallBoat/marina/blob/master/docs/DesignArchitectureDiagram.key) )

<img width ="960" height="540" src="https://github.com/TheSmallBoat/marina/blob/master/docs/DesignArchitectureDiagram.jpeg">

###

```

% sysctl -a | grep machdep.cpu | grep 'brand_'
machdep.cpu.brand_string: Intel(R) Core(TM) i5-7267U CPU @ 3.10GHz

% go test . -cover -v
=== RUN   TestPacket
--- PASS: TestPacket (0.00s)
=== RUN   TestPublishWorker
--- PASS: TestPublishWorker (0.01s)
=== RUN   TestPublishWorkerForMultipleSubscribe
--- PASS: TestPublishWorkerForMultipleSubscribe (0.01s)
=== RUN   TestSubscribeWorker
--- PASS: TestSubscribeWorker (0.00s)
=== RUN   TestTaskPool
--- PASS: TestTaskPool (0.00s)
=== RUN   TestTwinsPool
--- PASS: TestTwinsPool (0.02s)
PASS
coverage: 100.0% of statements
ok      github.com/TheSmallBoat/marina  0.138s  coverage: 100.0% of statements

% go test -bench=. -benchtime=10s
goos: darwin
goarch: amd64
pkg: github.com/TheSmallBoat/marina
BenchmarkTaskPool-4     34860289               348 ns/op               0 B/op          0 allocs/op
BenchmarkTwinsPool-4    19888471               599 ns/op        2336.85 MB/s          40 B/op          3 allocs/op
PASS
ok      github.com/TheSmallBoat/marina  25.257s

```
