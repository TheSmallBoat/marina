# marina
A streaming-hub micro-service over the peer-mesh network that implements streaming publish-subscribe mechanism base on topics.

## modules
1. The twins-tunnel module is a tunnel that pair by the peer-nodes, the local twin is the mirror of the remote twin,
 and the worker will unconditional push the packets to the remote twin pool from the local continually.
 While the remote twin got the packets, it will process according to consumers or cache those packets. 
 The role of the local twin pool is to gather all the packets that meet the requirements together and
  push them through a stream channel.
 
2. The topic-manager module is a manager to pickup the packet group by topics from a pool of the publisher,
 and match the subscribers to find the local twin mirror and then put those packets to the local twin pool. 
 
3. The publisher module is a pool to receive the packets continually, but it isn't the twin pool,
 those packets come from the producers.
 
4. The subscriber module is a pool to receive the request of subscribing and build the local twin for the peer-node.

5. The worker pool have some channels to execute the asynchronous task on the background.

6. The micro-service of the streaming-hub owns the above modules and build the peer-mesh network by discovering the service name.
 Maybe have publisher service, subscribe service and twin-tunnel service.