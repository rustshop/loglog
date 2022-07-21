# LilLil LogLog!

LilLil LogLog (or just LogLog for short) is (well... will be) a distributed, replicated,
ordered, strongly consistent, (eventually) fault-tolerant binary log in Rust.

Anytime I think about communication patterns in distributed systems,
it hits me that I really, really need:

* replicated log (so the data doesn't go *poof*)
* strongly and consistently ordered (no spooky non-deterministc behavior)
* with decent performance
* that scales reasonably both up and down (so I can start with my $.50/day k8s cluster,
  yet later down the line I don't have to rewrite everything, just because it starts
  to see some real action)

There's a reason every distributed storage starts with replicated log under the hood.
So why there are so few distributed logs that I could use directly?

The closest thing to what I want is [Log Cabin](https://logcabin.github.io),
but I want it be content-oblivious, and will try to make it faster. Plus
I'm not touching C++. Another similiar thing seem to be Apache BookKeeper, but
it is just another resource and operationally heavy Java project that I don't want to work with.

So here is the plan:

* Build the simplest viable distributed log in Rust as possible.
* Start with single node version, then add replication and Raft to get HA.
* Design data model for theoretically good perf.
* Steal all the good ideas I'm aware of from existing projects.
* Try making it scale up as much as possible without sharding by offloading as much as possible
  to client.
* The distributed part is mostly about replication (so we don't loose data). Sharding usually
  can't guarantee ordering between shards anyway, so it's not as interesting (at least yet)
* If you need sharding you probably outgrew LilLilLogLog - build something on top, or just split
  your log, or (fork? and) implement sharding.
* Use byte offsets in segmented (split into separate files) log as ids (like Kafka).
* Limit event sizes to around 3B value. Puts some reasonable bounds on latency.
* All writes start going through the leader, but the client is supposed to upload copies
  to other replicas themselves, to help get the load off the leader.
* On client "append" request, read the fixed header (including length of content) in one `read`,
  then use the lenght to copy the content to file's `fd` ASAP. Ideally this would be zero-copy,
  but seems like tcpsocket->file can't be zero-copy. Since we probably need to load into userspace
  buffer, might as well keep the most recent ones for fast response.
* Don't bother even looking at the data. If the client wants data integrity, they can append
  a checksum, and if they ever spot a corrupted data, they can try reading from other replicas.
  If the client wants multiple things in one append-operation, they can deal with it on their side it too.
* Readers will just get whole batches of events whole. They can use
  any replica, and replicas won't return anything they don't have or that is not yet commited.
* Seems like `tokio-uring` has enough functionality to fit these needs, so might as well start with it.

I'm definitely a lillil bit out of depth here, but I trust the Internet will reliably tell me which of my
ideas are stupid, and worst case I'll just learn some stuff and deliver nothing. 🤷

Read [the design doc](./README.design.md)
