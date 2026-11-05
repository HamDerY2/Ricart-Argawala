Ricart–Agrawala Distributed Mutual Exclusion

A Go implementation of the Ricart–Agrawala distributed mutual exclusion algorithm using gRPC for communication between nodes.


Requirements

- Go installed
- Three available local ports: `5001`, `5002`, and `5003`


Running the system

The system uses hardcoded peer addresses for service discovery. Each node runs as a separate process and acts as both a gRPC server and a client to the other nodes.

Open three separate terminals in the project directory.

Terminal 1:
write:
go run . -id=1

Terminal 2:
write:
go run . -id=2

Terminal 3:
write:
go run . -id=3

Wait until all three nodes have started.


Requesting access to the Critical Section

To request access, type:
request

Each node sends a request to its peers. It enters the Critical Section after receiving replies from all other nodes.

The Critical Section is emulated by printing entry and exit messages, with a 15-second delay between them.


Algorithm

The implementation uses Lamport logical clocks to order requests. When two requests have the same timestamp, the node ID is used as a tie-breaker.

Nodes defer replies to requests with lower priority while they have a higher-priority request of their own. Deferred replies are sent when the node leaves the Critical Section.

This ensures mutual exclusion and allows waiting nodes to eventually enter the Critical Section, assuming all nodes remain operational and messages are successfully delivered.