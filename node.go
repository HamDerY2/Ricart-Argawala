package main

import (
	"Ricart-Agrawala/pb"
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	cond *sync.Cond
	pb.UnimplementedNodeServiceServer
	ID                int
	Port              int
	Clock             int
	Requesting        bool
	InCriticalSection bool
	RequestTimestamp  int
	RepliesReceived   map[int]bool
	DeferredReplies   map[int]bool
	Peers             map[int]string
	Connections       map[int]pb.NodeServiceClient
	mu                sync.Mutex
}

func NewNode(id int) *Node {
	allNodes := map[int]string{
		1: "localhost:5001",
		2: "localhost:5002",
		3: "localhost:5003",
	}

	peers := make(map[int]string)

	for nodeID, address := range allNodes {
		if nodeID != id {
			peers[nodeID] = address
		}
	}

	node := &Node{
		ID:                id,
		Port:              5000 + id,
		Clock:             0,
		Requesting:        false,
		InCriticalSection: false,
		RequestTimestamp:  0,
		RepliesReceived:   make(map[int]bool),
		DeferredReplies:   make(map[int]bool),
		Peers:             peers,
		Connections:       make(map[int]pb.NodeServiceClient),
	}
	node.cond = sync.NewCond(&node.mu)
	return node
}

func (n *Node) StartServer() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", n.Port))
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	pb.RegisterNodeServiceServer(grpcServer, n)
	return grpcServer.Serve(listener)
}

func (n *Node) ConnectToPeers() error {
	for peerID, address := range n.Peers {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}

		n.Connections[peerID] = pb.NewNodeServiceClient(conn)
		// Connect to this peer
	}
	return nil
}

func (n *Node) SendRequest(ctx context.Context, req *pb.Request) (*pb.Ack, error) {
	// Handle incoming request
	n.mu.Lock()

	n.Clock = max(n.Clock, int(req.Timestamp)) + 1
	fmt.Println("Node", n.ID, "received REQUEST from node", req.NodeId)
	replyImmediately := false

	if !n.Requesting {
		replyImmediately = true
	} else if n.RequestTimestamp < int(req.Timestamp) ||
		(n.RequestTimestamp == int(req.Timestamp) && n.ID < int(req.NodeId)) {
		n.DeferredReplies[int(req.NodeId)] = true
		fmt.Println("Node", n.ID, "deferred REPLY to node", req.NodeId)
	} else {
		replyImmediately = true
	}

	n.mu.Unlock()

	if replyImmediately {
		n.mu.Lock()
		n.Clock++
		timestamp := n.Clock
		n.mu.Unlock()
		fmt.Println("Node", n.ID, "sending REPLY to node", req.NodeId)
		_, err := n.Connections[int(req.NodeId)].SendReply(ctx, &pb.Reply{
			NodeId: int32(n.ID), Timestamp: int64(timestamp),
		})

		if err != nil {
			return nil, err
		}
	}
	return &pb.Ack{}, nil
}

func (n *Node) SendReply(ctx context.Context, reply *pb.Reply) (*pb.Ack, error) {
	n.mu.Lock()
	n.Clock = max(n.Clock, int(reply.Timestamp)) + 1
	fmt.Println("Node", n.ID, "received REPLY from node", reply.NodeId)
	n.RepliesReceived[int(reply.NodeId)] = true
	n.cond.Broadcast()
	n.mu.Unlock()

	return &pb.Ack{}, nil
}

func (n *Node) RequestCS() {
	n.mu.Lock()
	n.Requesting = true
	n.RepliesReceived = make(map[int]bool)
	n.Clock++
	n.RequestTimestamp = n.Clock
	ctx := context.Background()
	request := &pb.Request{
		NodeId:    int32(n.ID),
		Timestamp: int64(n.RequestTimestamp),
	}
	n.mu.Unlock()

	for peerID, client := range n.Connections {
		// Do something with peerID and client
		_, err := client.SendRequest(ctx, request)
		if err != nil {
			fmt.Println("Failed to send request to node", peerID, ":", err)
		}
	}

	n.mu.Lock()

	for len(n.RepliesReceived) < len(n.Peers) {
		n.cond.Wait()
	}

	n.InCriticalSection = true

	n.mu.Unlock()

	fmt.Println("Node", n.ID, "entered the critical section")
	time.Sleep(15 * time.Second)

	fmt.Println("Node", n.ID, "leaving the critical section")
	n.LeaveCS()
}

func (n *Node) LeaveCS() {
	n.mu.Lock()

	n.InCriticalSection = false
	n.Requesting = false

	deferred := n.DeferredReplies
	n.DeferredReplies = make(map[int]bool)

	n.mu.Unlock()

	for peerID := range deferred {
		n.mu.Lock()
		n.Clock++
		timestamp := n.Clock
		n.mu.Unlock()

		_, err := n.Connections[peerID].SendReply(
			context.Background(),
			&pb.Reply{
				NodeId:    int32(n.ID),
				Timestamp: int64(timestamp),
			},
		)

		if err != nil {
			fmt.Println("Failed to send deferred reply to node", peerID, ":", err)
		}
	}
}
