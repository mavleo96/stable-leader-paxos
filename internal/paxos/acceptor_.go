package paxos

import (
	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewViewRequest handles the new view request rpc on server side
func (s *PaxosServer) NewViewRequest(req *pb.NewViewMessage, stream pb.PaxosNode_NewViewRequestServer) error {
	if !s.config.Alive {
		log.Warnf("Node is dead")
		return status.Errorf(codes.Unavailable, "node is dead")
	}

	// No need to acquire state mutex since AcceptRequest acquires it
	log.Infof("Received new view message %v", utils.BallotNumberString(req.B))
	s.state.AddNewViewMessage(req)

	s.logger.AddReceivedNewViewMessage(req)

	for _, acceptMessage := range req.AcceptLog {
		acceptedMessage, err := s.acceptor.AcceptRequestHandler(acceptMessage)
		if err != nil {
			log.Fatal(err)
			return err
		}
		if err := stream.Send(acceptedMessage); err != nil {
			log.Fatal(err)
		}
	}
	log.Infof("Finished streaming accepts for %s", utils.BallotNumberString(req.B))
	return nil
}
