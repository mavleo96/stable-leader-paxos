package paxos

import (
	"context"
	"time"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
)

// ElectionRouter is the main routine for the leader election
func (l *LeaderElector) ElectionRouter(ctx context.Context) {
electionLoop:
	for {
		select {
		case <-ctx.Done():
			log.Warnf("[ElectionRouter] Context done at %d", time.Now().UnixMilli())
			return
		case <-l.timer.TimeoutCh:
			log.Infof("[ElectionRouter] Timer timeout for ballot number %s at %d", utils.BallotNumberString(l.state.GetBallotNumber()), time.Now().UnixMilli())
		}

		// Reset leader and forwarded requests log
		l.state.SetLeader("")
		l.state.ResetForwardedRequestsLog()

		// Handle prepare messages in queue
		highestBallotNumber, ok := l.PrepareQueueHandler(time.Now())
		if ok {
			// If there is a valid prepare message then continue election loop
			continue electionLoop
		}

		// Initiate Election: Update state and run prepare phase
		newBallotNumber := &pb.BallotNumber{
			N:      highestBallotNumber.N + 1,
			NodeID: l.id,
		}
		l.state.SetBallotNumber(newBallotNumber)
		elected, ackMessages := l.InitiatePrepareHandler(newBallotNumber)
		if !elected {
			// If election is not successful then continue election loop
			continue electionLoop
		}

		// Election successful: give control to proposer
		highestCheckpointedSequenceNum, acceptMessages := aggregateAckMessages(newBallotNumber, ackMessages)
		go l.proposer.RunNewViewPhase(highestCheckpointedSequenceNum, acceptMessages)
		log.Infof("[ElectionRouter] New leader with promised ballot number %s at %d", utils.BallotNumberString(l.state.GetBallotNumber()), time.Now().UnixMilli())
	}
}
