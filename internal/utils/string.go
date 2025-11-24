package utils

import (
	"encoding/hex"
	"fmt"
	"strings"

	pb "github.com/mavleo96/stable-leader-paxos/pb"
)

func FormattedLoggingString(t any) string {
	switch v := t.(type) {
	case *pb.PrepareMessage:
		return prepareMessageString(v)
	case *pb.AckMessage:
		return ackMessageString(v, false)
	case *pb.AcceptMessage:
		return acceptMessageString(v)
	case *pb.AcceptedMessage:
		return acceptedMessageString(v)
	case *pb.CommitMessage:
		return commitMessageString(v)
	case *pb.NewViewMessage:
		return newViewMessageString(v, false)
	case *pb.CheckpointMessage:
		return checkpointMessageString(v)
	case *pb.TransactionRequest:
		return transactionRequestString(v)
	case *pb.TransactionResponse:
		return transactionResponseString(v)
	case *pb.CatchupRequestMessage:
		return catchupRequestMessageString(v)
	case *pb.CatchupMessage:
		return catchupMessageString(v, false)
	case *pb.GetCheckpointMessage:
		return getCheckpointMessageString(v)
	case *pb.Checkpoint:
		return checkpointString(v)
	case *pb.Transaction:
		return transactionString(v)
	case *pb.BallotNumber:
		return ballotNumberString(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func LoggingString(t any) string {
	switch v := t.(type) {
	case *pb.PrepareMessage:
		return prepareMessageString(v)
	case *pb.AckMessage:
		return ackMessageString(v, true)
	case *pb.AcceptMessage:
		return acceptMessageString(v)
	case *pb.AcceptedMessage:
		return acceptedMessageString(v)
	case *pb.CommitMessage:
		return commitMessageString(v)
	case *pb.NewViewMessage:
		return newViewMessageString(v, true)
	case *pb.CheckpointMessage:
		return checkpointMessageString(v)
	case *pb.TransactionRequest:
		return transactionRequestString(v)
	case *pb.TransactionResponse:
		return transactionResponseString(v)
	case *pb.CatchupRequestMessage:
		return catchupRequestMessageString(v)
	case *pb.CatchupMessage:
		return catchupMessageString(v, true)
	case *pb.GetCheckpointMessage:
		return getCheckpointMessageString(v)
	case *pb.Checkpoint:
		return checkpointString(v)
	case *pb.Transaction:
		return transactionString(v)
	case *pb.BallotNumber:
		return ballotNumberString(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func prepareMessageString(x *pb.PrepareMessage) string {
	return fmt.Sprintf("<PREPARE, %s>", ballotNumberString(x.B))
}

func ackMessageString(x *pb.AckMessage, short bool) string {
	acceptedMessageStringSlice := make([]string, 0)
	for _, acceptedMessage := range x.AcceptLog {
		if short {
			acceptedMessageStringSlice = append(acceptedMessageStringSlice, fmt.Sprintf("A%d", acceptedMessage.SequenceNum))
		} else {
			acceptedMessageStringSlice = append(acceptedMessageStringSlice, acceptedMessageString(acceptedMessage))
		}
	}
	acceptedMessageString := "{}"
	if len(acceptedMessageStringSlice) > 0 {
		acceptedMessageString = "{\n\t" + strings.Join(acceptedMessageStringSlice, ",\n\t") + "\n}"
	}
	if short {
		acceptedMessageString = strings.Join(acceptedMessageStringSlice, ", ")
	}
	return fmt.Sprintf("<ACK, %s, %s>", ballotNumberString(x.B), acceptedMessageString)
}

func acceptMessageString(x *pb.AcceptMessage) string {
	return fmt.Sprintf("<ACCEPT, %s, %d, %s>", ballotNumberString(x.B), x.SequenceNum, transactionRequestString(x.Message))
}

func acceptedMessageString(x *pb.AcceptedMessage) string {
	return fmt.Sprintf("<ACCEPTED, %s, %d, %s, %s>", ballotNumberString(x.B), x.SequenceNum, transactionRequestString(x.Message), x.NodeID)
}

func commitMessageString(x *pb.CommitMessage) string {
	return fmt.Sprintf("<COMMIT, %s, %d, %s>", ballotNumberString(x.B), x.SequenceNum, transactionRequestString(x.Message))
}

func newViewMessageString(x *pb.NewViewMessage, short bool) string {
	acceptMessageStringSlice := make([]string, 0)
	for _, acceptMessage := range x.AcceptLog {
		if short {
			acceptMessageStringSlice = append(acceptMessageStringSlice, fmt.Sprintf("A%d", acceptMessage.SequenceNum))
		} else {
			acceptMessageStringSlice = append(acceptMessageStringSlice, acceptMessageString(acceptMessage))
		}
	}
	acceptMessageString := "{}"
	if len(acceptMessageStringSlice) > 0 {
		acceptMessageString = "{\n\t" + strings.Join(acceptMessageStringSlice, ",\n\t") + "\n}"
	}
	if short {
		acceptMessageString = strings.Join(acceptMessageStringSlice, ", ")
	}
	return fmt.Sprintf("<NEWVIEW, %s, %d, %s>", ballotNumberString(x.B), x.SequenceNum, acceptMessageString)
}

func checkpointMessageString(x *pb.CheckpointMessage) string {
	return fmt.Sprintf("<CHECKPOINT, %d, %s>", x.SequenceNum, hex.EncodeToString(x.Digest))
}

func getCheckpointMessageString(x *pb.GetCheckpointMessage) string {
	return fmt.Sprintf("<GETCHECKPOINT, %d, %s>", x.SequenceNum, x.NodeID)
}

func checkpointString(x *pb.Checkpoint) string {
	return fmt.Sprintf("<%d, %s, %v>", x.SequenceNum, hex.EncodeToString(x.Digest), x.Snapshot)
}

func transactionRequestString(x *pb.TransactionRequest) string {
	if x == nil {
		return "<REQUEST, nil>"
	}
	return fmt.Sprintf("<REQUEST, %s, %d, %s>", transactionString(x.Transaction), x.Timestamp, x.Sender)
}

func transactionResponseString(x *pb.TransactionResponse) string {
	if x == nil {
		return "<REPLY, nil>"
	}
	return fmt.Sprintf("<REPLY, %s, %d, %s, %t>", ballotNumberString(x.B), x.Timestamp, x.Sender, x.Result)
}

func catchupRequestMessageString(x *pb.CatchupRequestMessage) string {
	return fmt.Sprintf("<CATCHUPREQUEST, %d, %s>", x.SequenceNum, x.NodeID)
}

func catchupMessageString(x *pb.CatchupMessage, short bool) string {
	checkpointMessageString := "nil"
	if x.Checkpoint != nil {
		checkpointMessageString = checkpointString(x.Checkpoint)
	}
	commitMessageStringSlice := make([]string, 0)
	for _, commitMessage := range x.CommitLog {
		if short {
			commitMessageStringSlice = append(commitMessageStringSlice, fmt.Sprintf("C%d", commitMessage.SequenceNum))
		} else {
			commitMessageStringSlice = append(commitMessageStringSlice, commitMessageString(commitMessage))
		}
	}
	commitMessageString := "{}"
	if len(commitMessageStringSlice) > 0 {
		commitMessageString = "{\n\t" + strings.Join(commitMessageStringSlice, ",\n\t") + "\n}"
	}
	if short {
		commitMessageString = strings.Join(commitMessageStringSlice, ", ")
	}

	return fmt.Sprintf("<CATCHUP, %s, %s, %s>", ballotNumberString(x.B), checkpointMessageString, commitMessageString)
}

func ballotNumberString(x *pb.BallotNumber) string {
	if x == nil {
		return "<0, nil>"
	}
	return fmt.Sprintf("<%d, %s>", x.N, x.NodeID)
}

func transactionString(x *pb.Transaction) string {
	if x == nil {
		return "(nil)"
	}
	return fmt.Sprintf("(%s, %s, %d)", x.Sender, x.Receiver, x.Amount)
}
