package surfstore

import (
	context "context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	// Added for discussion
	serverId            int64
	ipList              []string
	pendingCommits      []chan bool
	pendingCommitsMutex sync.RWMutex
	commitIndex         int64
	lastApplied         int64

	//log protection
	//Log mutex
	logMutex sync.Mutex

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// panic("todo")
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	for {
		msg, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if msg != nil && msg.Flag {
			break
		}
		if msg != nil && !msg.Flag && err != nil && strings.Contains(err.Error(), "Server is not the leader") {
			return nil, err
		}
	}
	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// panic("todo")
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	for {
		msg, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if msg != nil && msg.Flag {
			break
		}
		if msg != nil && !msg.Flag && err != nil && strings.Contains(err.Error(), "Server is not the leader") {
			return nil, err
		}
	}
	return s.metaStore.GetBlockStoreMap(ctx, hashes)
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// panic("todo")
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	for {
		msg, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if msg != nil && msg.Flag {
			break
		}
		if msg != nil && !msg.Flag && err != nil && strings.Contains(err.Error(), "Server is not the leader") {
			return nil, err
		}
	}
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)
	// return nil, nil
}

func (s *RaftSurfstore) attemptCommit(targetIdx int64, commitIdx int) {
	commitChan := make(chan *AppendEntryOutput, len(s.ipList))
	for idx := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}

		go s.commitEntry(int64(idx), targetIdx, commitChan)
	}

	commCount := 1
	for {
		//TODO: handling of nodes that hv crashed
		commit := <-commitChan
		s.isCrashedMutex.RLock()
		isCrashed := s.isCrashed
		s.isCrashedMutex.RUnlock()
		if isCrashed {
			s.pendingCommits[commitIdx] <- false
			return
		}
		if commit != nil && commit.Success {
			commCount++
		}
		if commit != nil && !commit.Success {
			s.pendingCommits[commitIdx] <- false
			break
		}
		s.isCrashedMutex.RLock()
		isCrashed = s.isCrashed
		s.isCrashedMutex.RUnlock()
		if isCrashed {
			s.pendingCommits[commitIdx] <- false
			return
		}
		if commCount > len(s.ipList)/2 {
			s.commitIndex = int64(math.Max(float64(s.commitIndex), float64(targetIdx)))
			s.pendingCommits[commitIdx] <- true
			break
		}
	}
}

func (s *RaftSurfstore) commitEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {
	for {
		addr := s.ipList[serverIdx]
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		defer conn.Close()
		if err != nil {
			return
		}
		client := NewRaftSurfstoreClient(conn)
		// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		// defer cancel()
		// state, err := client.IsCrashed(ctx, &emptypb.Empty{})
		// if err != nil {
		// 	return
		// }
		if s.isCrashed {
			continue
		}

		for nextIdx := entryIdx; nextIdx >= -1; nextIdx-- {
			prevLogTerm := int64(-1)
			if nextIdx >= 0 {
				prevLogTerm = s.log[nextIdx].Term
			}
			input := &AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  prevLogTerm,
				PrevLogIndex: nextIdx,
				Entries:      s.log,
				LeaderCommit: s.commitIndex,
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			s.isCrashedMutex.RLock()
			isCrashed := s.isCrashed
			s.isCrashedMutex.RUnlock()
			if isCrashed {
				commitChan <- nil
			}
			output, err := client.AppendEntries(ctx, input)
			if output != nil && output.Success {
				commitChan <- output
				return
			}
			if err != nil && strings.Contains(err.Error(), "Server is not the leader") {
				s.isLeaderMutex.Lock()
				s.isLeader = false
				s.isLeaderMutex.Unlock()
				commitChan <- &AppendEntryOutput{
					ServerId:     serverIdx,
					Term:         s.term,
					Success:      false,
					MatchedIndex: -1,
				}
				return
			}
		}

	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// panic("todo")
	// return nil, nil
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$")
	fmt.Println("Update file called: Printing leader details log length:", len(s.log), "serverId:", s.serverId, "filemeta:", filemeta)
	fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$")

	oper := &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.logMutex.Lock()
	s.log = append(s.log, oper)
	logIndex := int64(len(s.log) - 1)
	s.logMutex.Unlock()

	committed := make(chan bool)
	s.pendingCommitsMutex.Lock()
	s.pendingCommits = append(s.pendingCommits, committed)
	commitIdx := len(s.pendingCommits) - 1
	s.pendingCommitsMutex.Unlock()
	go s.attemptCommit(logIndex, commitIdx)
	success := <-committed

	if success {
		for {
			if s.lastApplied == logIndex-1 {
				break
			}
		}
		vers, err := s.metaStore.UpdateFile(ctx, filemeta)
		s.lastApplied++
		return vers, err
	}

	return nil, fmt.Errorf("failed to exec operation")

}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// panic("todo")
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	if input.Term < s.term {
		output := &AppendEntryOutput{
			ServerId:     s.serverId,
			Term:         s.term,
			Success:      false,
			MatchedIndex: -1,
		}
		return output, ERR_NOT_LEADER
	}

	prevIndex := input.PrevLogIndex
	prevTerm := input.PrevLogTerm

	if int64(len(s.log)) <= prevIndex {
		return &AppendEntryOutput{
			ServerId:     s.serverId,
			Term:         s.term,
			Success:      false,
			MatchedIndex: -1,
		}, nil
	}

	if prevIndex >= 0 {
		if s.log[prevIndex].Term != prevTerm {
			return &AppendEntryOutput{
				ServerId:     s.serverId,
				Term:         s.term,
				Success:      false,
				MatchedIndex: -1,
			}, nil
		}
	}

	if s.term < input.Term {
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
	}
	s.term = input.Term
	s.log = append(make([]*UpdateOperation, 0), input.Entries...)
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}

	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	}

	return &AppendEntryOutput{
		ServerId:     s.serverId,
		Term:         s.term,
		Success:      true,
		MatchedIndex: -1,
	}, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// panic("todo")
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		successNote := &Success{
			Flag: false,
		}
		s.isCrashedMutex.RUnlock()
		return successNote, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()
	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true
	s.term++
	successNote := &Success{
		Flag: true,
	}

	return successNote, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// panic("todo")
	heartBCount := 0
	for idx, addr := range s.ipList {
		if int64(idx) == s.serverId {
			heartBCount++
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue
		}
		client := NewRaftSurfstoreClient(conn)
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  -1,
			PrevLogIndex: -1,
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err := client.AppendEntries(ctx, input)
		if (err == nil) || (err != nil && !strings.Contains(err.Error(), "Server is crashed.")) {
			heartBCount++
		}
		if err != nil && strings.Contains(err.Error(), "Server is not the leader") {
			if output != nil {
				s.term = output.Term
			}
			s.isLeaderMutex.Lock()
			s.isLeader = false
			s.isLeaderMutex.Unlock()
			return &Success{
				Flag: false,
			}, ERR_NOT_LEADER
		}
		conn.Close()
	}
	if heartBCount > len(s.ipList)/2 {
		return &Success{
			Flag: true,
		}, nil
	}
	return &Success{
		Flag: false,
	}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
