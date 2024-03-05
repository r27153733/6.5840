package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"context"
	//	"bytes"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	electionTimeoutMin = 200 * time.Millisecond
	electionTimeoutMax = 350 * time.Millisecond
	heartbeatTime      = 100 * time.Millisecond
)

const (
	roleFollower = iota
	roleCandidate
	roleLeader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	muS       string
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role int
	//roleMu                  sync.RWMutex
	//higherTermChan          chan int
	//electionSuccessTermChan chan int
	appendEntriesChan chan struct{}
	//applyCommandChan  chan []*ApplyMsg
	applyCommandCond  *sync.Cond
	ctx               context.Context
	cancelFunc        context.CancelFunc
	loopCtx           context.Context
	cancelLoopFunc    context.CancelFunc
	runningTerm       int
	currentTerm       int       // 服务器最后知道的任期号（从0开始递增）
	votedFor          int       // 在当前任期内收到选票的 Candidate id（如果没有就为 -1）
	currentLogsLen    int       // 在当前任期内合法的日志
	logs              []*Logger // 日志条目；每个条目包含状态机的要执行命令和从 Leader 处收到时的任期号
	snapshot          Snapshot
	commitIndex       int // 已知的被提交的最大日志条目的索引值（从0开始递增）
	lastApplied       int // 被状态机执行的最大日志条目的索引值（从0开始递增）
	lastHeartbeatTime time.Time
	//nextIndex         []int // 对于每一个服务器，记录需要发给它的下一个日志条目的索引（初始化为最后一个日志条目的 index 加1）
	//matchIndex        []int // 对于每一个服务器，记录已经复制到该服务器的日志的最高索引值（从0开始递增）
	applyCh chan ApplyMsg
}

//	type leader struct {
//		nextIndex  []int // 对于每一个服务器，记录需要发给它的下一个日志条目的索引（初始化为最后一个日志条目的 index 加1）
//		matchIndex []int // 对于每一个服务器，记录已经复制到该服务器的日志的最高索引值（从0开始递增）
//	}
type Logger struct {
	Term    int
	Command interface{}
}

func (rf *Raft) switchLeader() {

	rf.role = roleLeader
	// do Leader
	rf.cancelLoopFunc()
	rf.loopCtx, rf.cancelLoopFunc = context.WithCancel(rf.ctx)
	rf.runningTerm = rf.currentTerm
	go rf.doLeader(rf.loopCtx, rf.currentTerm, rf.logLen())

}

func (rf *Raft) switchCandidate() {
	rf.role = roleCandidate
	// do Candidate
	rf.cancelLoopFunc()
	rf.loopCtx, rf.cancelLoopFunc = context.WithCancel(rf.ctx)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.currentLogsLen = -1
	rf.persist()
	rf.runningTerm = rf.currentTerm
	rf.lastHeartbeatTime = time.Now()
	lastLogIdx := rf.logLen() - 1

	if lastLog, ok := rf.getLog(lastLogIdx); ok {
		go rf.doCandidate(rf.loopCtx, rf.currentTerm, lastLogIdx, lastLog.Term)
	} else {
		go rf.doCandidate(rf.loopCtx, rf.currentTerm, lastLogIdx, rf.snapshot.LastIncludedTerm)
	}
}

func (rf *Raft) switchFollower() {
	if rf.role != roleFollower {
		rf.role = roleFollower
		// do Follower
		rf.cancelLoopFunc()
		rf.loopCtx, rf.cancelLoopFunc = context.WithCancel(rf.ctx)
		go rf.doFollower(rf.loopCtx)
	}
}

func (rf *Raft) doFollower(ctx context.Context) {
	DPrintln("doFollower: ", rf.me)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(50+rand.Int64N(150)) * time.Millisecond):

			rf.mu.Lock()
			rf.muS = "doFollower"
			t := time.Since(rf.lastHeartbeatTime)

			if t > time.Duration(rand.Int64N(int64(electionTimeoutMax-electionTimeoutMin)))+electionTimeoutMin {
				DPrintln(rf.me, "heartbeatTimeOut", t)
				rf.switchCandidate()
				rf.mu.Unlock()
				return
			} else {
				rf.mu.Unlock()
			}
		}
	}
}
func (rf *Raft) doCandidate(ctx context.Context, term, lastLogIdx, lastLogTerm int) {
	DPrintln("doCandidate: ", rf.me, term)
	//DPrintln(rf.me, "heartbeatTimeOut", time.Since(rf.lastHeartbeatTime))
	timeoutChan := time.After(time.Duration(rand.Int64N(int64(electionTimeoutMax-electionTimeoutMin))) + electionTimeoutMin)
	req := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIdx,
		LastLogTerm:  lastLogTerm,
	}
	peerSize := len(rf.peers)
	voteChan := make(chan bool, peerSize)
	childCtx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		peer := peer
		go func() {
			rf.sendRequestVoteOk(childCtx, peer, &req, voteChan)
		}()
	}
	voteCnt := 0
	disVoteCnt := 0
	voteChan <- true
	for {
		select {
		case <-ctx.Done():
			return
		case <-timeoutChan:
			DPrintln(rf.me, "timeoutC")
			rf.mu.Lock()
			rf.muS = "doCandidate 1"
			rf.switchCandidate()
			rf.mu.Unlock()
			return
		case ok := <-voteChan:
			if ok {
				voteCnt++
			} else {
				disVoteCnt++
			}
			DPrintln(rf.me, voteCnt, disVoteCnt)
			if voteCnt*2 > peerSize {
				rf.mu.Lock()
				rf.muS = "doCandidate 2"
				rf.switchLeader()
				rf.mu.Unlock()
				return
			}
			if disVoteCnt*2 > peerSize {
				rf.mu.Lock()
				rf.muS = "doCandidate 3"
				rf.switchFollower()
				rf.mu.Unlock()
				return
			}
		}
	}
}

func (rf *Raft) sendRequestVoteOk(ctx context.Context, peer *labrpc.ClientEnd, req *RequestVoteArgs, voteChan chan bool) {
	resp := RequestVoteReply{}
	for {
		select {
		case <-ctx.Done():
			return
		default:
			ok := sendRequestVote(peer, req, &resp)
			if ok {
				rf.mu.Lock()
				rf.muS = "sendRequestVoteOk"
				rf.tryUpdateTerm(resp.Term)
				rf.mu.Unlock()
				voteChan <- resp.VoteGranted
				return
			}
		}
	}
}

func (rf *Raft) doLeader(ctx context.Context, term, nextLogIndex int) {
	DPrintln("doLeader: ", rf.me, term)
	ticker := time.NewTicker(heartbeatTime)
	nextIndex := make([]int, len(rf.peers))  // 对于每一个服务器，记录需要发给它的下一个日志条目的索引（初始化为最后一个日志条目的 index 加1）
	matchIndex := make([]int, len(rf.peers)) // 对于每一个服务器，记录已经复制到该服务器的日志的最高索引值（从0开始递增）
	for i := 0; i < len(rf.peers); i++ {
		nextIndex[i] = nextLogIndex
	}
	rf.allSendAppendEntries(term, nextIndex, matchIndex)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			//DPrintln("ticker", rf.me)
			rf.allSendAppendEntries(term, nextIndex, matchIndex)
		case <-rf.appendEntriesChan:
			rf.allSendAppendEntries(term, nextIndex, matchIndex)
		}
	}
}

func (rf *Raft) allSendAppendEntries(term int, nextIndex, matchIndex []int) {
	rf.mu.Lock()
	rf.muS = "allSendAppendEntries"
	commitLogIndex := rf.logLen() - 1
	var commitLogTerm int
	if commitLog, ok := rf.getLog(commitLogIndex); ok {
		commitLogTerm = commitLog.Term
	} else {
		commitLogTerm = rf.snapshot.LastIncludedTerm
	}
	if rf.role != roleLeader || rf.currentTerm != term || (commitLogTerm != term && commitLogTerm != 0) {
		rf.mu.Unlock()
		return
	}
	existLogPeerCnt := 1
	appendEntriesReqs := make([]*AppendEntriesArgs, len(rf.peers))
	//installSnapshotReqs := make([]*InstallSnapshotArgs, len(rf.peers))
	installSnapshotReq := InstallSnapshotArgs{
		Term:     term,
		LeaderId: rf.me,
		Snapshot: rf.snapshot,
	}
	for i := 0; i < len(rf.peers); i++ {
		if rf.logLen()-nextIndex[i] < 0 || rf.me == i {
			continue
		}
		entries := make([]*Logger, rf.logLen()-nextIndex[i])
		splitLog, ok := rf.splitLog(nextIndex[i])
		if !ok {
			continue
		}
		copy(entries, splitLog)
		var prevLogTerm int
		if log, ok := rf.getLog(nextIndex[i] - 1); ok {
			prevLogTerm = log.Term
		} else {
			prevLogTerm = rf.snapshot.LastIncludedTerm
		}
		appendEntriesReqs[i] = &AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex[i] - 1,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
	}
	rf.mu.Unlock()
	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		peer := peer
		i := i
		go func() {
			if appendEntriesReqs[i] == nil {
				resp := InstallSnapshotReply{}
				ok := sendInstallSnapshot(peer, &installSnapshotReq, &resp)
				if ok {
					rf.tryUpdateTerm(resp.Term)
				}
				return
			}
			resp := AppendEntriesReply{}
			//DPrintln(rf.me, "send", i)
			ok := sendAppendEntries(peer, appendEntriesReqs[i], &resp)
			//DPrintln(ok, rf.me, i)
			if ok {
				rf.mu.Lock()
				rf.muS = "allSendAppendEntries fun1"
				rf.tryUpdateTerm(resp.Term)
				if rf.role != roleLeader || rf.currentTerm != term {
					rf.mu.Unlock()
					return
				}
				if resp.Success {
					if nextIndex[i] < rf.logLen() {
						nextIndex[i] = max(nextIndex[i], appendEntriesReqs[i].PrevLogIndex+2)
					}
					matchIndex[i] = max(matchIndex[i], commitLogIndex)
					existLogPeerCnt++
					//if len(appendEntriesReqs[i].Entries) != 0 {
					//	log.Println(i, "ok commit", commitLogIndex, nextIndex, matchIndex)
					//}
					if existLogPeerCnt*2 > len(rf.peers) && rf.commitIndex < commitLogIndex {
						//log.Println(rf.me, "leader add commit", commitLogIndex, matchIndex)
						rf.commitIndex = commitLogIndex
						rf.applyLogS()
					}
				} else {
					nextIndexPeer := -1
					if resp.XTerm != -1 {
						nextIndexPeer = rf.findTermLogLastIndex(resp.XTerm)
						if nextIndexPeer == -1 {
							nextIndexPeer = min(resp.XIndex, max(appendEntriesReqs[i].PrevLogIndex, 1))
						}

					} else {
						nextIndexPeer = resp.XLen
					}
					nextIndex[i] = min(nextIndex[i], nextIndexPeer)
					//if rf.nextIndex[i] > 1 {
					//	rf.nextIndex[i]--
					//}
				}
				rf.mu.Unlock()
			}
		}()
	}
}
func (rf *Raft) applyLogS() {
	rf.applyCommandCond.Signal()
	//applyMsgs := make([]*ApplyMsg, 0)
	//for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
	//	getLog, _ := rf.getLog(i)
	//	applyMsgs = append(applyMsgs, &ApplyMsg{
	//		CommandValid:  true,
	//		Command:       getLog.Command,
	//		CommandIndex:  i,
	//		SnapshotValid: false,
	//		Snapshot:      nil,
	//		SnapshotTerm:  0,
	//		SnapshotIndex: 0,
	//	})
	//	//if applyMsgs[len(applyMsgs)-1].Command == nil {
	//	//	panic(applyMsgs)
	//	//}
	//}
	//rf.lastApplied = rf.commitIndex
	////log.Println("send msgS", applyMsgs)
	//go func() { rf.applyCommandChan <- applyMsgs }()
}
func (rf *Raft) handleApplyLog() {
	for {
		rf.mu.Lock()
		rf.applyCommandCond.Wait()
		rf.muS = "handleApplyLog wait"
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		applyMsgs := make([]*ApplyMsg, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			getLog, _ := rf.getLog(i)
			if i == 0 {
				panic(getLog)
			}
			applyMsgs = append(applyMsgs, &ApplyMsg{
				CommandValid:  true,
				Command:       getLog.Command,
				CommandIndex:  i,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			})
			//if applyMsgs[len(applyMsgs)-1].Command == nil {
			//	panic(applyMsgs)
			//}
		}
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
		for _, msg := range applyMsgs {
			if msg.CommandIndex == 0 {
				panic(msg)
			}
			rf.applyCh <- *msg
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//DPrintln("GetState s", rf.me)
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.muS = "GetState"
	term = rf.currentTerm
	isleader = rf.role == roleLeader
	//DPrintln("GetState e", rf.me)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		panic(err)
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		panic(err)
	}
	err = e.Encode(rf.currentLogsLen)
	if err != nil {
		panic(err)
	}
	err = e.Encode(rf.logs)
	if err != nil {
		panic(err)
	}
	err = e.Encode(rf.snapshot.LastIncludedIndex)
	if err != nil {
		panic(err)
	}
	err = e.Encode(rf.snapshot.LastIncludedTerm)
	if err != nil {
		panic(err)
	}
	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.snapshot.Data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) bool {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintln(rf.me, false)
		return false
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&rf.currentTerm); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.votedFor); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.currentLogsLen); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.logs); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.snapshot.LastIncludedIndex); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.snapshot.LastIncludedTerm); err != nil {
		panic(err)
	}
	DPrintln(rf.me, true, rf.currentTerm, rf.votedFor, rf.logs)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.muS = "Snapshot"
	if rf.snapshot.LastIncludedIndex == index {
		return
	}
	logger, _ := rf.getLog(index)
	rf.saveSnapshot(index, logger.Term, snapshot)
}

func (rf *Raft) saveSnapshot(index, term int, snapshot []byte) {

	var newLog []*Logger
	if index+1 < rf.logLen() {
		logs, ok := rf.splitLog(index + 1)
		if !ok {
			return
		}
		newLog = make([]*Logger, len(logs))
		copy(newLog, logs)
	} else {
		newLog = make([]*Logger, 0)
	}

	//if newLog[0].Command == nil {
	//	panic(newLog)
	//}
	rf.logs = newLog
	rf.snapshot.Data = snapshot
	rf.snapshot.LastIncludedIndex = index
	rf.snapshot.LastIncludedTerm = term
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // Candidate 的任期号
	CandidateId  int // 请求投票的 Candidate id
	LastLogIndex int // Candidate 最新日志条目的索引值
	LastLogTerm  int // Candidate 最新日志条目对应的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前的任期号，用于 Candidate 更新自己的任期号
	VoteGranted bool // 如果 Candidate 收到选票为 true
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.muS = "RequestVote"

	rf.tryUpdateTerm(args.Term)
	reply.Term = rf.currentTerm

	var lastLogTerm int
	if log, ok := rf.getLog(rf.logLen() - 1); ok {
		lastLogTerm = log.Term
	} else {
		lastLogTerm = rf.snapshot.LastIncludedTerm
	}

	reply.VoteGranted = args.Term >= rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && args.LastLogIndex >= rf.logLen()-1))

	DPrintln("RequestVote", rf.me, *args, *reply, rf.currentTerm, rf.votedFor)
	if reply.VoteGranted {
		rf.lastHeartbeatTime = time.Now()
		rf.votedFor = args.CandidateId
		rf.persist()
	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func sendRequestVote(peer *labrpc.ClientEnd, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := peer.Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int       // Leader 的任期号
	LeaderId     int       //	Leader 的 id，为了其他服务器能重定向到客户端
	PrevLogIndex int       // 最新日志之前的日志的索引值
	PrevLogTerm  int       // 最新日志之前的日志的 Leader 任期号
	Entries      []*Logger // 将要存储的日志条目（表示 heartbeat 时为空，有时会为了效率发送超过一条）
	LeaderCommit int       // Leader 提交的日志条目索引值
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int  // 当前的任期号，用于 Leader 更新自己的任期号
	Success bool // 如果其它服务器包含能够匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
	XTerm   int  // term in the conflicting entry (if any)
	XIndex  int  // index of first entry with that term (if any)
	XLen    int  // log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintln("AppendEntries", rf.me, *args, *reply)
	rf.muS = "AppendEntries"
	rf.lastHeartbeatTime = time.Now()
	//rf.tryUpdateCurrentTerm(args.Term)

	rf.tryUpdateTerm(args.Term)
	reply.Term = rf.currentTerm
	lenOk := rf.logLen() > args.PrevLogIndex

	prevLogIndexTerm := -1
	if lenOk {
		if log, ok := rf.getLog(args.PrevLogIndex); ok {
			prevLogIndexTerm = log.Term
		} else if args.PrevLogIndex == rf.snapshot.LastIncludedIndex {
			prevLogIndexTerm = rf.snapshot.LastIncludedTerm
		}
	}
	reply.Success = args.Term >= rf.currentTerm &&
		lenOk && prevLogIndexTerm == args.PrevLogTerm

	if reply.Success {
		//if args.PrevLogIndex+1+len(args.Entries) > rf.currentLogsLen {
		//	rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
		//	rf.currentLogsLen = len(rf.logs)
		//}
		//rf.persist()
		rf.mergeLog(args.PrevLogIndex, args.Entries)
		if args.LeaderCommit > rf.commitIndex {
			//if rf.commitIndex > min(args.LeaderCommit, len(rf.logs)) {
			//	log.Println(rf.me, rf.role, "add commit error for", args.LeaderId, args.Term, args.PrevLogIndex, *reply, rf.commitIndex, min(args.LeaderCommit, len(rf.logs)))
			//} else if rf.commitIndex < min(args.LeaderCommit, len(rf.logs)) {
			//	log.Println(rf.me, rf.role, "add commit for", args.LeaderId, args.Term, args.PrevLogIndex, *reply, rf.commitIndex, min(args.LeaderCommit, len(rf.logs)))
			//}
			rf.commitIndex = min(max(args.LeaderCommit, rf.commitIndex), rf.logLen())
			rf.applyLogS()
		}
	} else {
		reply.XLen = rf.logLen()
		if lenOk && prevLogIndexTerm != -1 {
			reply.XTerm = prevLogIndexTerm
			reply.XIndex = rf.findTermLogFirstIndex(prevLogIndexTerm)
		} else {
			reply.XTerm = -1
			reply.XIndex = -1
		}

	}
	if rf.me == 0 {
		es := make([]Logger, len(args.Entries))
		for i, entry := range args.Entries {
			es[i] = *entry
		}
		//log.Println("AppendEntries", rf.me, *args, *reply, rf.commitIndex, es)
	}
	//DPrintln("AppendEntries", rf.me, *args, *reply, rf.commitIndex)
}

func sendAppendEntries(peer *labrpc.ClientEnd, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := peer.Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.muS = "Start"
	if rf.role != roleLeader {
		return -1, rf.currentTerm, false
	}

	newLog := Logger{Command: command, Term: rf.currentTerm}
	// leader first append log entry to its local log.
	rf.logs = append(rf.logs, &newLog)
	rf.persist()
	curLogIdx := rf.logLen() - 1
	select {
	case rf.appendEntriesChan <- struct{}{}:
	default:
	}
	return curLogIdx, rf.runningTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.cancelFunc()
	rf.applyCommandCond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.appendEntriesChan = make(chan struct{})
	//rf.applyCommandChan = make(chan []*ApplyMsg)
	rf.applyCommandCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	if !rf.readPersist(persister.ReadRaftState()) {
		rf.votedFor = -1
		rf.logs = []*Logger{{
			Term:    0,
			Command: nil,
		}}
		rf.snapshot = Snapshot{
			LastIncludedIndex: -1,
			LastIncludedTerm:  0,
			Data:              nil,
		}
	} else {
		rf.snapshot.Data = rf.persister.ReadSnapshot()
		if rf.snapshot.LastIncludedIndex != -1 {
			rf.lastApplied = rf.snapshot.LastIncludedIndex
			rf.commitIndex = rf.snapshot.LastIncludedIndex
		}
	}
	rf.ctx, rf.cancelFunc = context.WithCancel(context.Background())
	rf.loopCtx, rf.cancelLoopFunc = context.WithCancel(rf.ctx)
	go rf.handleApplyLog()
	go rf.doFollower(rf.loopCtx)
	//go rf.switchRole()
	//go func() {
	//	for !rf.killed() {
	//		time.Sleep(10 * time.Second)
	//		ok := rf.mu.TryLock()
	//		if ok {
	//			log.Println(me, "life", rf.role, ok, rf.muS)
	//			rf.mu.Unlock()
	//		} else {
	//			log.Println(me, "life", rf.role, ok, rf.muS)
	//		}
	//	}
	//}()
	return rf
}

func (rf *Raft) tryUpdateTerm(term int) {
	if rf.currentTerm < term {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.currentLogsLen = 0
		rf.switchFollower()
		rf.persist()
	}
}

func (rf *Raft) getLog(index int) (*Logger, bool) {
	l := index - rf.snapshot.LastIncludedIndex - 1
	if l < 0 {
		return nil, false
	}
	return rf.logs[l], true
}

func (rf *Raft) logLen() int {
	return len(rf.logs) + rf.snapshot.LastIncludedIndex + 1
}

func (rf *Raft) splitLog(startIndex int) ([]*Logger, bool) {
	s := startIndex - rf.snapshot.LastIncludedIndex - 1
	if s < 0 {
		return nil, false
	}
	return rf.logs[s:], true
}

//	if args.PrevLogIndex+1+len(args.Entries) > rf.currentLogsLen {
//		rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
//		rf.currentLogsLen = len(rf.logs)
//	}
//
// rf.persist()
func (rf *Raft) mergeLog(prevLogIndex int, entries []*Logger) {
	if prevLogIndex+1+len(entries) > rf.currentLogsLen {
		if rf.me == 0 {
			es := make([]Logger, len(entries))
			for i, entry := range entries {
				es[i] = *entry
			}
			DPrintln("merge", rf.me, prevLogIndex+1+len(entries), rf.currentLogsLen, rf.commitIndex, es)
		}
		rf.logs = append(rf.logs[:prevLogIndex+1-rf.snapshot.LastIncludedIndex-1], entries...)
		rf.currentLogsLen = rf.logLen()
		rf.persist()
	} else {
		//log.Println("merge fail", rf.me, prevLogIndex+1+len(entries), rf.currentLogsLen, rf.commitIndex, entries)
	}
}
func (rf *Raft) findTermLogLastIndex(xTerm int) int {
	for i := len(rf.logs) - 1; i > 0; i-- {
		if rf.logs[i].Term == xTerm {
			if rf.snapshot.LastIncludedTerm == xTerm {
				return rf.snapshot.LastIncludedIndex
			}
			return i + rf.snapshot.LastIncludedIndex + 1
		}
	}
	return -1
}
func (rf *Raft) findTermLogFirstIndex(xTerm int) int {
	if rf.snapshot.LastIncludedTerm == xTerm {
		return rf.snapshot.LastIncludedIndex
	}
	for i := 0; i < len(rf.logs); i++ {
		if rf.logs[i].Term == xTerm {
			return i + rf.snapshot.LastIncludedIndex + 1
		}
	}
	return -1
}

type Snapshot struct {
	LastIncludedIndex int    // 快照中包含的最后日志条目的索引值
	LastIncludedTerm  int    // 快照中包含的最后日志条目的任期号
	Data              []byte // 快照块的原始数据
}
type InstallSnapshotArgs struct {
	Term     int // Leader 的任期
	LeaderId int // 为了 Follower 能重定向到客户端
	Snapshot
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	rf.muS = "InstallSnapshot"
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.tryUpdateTerm(args.Term)
	if rf.snapshot.LastIncludedIndex >= args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	rf.lastHeartbeatTime = time.Now()
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.currentLogsLen < args.LastIncludedIndex {
		rf.currentLogsLen = args.LastIncludedIndex
	}
	rf.saveSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
		rf.mu.Unlock()
		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			Command:       nil,
			CommandIndex:  0,
			SnapshotValid: true,
			Snapshot:      rf.snapshot.Data,
			SnapshotTerm:  rf.snapshot.LastIncludedTerm,
			SnapshotIndex: rf.snapshot.LastIncludedIndex,
		}
	} else {
		rf.mu.Unlock()
	}
}

func sendInstallSnapshot(peer *labrpc.ClientEnd, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := peer.Call("Raft.InstallSnapshot", args, reply)
	return ok
}
