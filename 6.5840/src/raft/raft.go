package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

const (
	electionRpcTimout = 150
	appendRpcTimeout  = 200
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// Persistent state a Raft server must maintain.
	currentTerm   int
	currentLeader int
	votedFor      int
	logs          []*LogEntry
	state         State

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int // 用于跟踪需要发送给每个跟随者的下一个日志条目的索引。
	matchIndex []int // 每个跟随者的日志中已经复制上去的最高的日志条目索引。
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.currentTerm, rf.currentLeader == rf.me
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("【%d, %d】 ************》[%d, %d]  requestVote", args.CandidateId, args.Term, rf.me, rf.currentTerm)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && (rf.state == 1 || rf.state == 2)) {
		reply.VoteGranted = false
		return
	}

	rf.currentTerm = args.Term
	rf.state = 0

	// 如果已经投过票了
	if rf.votedFor != -1 {
		DPrintf("【%d, %d】<------x------ [%d, %d](status: %d) , already vote or leader for %d, leader %d", args.CandidateId, args.Term, rf.me, rf.currentTerm, rf.state, rf.votedFor, rf.currentLeader)
		reply.VoteGranted = false
		return
	}

	// 日志落后
	if args.LastLogIndex < len(rf.logs)-1 {
		DPrintf("【%d, %d】<------x------ [%d, %d](status: %d) , log is less than %d, leader %d", args.CandidateId, args.Term, rf.me, rf.currentTerm, rf.state, rf.votedFor, rf.currentLeader)
		reply.VoteGranted = false
		return
	}

	DPrintf("【%d, %d】<------------ [%d, %d](status: %d) , ", args.CandidateId, args.Term, rf.me, rf.currentTerm)
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) initiateAsNewLeader() {
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers)) // initialized to leader last log index + 1
	for index := range rf.nextIndex {
		rf.nextIndex[index] = len(rf.logs)
	}
}

// beginAppendEntries as a leader, send info
func (rf *Raft) beginAppendEntries() {
	// broadcast to all other endClients
	appendArgs := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	appendReply := AppendEntriesReply{}

	var wg sync.WaitGroup

	for peer := range rf.peers {
		//BPrintf("peer:[%d]: len of log : %d", peer, len(rf.logs))
		if peer != rf.me {
			// todo 需要判断ture false, 通过matchIndex更新commitIndex
			go func(peerIndex int) {
				wg.Add(1)
				appendArgs.PrevLogIndex = rf.nextIndex[peerIndex] - 1
				if appendArgs.PrevLogIndex >= 0 {
					appendArgs.PrevLogTerm = rf.logs[appendArgs.PrevLogIndex].Term
					appendArgs.Entries = rf.logs[appendArgs.PrevLogIndex+1:]
				} else {
					BPrintf("error!!! log should begin at 1")
				}

				ch := make(chan bool, 1)
				// 嵌套机制方便实现超时判断
				go func() {
					ch <- rf.sendAppendEntries(peerIndex, &appendArgs, &appendReply)
				}()

				select {
				case ok := <-ch:
					if ok {
						BPrintf("leader %d <- - - %t- - - -  peer [%d], MatchIndex:%d", rf.me, appendReply.Success, peerIndex, appendReply.MatchIndex)
						if appendReply.Term > rf.currentTerm {
							// 自己领导的角色就不保了
							rf.state = 0
							wg.Done()
							return
						}
						if appendReply.Success {
							rf.mu.Lock()
							rf.nextIndex[peerIndex] = appendReply.MatchIndex + 1
							rf.matchIndex[peerIndex] = appendReply.MatchIndex
							rf.mu.Unlock()
						} else {
							rf.mu.Lock()
							// 访问失败，只修改nextIndex
							rf.nextIndex[peerIndex] -= 1
							rf.mu.Unlock()
						}
					}
					wg.Done()
				case <-time.After(time.Duration(appendRpcTimeout) * time.Millisecond):
					wg.Done()
				}
			}(peer)
		}
	}

	// wait until get all result
	wg.Wait()

	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
	// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
	rf.mu.Lock()
	var matchIndexCopy = make([]int, len(rf.matchIndex))
	copy(matchIndexCopy, rf.matchIndex)

	sort.Ints(matchIndexCopy)
	BPrintf("for leader %d: matchIndexCopy change to %v", rf.me, rf.matchIndex)
	N := matchIndexCopy[len(matchIndexCopy)/2]
	if N > rf.commitIndex && rf.logs[N].Term == rf.currentTerm {
		rf.commitIndex = N // todo
	}
	rf.mu.Unlock()
}

// AppendEntries 收到了领导的来信
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if args.Term < rf.currentTerm {
		DPrintf("                       [%d,%d] deny leader of [%d]", rf.me, rf.state, rf.currentLeader)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	reply.Term = rf.currentTerm

	// heartbeat的行为
	if len(args.Entries) == 0 {
		reply.Success = true
		rf.currentTerm = args.Term
		rf.currentLeader = args.LeaderId
		//rf.votedFor = -1 // 新的term，清空投票结果
		reply.MatchIndex = len(rf.logs) - 1
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.logs))))
		rf.state = 0

		DPrintf("                        *[%d, %d]*  ----leader---> [%d,%d]", rf.currentLeader, args.Term, rf.me, rf.currentTerm)
		BPrintf("leader %d ----------------> peer:[%d]. heartbeat", args.LeaderId, rf.me)
		return
	}

	// 有日志数据，进入判断
	if args.PrevLogTerm >= len(rf.logs) || rf.logs[args.PrevLogTerm].Term != args.PrevLogTerm {
		DPrintf("                       [%d,%d] deny leader of [%d] because of Prev log and term", rf.me, rf.state, rf.currentLeader)
		reply.MatchIndex = len(rf.logs) - 1
		reply.Success = false
	} else {
		reply.Success = true
		// 应该从发送的第几个开始append
		rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.logs))))
		reply.MatchIndex = len(rf.logs) - 1
		BPrintf("leader %d ----------------> peer:[%d]. index:%d, entry:%v, peer's MatchIndex:%d", args.LeaderId, rf.me, args.PrevLogIndex+1, args.Entries, reply.MatchIndex)
		// todo rf.lastApplied
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	if rf.currentLeader != rf.me {
		return len(rf.logs), rf.currentTerm, false
	}

	newEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, &newEntry)

	if rf.currentLeader == rf.me {
		rf.matchIndex[rf.me] = len(rf.logs) - 1
		BPrintf("!!== leader %d get new command, matchIndex change to %d == !!", rf.me, rf.matchIndex[rf.me])
	}
	return len(rf.logs) - 1, rf.currentTerm, rf.currentLeader == rf.me
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) tickerAsFollower() {
	for {
		// 判断是否超时，转换状态
		rf.currentLeader = -1
		ms := 500 + (rand.Int63() % 400)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		DPrintf("[%d,%d] is follower, leader : %d", rf.me, rf.currentTerm, rf.currentLeader)

		if rf.currentLeader == -1 {
			rf.state = 1
			return
		}
	}
}

func (rf *Raft) tickerAsCandidate() {
	for {
		rf.votedFor = -1
		rf.currentLeader = -1
		//DPrintf("-------------------------%d time out, term:%d---------------------", rf.me, rf.currentTerm)
		ms := 200 + (rand.Int63() % 400)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		if rf.state != 1 {
			return
		}

		DPrintf("------------------------- %d begin election :%d (status: %d)---------------------", rf.me, rf.currentTerm, rf.state)
		rf.beginElection()
	}
}

func (rf *Raft) beginElection() {

	rf.currentTerm += 1
	rf.votedFor = rf.me

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs) - 1,
	}

	// args.LastLogTerm todo 这里怎么设置呢
	reply := RequestVoteReply{
		VoteGranted: false,
	}

	posVote := 1
	var wg sync.WaitGroup

	for peerIndex := range rf.peers {
		// do not need to send to candidate itself
		if peerIndex == rf.me {
			continue
		}
		wg.Add(1)
		go func(peerIndex int) {
			ch := make(chan bool, 1)

			// 嵌套机制方便实现超时判断
			go func() {
				ch <- rf.sendRequestVote(peerIndex, &args, &reply)
			}()

			select {
			case ok := <-ch:
				if ok && reply.VoteGranted {
					rf.mu.Lock()
					posVote += 1
					rf.mu.Unlock()
				}
				wg.Done()
			case <-time.After(time.Duration(electionRpcTimout) * time.Millisecond):
				wg.Done()
			}
		}(peerIndex)
	}

	wg.Wait()
	DPrintf("vote result:%d", posVote)
	if rf.state == 1 && posVote > len(rf.peers)/2 {
		rf.currentLeader = rf.me
		rf.state = 2
		// done = true
		rf.initiateAsNewLeader()
		// todo 这里为什么需要用go线程
		rf.beginAppendEntries()

	}
}

func (rf *Raft) tickerAsLeader() {
	for {
		if rf.state != 2 {
			return
		}

		rf.currentLeader = rf.me
		rf.beginAppendEntries()
		ms := 100 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) mainLoop() {
	for rf.killed() == false {
		switch rf.state {
		case 0:
			rf.tickerAsFollower()
			break
		case 1:
			rf.tickerAsCandidate()
			break
		case 2:
			rf.tickerAsLeader()
			break
		}
	}
}

func (rf *Raft) applyLogs(applyCh chan ApplyMsg) {
	for rf.killed() == false {

		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(appendRpcTimeout) * time.Millisecond)
	}
}

// Make the service or tester wants to create a Raft server. the ports
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
	rf.currentLeader = -1
	rf.state = 1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = append(rf.logs, &LogEntry{Term: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.mainLoop()

	go rf.applyLogs(applyCh)
	return rf
}
