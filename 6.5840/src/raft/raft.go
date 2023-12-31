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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log Entries are
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

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	currentLeader int
	votedFor      int
	log           []byte
	state         State
}

// return currentTerm and whether this server
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's Term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry (§5.4)
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
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

	if args.LastLogIndex >= len(rf.log)-1 {
		DPrintf("【%d, %d】<------------ [%d, %d](status: %d) , ", args.CandidateId, args.Term, rf.me, rf.currentTerm)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		return
	}

	reply.VoteGranted = false
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

func (rf *Raft) beginAppendEntries() {
	// broadcast to all other endClients
	appendArgs := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		// PrevLogIndex: rf.log,
		// LeaderCommit
		PrevLogTerm: rf.currentTerm - 1,
	}

	appendReply := AppendEntriesReply{}
	for peer := range rf.peers {
		if peer != rf.me {
			// 是否需要判断ture false? 如果很多都是false怎么办，需要重新发送吗？好像不需要....
			go func(peerIndex int) {
				rf.sendAppendEntries(peerIndex, &appendArgs, &appendReply)
				if appendReply.Term > rf.currentTerm {
					rf.state = 0
				}
			}(peer)
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm

	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentLeader = args.LeaderId
		rf.votedFor = -1 // 新的term，清空投票结果
		rf.state = 0
		reply.Success = true
		DPrintf("                        *[%d, %d]*  ----leader---> [%d,%d]", rf.currentLeader, args.Term, rf.me, rf.currentTerm)
		return
	} else {
		DPrintf("                       [%d,%d] deny leader of [%d]", rf.me, rf.state, rf.currentLeader)
	}

	reply.Success = false
	return

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

	timeout := 150

	rf.currentTerm += 1
	rf.votedFor = rf.me

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
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
			case <-time.After(time.Duration(timeout) * time.Millisecond):
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
		go rf.beginAppendEntries()

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
	rf.currentLeader = -1
	rf.state = 1
	rf.currentTerm = 0
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.mainLoop()

	return rf
}
