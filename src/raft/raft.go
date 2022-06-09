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
	// "fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//

const (
	LEADER StateType = iota
	FOLLOWER
	CANDIDATE
)

type StateType uint

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int // Último termo que o servidor recebeu ?????
	votedFor    int // Id do candidato que recebeu voto

	//mensagens são enviadas pelo líder em intervalos especificados pelo tempo limite de pulsação .
	heartbeatPulse chan bool
	changeToLeader chan bool

	state     StateType // Estado atual do servidor
	voteCount int       // Contador de votos recebidos
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int = rf.currentTerm          // atribui o termo atual
	var isleader bool = rf.state == LEADER // verifica se o servidor é o líder
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // termo do candidato
	CandidateId int // id do candidato que esta requisitando voto
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // Número do período atual - Eleição em que ela se encontra
	VoteGranted bool // true se o candidato recebeu o voto
}

func (rf *Raft) requestAllVotes() {
	args := &RequestVoteArgs{ // cria o objeto de argumentos
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	for i := range rf.peers { //Itera sobre os servidores requisitando os votos
		if i != rf.me {
			go func(i int) {
				rf.sendRequestVote(i, args, &RequestVoteReply{})
			}(i)
		}
	}
}

//
// example RequestVote RPC handler.
// Método invocado via chamada RPC por outro servidor
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()         // bloqueia o servidor para que não receba votos enquanto está processando
	defer rf.mu.Unlock() // libera o servidor para que outros possam receber votos ao retornar o método

	if rf.currentTerm < args.Term {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm // atualiza o termo do candidato

	if rf.votedFor == -1 { // se não votou nenhum candidato
		rf.votedFor = args.CandidateId // atribui o candidato que está requisitando voto (Pra quem ele votou)
		reply.VoteGranted = true       // Flag que confirma o voto
	} else { //Se ele não votou em ninguem, ele não é um seguidor
		reply.VoteGranted = false
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok { //Se servidor respondeu com sucesso
		if rf.state != CANDIDATE {
			return ok
		}

		if rf.currentTerm < reply.Term {
			rf.becomeFollower(reply.Term)
			return ok
		}

		if reply.VoteGranted {
			rf.voteCount++
			// fmt.Printf("%d: Recebeu o voto de: %d - Quantidade de votos: %d\n", rf.me, server, rf.voteCount)
			if rf.voteCount == len(rf.peers)/2+1 {
				// fmt.Printf("Opa, Tô virando lider: %d \n", rf.me)
				rf.changeToLeader <- true
			}
		}
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = FOLLOWER   //Nó atual ganha status de seguidor
	rf.currentTerm = term // atualiza o termo do nó
	rf.votedFor = -1
}

func (rf *Raft) becomeCandidate() {
	rf.state = CANDIDATE                // Se torna candidato
	rf.currentTerm = rf.currentTerm + 1 // Incrementa o termo pq ele votou
	rf.voteCount = 1                    // Diz que ele votou
	rf.votedFor = rf.me                 // Votou em si mesmo, pois é candidato.
}

func (rf *Raft) becomeLeader() {
	rf.state = LEADER
	rf.votedFor = -1
	// fmt.Printf("%d: Virou o lider \n", rf.me)
}

func (rf *Raft) addHeartBeats() {
	rf.heartbeatPulse <- true
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.heartbeatPulse = make(chan bool, 100)
	rf.changeToLeader = make(chan bool, 100)
	rf.state = FOLLOWER // inicializa o estado como follower
	rf.currentTerm = 0  // termo atual, inicialmente 0
	rf.votedFor = -1    // inicializa com -1, pois ninguém votou

	// Your initialization code here (2A, 2B, 2C).

	go func() {
		for { // Loop infinito
			switch rf.state {
			case FOLLOWER:
				select {
				case <-rf.heartbeatPulse:
				case <-time.After(time.Duration(rand.Int63()%300+300) * time.Millisecond): // Bloqueante até termine o time.After
					rf.becomeCandidate()
				}
			case CANDIDATE:
				rf.requestAllVotes()
				select {
				case <-rf.changeToLeader: //Recebe o sinal de que o nó se tornou o lider
					rf.becomeLeader()
				case <-rf.heartbeatPulse: // Recebe uma pulsação
					rf.becomeFollower(rf.currentTerm)
				case <-time.After(time.Duration(rand.Int63()%300+300) * time.Millisecond): //Não recebeu nada e vai ter uma nova eleição (Se torna candidato)
					rf.becomeCandidate()
				}

			case LEADER:
				rf.addHeartBeats()
				time.Sleep(100 * time.Millisecond)
			}
		}
	}() //Chama função anonima para iniciar o loop de execução em background

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
