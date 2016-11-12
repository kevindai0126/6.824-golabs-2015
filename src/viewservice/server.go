package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu           sync.Mutex
	l            net.Listener
	dead         int32 // for testing
	rpccount     int32 // for testing
	me           string

			   // Your declarations here.
	view         View
	times        map[string]uint
	primaryAcked uint
	backupAcked  uint
	currentTick  uint
}

func (vs *ViewServer) IsAcked() bool {
	return vs.view.Primary != "" && vs.primaryAcked == vs.view.Viewnum
}

func (vs *ViewServer) IsPrimary(name string) bool {
	return vs.view.Primary == name
}

func (vs *ViewServer) IsBackup(name string) bool {
	return vs.view.Backup == name
}

func (vs *ViewServer) HasPrimary() bool {
	return vs.view.Primary != ""
}

func (vs *ViewServer) HasBackup() bool {
	return vs.view.Backup != ""
}

func (vs *ViewServer) PromoteBackupAsPrimary() {
	if vs.HasBackup() {
		vs.view.Primary = vs.view.Backup
		vs.view.Backup = ""
		vs.view.Viewnum++
		vs.primaryAcked = vs.backupAcked
	}
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	Viewnum := args.Viewnum
	Server := args.Me
	vs.times[Server] = vs.currentTick

	vs.mu.Lock()

	if Viewnum == 0 && !vs.HasPrimary() {
		vs.view.Primary = Server
		vs.view.Viewnum++
		vs.primaryAcked = 0
	} else if vs.IsPrimary(Server) {
		//Primary crashed and restarted, proceeds to a new view
		if Viewnum == 0 && vs.IsAcked() {
			vs.PromoteBackupAsPrimary()
		} else {
			vs.primaryAcked = Viewnum
		}
	} else if vs.IsBackup(Server) {
		//Backup crashed and restarted, proceeds to a new view
		if Viewnum == 0 && vs.IsAcked() {
			vs.view.Backup = Server
			vs.view.Viewnum++
		} else if Viewnum != 0 {
			vs.backupAcked = Viewnum
		}
	} else if !vs.HasBackup() && vs.IsAcked() {
		//There is no backup and there is an idle server, proceeds to a new view
		vs.view.Backup = Server
		vs.view.Viewnum++
	}

	vs.mu.Unlock()

	reply.View = vs.view
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.view
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.currentTick++
	vs.mu.Lock()

	//Hasn't received recent Pings from both primary, proceeds to a new view
	if(vs.HasPrimary()) {
		primaryTick, ok := vs.times[vs.view.Primary]
		if ok && vs.currentTick - primaryTick >= DeadPings && vs.IsAcked() {
			vs.PromoteBackupAsPrimary()
		}
	}

	//Hasn't received recent Pings from both backup, proceeds to a new view
	if (vs.HasBackup()) {
		backupTick, ok := vs.times[vs.view.Backup]
		if ok && vs.currentTick - backupTick >= DeadPings && vs.IsAcked() {
			vs.view.Backup = ""
			vs.view.Viewnum++
		}
	}

	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.view = View{0, "", ""}
	vs.times = make(map[string]uint)
	vs.primaryAcked = 0
	vs.backupAcked = 0
	vs.currentTick = 0

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
