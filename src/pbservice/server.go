package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import (
	"math/rand"
	"errors"
)


type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.

	view viewservice.View //current view
	storage map[string]string //kv storage
	smu	sync.Mutex //mutex for accessing storage
	seen 	map[string]int64 // store the seen uid for put request
	semu    sync.Mutex //mutex for accessing seen

}

func (pb *PBServer) isPrimary() bool {
	return pb.me == pb.view.Primary
}

func (pb *PBServer) isBackup() bool {
	return pb.me == pb.view.Backup
}

func (pb *PBServer) hasSeen(from string, uid int64) bool {
	pb.semu.Lock()
	re, ok := pb.seen[from]
	pb.semu.Unlock()

	return ok && re == uid
}

func (pb *PBServer) setSeen(from string, uid int64) {
	pb.semu.Lock()
	pb.seen[from] = uid
	pb.semu.Unlock()
}

func (pb *PBServer) Forward(key string, value string, op string, uid int64, from string) {
	if(pb.view.Backup != "") {
		args := ForwardArgs{PutAppendArgs{key, value, op, uid, from}}
		var reply ForwardReply

		view := pb.view
		for {
			ok := call(view.Backup, "PBServer.DoForward", &args, &reply)
			if ok {
				return
			} else {
				newview, ok := pb.vs.Ping(view.Viewnum)
				if(ok == nil) {
					view = newview
				}
			}
		}
	}
}

//Forward the Put request to backup
func (pb *PBServer) DoForward(args *ForwardArgs, reply *ForwardReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if(pb.isPrimary()) {
		return errors.New("It's Primary")
	} else {
		if(pb.hasSeen(args.Args.From, args.Args.Uid)) {
			return nil
		}

		pb.WritePut(args.Args.Key, args.Args.Value, args.Args.Op)
		pb.setSeen(args.Args.From, args.Args.Uid)
	}

	return nil
}

func (pb *PBServer) Transfer(content map[string]string) error {
	if(pb.view.Backup != "") {
		args := TransferArgs{pb.storage, pb.seen}
		var reply TransferReply

		ok := call(pb.view.Backup, "PBServer.DoTransfer", &args, &reply)
		if !ok {
			return errors.New(ErrTransfer)
		}
	}

	return nil
}

//Transfer the complete kv from the primary to the newly promoted backup
func (pb *PBServer) DoTransfer(args *TransferArgs, reply *TransferReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if(!pb.isBackup()) {
		return errors.New(ErrTransfer)
	} else {
		pb.smu.Lock()
		pb.storage = args.Content
		pb.smu.Unlock()
		reply.Err = OK
		pb.semu.Lock()
		pb.seen = args.Seen
		pb.semu.Unlock()
	}

	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if(pb.isPrimary()) {
		key := args.Key
		pb.smu.Lock()
		value, ok := pb.storage[key]
		pb.smu.Unlock()
		if ok {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Value = ""
			reply.Err = ErrNoKey
		}
	} else {
		reply.Value = ""
		reply.Err = ErrWrongServer
	}

	return nil
}


func (pb *PBServer) WritePut(key string, value string, op string) {
	pb.smu.Lock()
	defer pb.smu.Unlock()

	switch op {
	case "Put":
		pb.storage[key] = value
	case "Append":
		preValue, ok := pb.storage[key]
		if ok {
			pb.storage[key] = preValue + value
		} else {
			pb.storage[key] = value
		}
	}
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if(pb.isPrimary()) {
		if(pb.hasSeen(args.From, args.Uid)) {
			reply.Err = OK
		} else {
			pb.Forward(args.Key, args.Value, args.Op, args.Uid, args.From)
			pb.setSeen(args.From, args.Uid)
			pb.WritePut(args.Key, args.Value, args.Op)
			reply.Err = OK
		}
	} else {
		reply.Err = ErrWrongServer
	}

	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	view, err := pb.vs.Ping(pb.view.Viewnum)

	if err != nil {
		fmt.Print("ERROR for ping view")
	}

	needTransfer := view.Viewnum != pb.view.Viewnum && view.Primary == pb.me
	pb.view = view

	if needTransfer {
		pb.Transfer(pb.storage)
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.storage = make(map[string]string)
	pb.seen = make(map[string]int64)
	pb.view = viewservice.View{0, "", ""}

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
