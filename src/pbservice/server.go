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

	view viewservice.View
	storage map[string]string
	smu	sync.Mutex
	seen 	map[int64]Err
	semu    sync.Mutex

}

func (pb *PBServer) isPrimary() bool {
	return pb.me == pb.view.Primary
}

func (pb *PBServer) isBackup() bool {
	return pb.me == pb.view.Backup
}


func (pb *PBServer) Forward(key string, value string, op string, uid int64) error {
	if(pb.view.Backup != "") {
		args := ForwardArgs{PutAppendArgs{key, value, op, uid}}
		var reply ForwardReply

		ok := call(pb.view.Backup, "PBServer.DoForward", &args, &reply)
		if !ok {
			return errors.New(ErrForward)
		}
	}

	return nil
}

func (pb *PBServer) DoForward(args *ForwardArgs, reply *ForwardReply) error {
	pb.mu.Lock()
	if(pb.isPrimary()) {
		pb.mu.Unlock()
		return errors.New(ErrForward)
	} else {
		pb.WritePut(args.Args.Key, args.Args.Value, args.Args.Op)
		pb.mu.Unlock()
		pb.semu.Lock()
		pb.seen[args.Args.Uid] = OK
		pb.semu.Unlock()
		//pb.PutAppend(&args.Args, &reply.Reply)
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

func (pb *PBServer) DoTransfer(args *TransferArgs, reply *TransferReply) error {
	pb.mu.Lock()
	if(!pb.isBackup()) {
		pb.mu.Unlock()
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
	pb.mu.Unlock()

	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	if(pb.isPrimary()) {
		key := args.Key
		value, ok := pb.storage[key]
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
	pb.mu.Unlock()
	return nil
}


func (pb *PBServer) WritePut(key string, value string, op string) {
	pb.smu.Lock()
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
	pb.smu.Unlock()
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	if(pb.isPrimary()) {
		re, ok := pb.seen[args.Uid]
		if ok {
			reply.Err = re
		} else {
			ok := pb.Forward(args.Key, args.Value, args.Op, args.Uid)
			if ok == nil {
				pb.WritePut(args.Key, args.Value, args.Op)
				reply.Err = OK
				pb.semu.Lock()
				pb.seen[args.Uid] = reply.Err
				pb.semu.Unlock()
			} else {
				pb.mu.Unlock()
				return errors.New(ErrForward)
			}
		}
	} else {
		reply.Err = ErrWrongServer
	}
 	pb.mu.Unlock()

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
	view, err := pb.vs.Ping(pb.view.Viewnum)

	if err != nil {
		fmt.Print("ERROR for ping view")
	}

	needTransfer := view.Viewnum != pb.view.Viewnum && view.Primary == pb.me
	pb.view = view

	if needTransfer {
		pb.Transfer(pb.storage)
	}

	pb.mu.Unlock()
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
	pb.seen = make(map[int64]Err)
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
