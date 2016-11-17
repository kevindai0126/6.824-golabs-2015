package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	ErrTransfer	= "ErrTransfer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Op string
	Uid int64
	From  string
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Uid int64
	From  string
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type ForwardArgs struct {
	Args PutAppendArgs
}

type ForwardReply struct {
	Reply PutAppendReply
}

type TransferArgs struct {
	Content map[string]string
	Seen	map[string]int64
}

type TransferReply struct {
	Err Err
}