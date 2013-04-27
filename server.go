package gomapreduce

import (
	"fmt"
	"sync"
	"net"
)




type MapReduceNode struct {
	mu sync.Mutex
	l net.Listener
	me int                 // index into nodes []string
	nodes []string         // Fellow Map Reduce Nodes
	dead bool
	unreliable bool
}


func (self *MapReduceNode) Start() {
	fmt.Println("Start!!")
}


// func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
//   px := &Paxos{}
//   px.peers = peers
//   px.me = me
//   // Your initialization code here.
//   px.peer_count = len(peers)
//   px.state = map[int]*AgreementState{}
//   px.done = map[string]int{}
//   for _, peer := range px.peers {
//     // First agreement instance agreement_number is 0. Initially clients have not marked it done.
//     px.done[peer] = -1     
//   }
  
//   if rpcs != nil {
//     // caller will create socket &c
//     rpcs.Register(px)
//   } else {
//     rpcs = rpc.NewServer()
//     rpcs.Register(px)

//     // prepare to receive connections from clients.
//     // change "unix" to "tcp" to use over a network.
//     os.Remove(peers[me]) // only needed for "unix"
//     l, e := net.Listen("unix", peers[me]);
//     if e != nil {
//       log.Fatal("listen error: ", e);
//     }
//     px.l = l
    
//     // please do not change any of the following code,
//     // or do anything to subvert it.
    
//     // create a thread to accept RPC connections
//     go func() {
//       for px.dead == false {
//         conn, err := px.l.Accept()
//         if err == nil && px.dead == false {
//           if px.unreliable && (rand.Int63() % 1000) < 100 {
//             // discard the request.
//             conn.Close()
//           } else if px.unreliable && (rand.Int63() % 1000) < 200 {
//             // process the request but force discard of reply.
//             c1 := conn.(*net.UnixConn)
//             f, _ := c1.File()
//             err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
//             if err != nil {
//               fmt.Printf("shutdown: %v\n", err)
//             }
//             px.rpcCount++
//             go rpcs.ServeConn(conn)
//           } else {
//             px.rpcCount++
//             go rpcs.ServeConn(conn)
//           }
//         } else if err == nil {
//           conn.Close()
//         }
//         if err != nil && px.dead == false {
//           fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
//         }
//       }
//     }()
//   }


//   return px
// }




// // servers[] contains the ports of the set of
// // MapReduceNode servers that will cooperate to 
// // form the fault-tolerant mapreduce service.
// // me is the index of the current server in servers[].
// // 
// func StartServer(servers []string, me int) *ShardMaster {
//   //gob.Register(Op{})
//   // RPC library needs to know how to marshall/unmarshall the different types of Args
 
//   mrn := new(MapReduceServer)
//   mrn.me = me

//   sm.configs = make([]Config, 1)               // initially just a 0th Config
//   sm.configs[0].Groups = map[int64][]string{}  // initialize map
//   sm.operation_number = -1                     // first agreement number is 0

//   rpcs := rpc.NewServer()
//   rpcs.Register(sm)

//   sm.px = paxos.Make(servers, me, rpcs)

//   os.Remove(servers[me])
//   l, e := net.Listen("unix", servers[me]);
//   if e != nil {
//     log.Fatal("listen error: ", e);
//   }
//   sm.l = l

//   // please do not change any of the following code,
//   // or do anything to subvert it.

//   go func() {
//     for sm.dead == false {
//       conn, err := sm.l.Accept()
//       if err == nil && sm.dead == false {
//         if sm.unreliable && (rand.Int63() % 1000) < 100 {
//           // discard the request.
//           conn.Close()
//         } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
//           // process the request but force discard of reply.
//           c1 := conn.(*net.UnixConn)
//           f, _ := c1.File()
//           err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
//           if err != nil {
//             fmt.Printf("shutdown: %v\n", err)
//           }
//           go rpcs.ServeConn(conn)
//         } else {
//           go rpcs.ServeConn(conn)
//         }
//       } else if err == nil {
//         conn.Close()
//       }
//       if err != nil && sm.dead == false {
//         fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
//         sm.Kill()
//       }
//     }
//   }()

//   return sm
// }