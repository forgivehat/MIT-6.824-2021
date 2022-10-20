package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply := fetchTask()
		//log.Printf("Worker: recieve the reply %v form Coordinator\n", reply)
		switch reply.JobType {
		case MapJob:
			doMapJob(reply, mapf)
		case ReduceJob:
			doReduceJob(reply, reducef)
		case WaitJob:
			time.Sleep(time.Second * 1)
		case CompleteJob:
			return
		default:
			panic("unexpected branch...\n")
		}
	}
}

func fetchTask() *HeartbeatReply {
	args := HeartbeatArgs{}
	reply := HeartbeatReply{}
	call("Coordinator.Request", &args, &reply)
	return &reply
}

func reportTask(args JobFinishArgs) {
	reply := JobFinishReply{}
	call("Coordinator.Report", &args, &reply)
}

/*
worker收到master的reply后，如果拿到一份mapJob，打开reply里指定了文件名的文件,
读取文件内容，调用mapf函数处理文件内容得到形如
add 1
apple 2
bad 2
这样的键值对数组（[]KeyValue），再将其中的键值对按哈希值写入不同的中间文件mr-x-x,前者为mapId, 由reply指定，
后者由ihash(key) % NReduce得出, NReduce由master设置为固定大小，本实验设置为输入文件数量
*/
func doMapJob(reply *HeartbeatReply, mapf func(string, string) []KeyValue) {
	fileName := reply.Filename
	content, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	kva := mapf(fileName, string(content))
	intermediates := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		index := ihash(kv.Key) % reply.NReduce
		intermediates[index] = append(intermediates[index], kv)
	}
	var wg sync.WaitGroup
	for index, intermediate := range intermediates {
		wg.Add(1)
		go func(index int, intermediate []KeyValue) {
			defer wg.Done()
			intermediateFilePath := generateMapResultFileName(reply.Id, index)
			tmpFile, err := os.CreateTemp(".", intermediateFilePath)
			if err != nil {
				log.Fatalf("cannot create tmp file %v", intermediateFilePath)
			}
			enc := json.NewEncoder(tmpFile)
			for _, kv := range intermediate {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode json %v", kv.Key)
				}
			}
			if err := os.Rename(tmpFile.Name(), intermediateFilePath); err != nil {
				fmt.Errorf("cannot replace %q with tempfile %q: %v",
					intermediateFilePath, tmpFile.Name(), err)
			}
		}(index, intermediate)
	}
	wg.Wait()
	reportTask(JobFinishArgs{reply.Id, MapPhase})

}

func doReduceJob(reply *HeartbeatReply, reducef func(string, []string) string) {
	var kva []KeyValue
	for i := 0; i < reply.NMap; i++ {
		filePath := generateMapResultFileName(i, reply.Id)
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("cannot open %v", filePath)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	results := make(map[string][]string)
	for _, kv := range kva {
		results[kv.Key] = append(results[kv.Key], kv.Value)
	}

	reduceFileName := generateReduceResultFileName(reply.Id)
	tmpFile, err := os.CreateTemp(".", reduceFileName)
	if err != nil {
		log.Fatalf("cannot create tmp file %v", reduceFileName)
	}
	for key, values := range results {
		output := reducef(key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", key, output)
	}
	if err := os.Rename(tmpFile.Name(), reduceFileName); err != nil {
		fmt.Errorf("cannot replace %q with tempfile %q: %v",
			reduceFileName, tmpFile.Name(), err)
	}
	reportTask(JobFinishArgs{reply.Id, ReducePhase})
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
