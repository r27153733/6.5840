package mr

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	WorkerTmpDir = "tmp-worker" + strconv.Itoa(WorkerId)
	err := os.Mkdir(WorkerTmpDir, 0777)
	if err != nil {
		log.Fatalf("cannot mkdir %s", err.Error())
		return
	}
	for {
		mapSource, reduceSource, isDone := callGetJob()
		if mapSource != nil {
			err = WorkerMap(mapSource, mapf)
			if err != nil {
				log.Println(err)
			}
		} else if reduceSource != nil {
			err = WorkerReduce(reduceSource, reducef)
			if err != nil {
				log.Println(err)
			}
		} else if isDone {
			err = os.RemoveAll(WorkerTmpDir)
			log.Println(err)
			return
		} else {
			time.Sleep(time.Second)
		}
	}
}

//const tmpDir = "./tmp/"

var WorkerId = os.Getpid()
var WorkerTmpDir string

func WorkerMap(mapSource *MapSource, mapf func(string, string) []KeyValue) error {
	var intermediate []KeyValue
	for _, path := range mapSource.FilePaths {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", path)
		}
		kva := mapf(path, string(content))
		intermediate = append(intermediate, kva...)
		_ = file.Close()
	}
	hashKV := make([][]KeyValue, mapSource.NReduce)
	for _, v := range intermediate {
		i := ihash(v.Key) % mapSource.NReduce
		hashKV[i] = append(hashKV[i], v)
	}
	hashKeyFile := make([]string, mapSource.NReduce)
	for i, v := range hashKV {
		filePath := filepath.Join(WorkerTmpDir, "map-"+strconv.Itoa(mapSource.ID)+"-"+strconv.Itoa(i))
		err := SaveObjectToFile(v, filePath)
		if err != nil {
			return err
		}
		hashKeyFile[i] = filePath
	}
	return callSubmitMap(mapSource.ID, hashKeyFile)

}
func WorkerReduce(reduceSource *ReduceSource, reducef func(string, []string) string) error {
	KVs := map[string][]string{}
	for _, path := range reduceSource.FilePaths {
		var arr []KeyValue
		err := LoadObjectFromFile(&arr, path)
		if err != nil {
			return err
		}
		for _, v := range arr {
			KVs[v.Key] = append(KVs[v.Key], v.Value)
		}
	}
	filePath := filepath.Join(WorkerTmpDir, "reduce-"+strconv.Itoa(reduceSource.HashKey))
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	for key, values := range KVs {
		s := reducef(key, values)
		_, err = fmt.Fprintf(file, "%v %v\n", key, s)
		if err != nil {
			return err
		}
	}
	_ = file.Close()
	return callSubmitReduce(reduceSource.HashKey, filePath)
}

var rpcCallFailed = errors.New("rpc call failed")

func callSubmitMap(id int, hashKeyFile []string) error {
	var res SubmitMapReply
	ok := call(RPCFunSubmitMapJob, &SubmitMapArgs{
		ID:          id,
		HashKeyFile: hashKeyFile,
	}, &res)
	if ok {
		return res.Err
	}
	return fmt.Errorf("%w, fun: %s", rpcCallFailed, RPCFunSubmitMapJob)
}
func callSubmitReduce(hashKey int, filePath string) error {
	var res SubmitReduceReply
	ok := call(RPCFunSubmitReduceJob, &SubmitReduceArgs{
		HashKey:  hashKey,
		FilePath: filePath,
	}, &res)
	if ok {
		return res.Err
	}
	return fmt.Errorf("%w, fun: %s", rpcCallFailed, RPCFunSubmitReduceJob)
}

func callGetJob() (mapSource *MapSource, reduceSource *ReduceSource, isDone bool) {
	var res GetJobReply
	ok := call(RPCFunGetJob, struct{}{}, &res)
	if ok {
		return res.MapSource, res.ReduceSource, res.isDone
	}

	return nil, nil, false
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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

// SaveObjectToFile 将对象保存到指定文件中
func SaveObjectToFile(obj any, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(obj)
	if err != nil {
		return err
	}
	return nil
}

// LoadObjectFromFile 从指定文件中加载对象
func LoadObjectFromFile(obj any, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(obj)
	if err != nil {
		return err
	}
	return nil
}
