package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

	mapSourceChan          chan *MapSource
	reduceSourceChan       chan *ReduceSource
	done                   chan struct{}
	submitLock             sync.RWMutex
	hashKeyFiles           [][]string
	finishedMapJob         []bool
	finishedMapJobCount    int
	finishedReduceJob      []bool
	finishedReduceJobCount int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RPCSubmitMap(args *SubmitMapArgs, reply *SubmitMapReply) error {
	reply.Err = c.submitMap(args.ID, args.HashKeyFile)
	return nil
}

func (c *Coordinator) RPCSubmitReduce(args *SubmitReduceArgs, reply *SubmitReduceReply) error {
	reply.Err = c.submitReduce(args.HashKey, args.FilePath)
	return nil
}

func (c *Coordinator) RPCGetJob(_ struct{}, reply *GetJobReply) error {
	mapSource, reduceSource, isDone := c.getJob()
	reply.MapSource = mapSource
	reply.ReduceSource = reduceSource
	reply.isDone = isDone
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	m := 5
	c.finishedMapJob = make([]bool, m)
	c.finishedReduceJob = make([]bool, nReduce)
	c.hashKeyFiles = make([][]string, nReduce)
	array := splitArray(files, m)
	c.mapSourceChan = make(chan *MapSource, len(files))
	c.reduceSourceChan = make(chan *ReduceSource, nReduce)
	c.done = make(chan struct{})
	for i, strings := range array {
		c.mapSourceChan <- &MapSource{
			ID:        i,
			FilePaths: strings,
			NReduce:   nReduce,
		}
	}

	c.server()
	return &c
}

const (
	Map = iota
	Reduce
)

type MapSource struct {
	ID        int
	FilePaths []string
	NReduce   int
}
type ReduceSource struct {
	HashKey   int
	FilePaths []string
}

func (c *Coordinator) submitMap(id int, hashKeyFile []string) error {
	c.submitLock.Lock()
	defer c.submitLock.Unlock()
	if c.finishedMapJob[id] {
		return errors.New("duplicate map submission")
	}
	c.finishedMapJob[id] = true

	for i, file := range hashKeyFile {
		if file != "" {
			c.hashKeyFiles[i] = append(c.hashKeyFiles[i], file)
		}
	}
	c.finishedMapJobCount++
	if len(c.finishedMapJob) == c.finishedMapJobCount {
		go func() {
			close(c.mapSourceChan)
			log.Println("map ok")
			for id, strings := range c.hashKeyFiles {
				c.reduceSourceChan <- &ReduceSource{
					HashKey:   id,
					FilePaths: strings,
				}
			}
		}()
	}
	return nil
}

func (c *Coordinator) submitReduce(hashKey int, filePath string) error {
	c.submitLock.Lock()
	defer c.submitLock.Unlock()
	if c.finishedReduceJob[hashKey] {
		return errors.New("duplicate reduce submission")
	}
	name := fmt.Sprintf("mr-out-%v.txt", hashKey)
	err := os.Rename(filePath, name)
	if err != nil {
		return err
	}
	c.finishedReduceJob[hashKey] = true
	c.finishedReduceJobCount++
	if len(c.finishedReduceJob) == c.finishedReduceJobCount {
		close(c.reduceSourceChan)
		go func() {
			log.Println("reduce ok")
			c.done <- struct{}{}
		}()
	}
	return nil
}

func (c *Coordinator) getJob() (mapSource *MapSource, reduceSource *ReduceSource, isDone bool) {
	select {
	case mapSource, ok := <-c.mapSourceChan:
		if ok {
			return mapSource, nil, false
		}
	case reduceSource, ok := <-c.reduceSourceChan:
		if ok {
			return nil, reduceSource, false
		} else {
			return nil, nil, true
		}
	default:
	}
	return nil, nil, false
}

// 平均分配数组元素到指定数量的切片
func splitArray[T any](arr []T, num int) [][]T {
	avg := len(arr) / num
	remainder := len(arr) % num
	result := make([][]T, num)
	idx := 0
	for i := 0; i < num; i++ {
		length := avg
		if remainder > 0 {
			length++
			remainder--
		}
		result[i] = arr[idx : idx+length]
		idx += length
	}
	return result
}
