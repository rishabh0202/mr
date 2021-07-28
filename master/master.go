/*
param-1: number of mappers (4)
param-2: number of reducers (4)
param-3: text files (pg-*.txt)
*/
package main
import(
	"os"
	"sync"
	"fmt"
	"log"
	"strconv"
	"os/exec"
	"math"
)
//0->unallocated
//1->allocated
//2->finsihed

type  Master struct{
	mappers map[string]string
	reducers map[string]string
	rawfiles []string
	numRawFiles int
	numMappers int
	numReducers int
	RWLock *sync.RWMutex
}

func main(){
	validate()
	
	m := _init()
		
	go triggerMappers(m)
	done := false
	for !done{
		done=checkAllMapTask(m)
	}
	log.Println("Mapping Completed!!")
	triggerReducers(m)
	done = false
	for !done{
		done=checkAllReduceTask(m)
	}
	log.Println("Reducing Completed!!")
	//deleteIntermediateFiles(m)
}

func validate()bool{
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Input parameters are missing\n")
		os.Exit(1)
		return false
	}
	return true
}

func _init() *Master{
	m := Master{}
	var err error
	m.mappers = make(map[string]string)
	m.reducers=make(map[string]string)
	m.RWLock = new(sync.RWMutex)
	for _, filename := range os.Args[3:]{
		m.mappers[filename]="0"
	}
	m.rawfiles=os.Args[3:]
	m.numRawFiles=len(os.Args)-3
	m.numMappers, err = strconv.Atoi(os.Args[1])
	if(err!=nil){
		fmt.Println(err)
	}
	m.numReducers, err = strconv.Atoi(os.Args[2])
	for i := 0; i < m.numReducers; i++ {
		m.reducers[strconv.Itoa(i)]="0"	
	}
	if(err!=nil){
		fmt.Println(err)
	}
	return &m
}

func triggerMappers(m *Master){
	typeOfJob:="Map"
	buckets := m.numMappers
	eachBucketFiles := int(math.Ceil(float64(m.numRawFiles)/float64(m.numMappers)))
	for i := 0; i < buckets; i++ {
		m.RWLock.Lock()
		for j:=i*eachBucketFiles; j<min((i+1)*eachBucketFiles, m.numRawFiles);j++{
			log.Println(m.rawfiles[j])
			m.mappers[m.rawfiles[j]]="1"
		}
		m.RWLock.Unlock()
		go trackMapper(m, m.rawfiles[i*eachBucketFiles:min((i+1)*eachBucketFiles, m.numRawFiles)], typeOfJob, i)	
	}
}

func triggerReducers(m *Master){
	typeOfJob:="Reduce"
	for i := 0; i < m.numReducers; i++ {
		m.RWLock.Lock()
		m.reducers[strconv.Itoa(i)]="1"
		m.RWLock.Unlock()
		go trackReducer(m, i, typeOfJob)	
	}
}

func trackMapper(m *Master, filenames []string, typeOfJob string, mapNumber int){
	var param []string
	param = append(param, typeOfJob)
	param = append(param, strconv.Itoa(mapNumber))
	param = append(param, strconv.Itoa(m.numReducers))
	param = append(param, filenames...)
	cmd := exec.Command("./worker", param...)
	stdout, err := cmd.Output()
	if err != nil {
        	fmt.Println(err.Error())
        	return
    	}
    	log.Println(string(stdout))
    	m.RWLock.Lock()
    	for i := 0; i < len(filenames); i++{
		m.mappers[filenames[i]]="2"
	}
	m.RWLock.Unlock()
}

func trackReducer(m *Master, reducerNum int, typeOfJob string){
	var param []string
	param = append(param, typeOfJob)
	param = append(param, strconv.Itoa(reducerNum))
	cmd := exec.Command("./worker", param...)
	stdout, err := cmd.Output()
	if err != nil {
        	fmt.Println(err.Error())
        	return
    	}
    	log.Println(string(stdout))
    	m.RWLock.Lock()
    	m.reducers[strconv.Itoa(reducerNum)]="2"
	m.RWLock.Unlock()
}

func checkAllMapTask(m *Master) bool {
	m.RWLock.RLock()
	defer m.RWLock.RUnlock()
	for _,v := range m.rawfiles {
		if m.mappers[v] != "2" {
			return false
		}
	}
	return true
}

func checkAllReduceTask(m *Master) bool {
	m.RWLock.RLock()
	defer m.RWLock.RUnlock()
	for i := 0; i < m.numReducers; i++{
		if m.reducers[strconv.Itoa(i)] != "2"{
			return false
		}
	}
	return true
}

func deleteIntermediateFiles(m *Master){
	m.RWLock.RLock()
	defer m.RWLock.RUnlock()
	for i := 0; i < m.numReducers; i++{
		err := os.RemoveAll(strconv.Itoa(i))
    		if err != nil {
        		log.Fatal(err)
    		}
	}
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}
