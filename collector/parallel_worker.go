package collector

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/wayming/sdc/sdclogger"
)

type ParallelWorker struct {
	wFac IWorkerFactory
	wim  IWorkItemManager
}

type IWorkItem interface {
	ToString() string
}

type IWorkItemManager interface {
	Prepare() error
	Next() (IWorkItem, error)
	Size() int64
	OnProcessError(IWorkItem, error) error
	OnProcessSuccess(IWorkItem) error
	Summary() string
}

type Request struct {
	wi IWorkItem
}
type Response struct {
	wi  IWorkItem
	err error
}

func (pw *ParallelWorker) workerRoutine(
	goID string,
	inChan chan Request,
	outChan chan Response,
	wg *sync.WaitGroup,
) {

	defer wg.Done()

	// Logger
	file, _ := os.OpenFile(LOG_FILE+"."+goID, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	logger := log.New(file, "sdc: ", log.Ldate|log.Ltime)
	defer file.Close()
	logMessage := func(text string) {
		logger.Println("[Go" + goID + "] " + text)
	}
	logMessage("Begin")

	worker := pw.wFac.MakeWorker(logger)
	var err error
	for err == nil {
		if err := worker.Init(); err != nil {
			logMessage(err.Error())
			outChan <- Response{nil, err}
			return
		}
		complete := false
		for {
			r, ok := <-inChan
			if !ok {
				logMessage("All work items are processed")
				complete = true
				break
			} else {
				logMessage("Begin processing [" + r.wi.ToString() + "]")
			}

			if err := worker.Do(r.wi); err != nil {
				logMessage(err.Error())
				if worker.Retry(err) {
					logMessage("Recreate worker for processing.")
					break
				}
				outChan <- Response{r.wi, err}
				logMessage("End processing [" + r.wi.ToString() + "].")
			} else {
				outChan <- Response{r.wi, nil}
				logMessage("End processing [" + r.wi.ToString() + "]. Succeeded.")
			}
		}

		if complete {
			break
		} else {
			if err := worker.Done(); err != nil {
				outChan <- Response{nil, err}
			}
			worker = pw.wFac.MakeWorker(logger)
		}

	}

	logMessage("Finish")
}
func (pw *ParallelWorker) Execute(parallel int) error {

	if err := pw.wim.Prepare(); err != nil {
		sdclogger.SDCLoggerInstance.Printf("Failed to prepare work items. Error: %v", err)
	}

	nAll := pw.wim.Size()
	summary := "Results Summary:\n"

	var wg sync.WaitGroup
	inChan := make(chan Request, 1000*1000)
	outChan := make(chan Response, 1000*1000)

	sdclogger.SDCLoggerInstance.Printf(
		"Parallel works begin, process %d work items with %d threads", pw.wim.Size(), parallel)

	// Push workitem to channel
	go func() {
		for {
			wi, err := pw.wim.Next()
			if wi == nil {
				if err == nil {
					sdclogger.SDCLoggerInstance.Printf("Sent all work items to [input] channel.")
				} else {
					sdclogger.SDCLoggerInstance.Printf("Failed to get the next work item. Error: %v.", err)
				}
				break
			}
			sdclogger.SDCLoggerInstance.Printf("Push %s into [input] channel.", wi.ToString())
			inChan <- Request{wi}
		}
		defer close(inChan) // Close the inChan when done
	}()

	// Start goroutine
	i := 0
	for ; i < parallel; i++ {
		wg.Add(1)
		go pw.workerRoutine(strconv.Itoa(i), inChan, outChan, &wg)
	}

	// Cleanup
	go func() {
		wg.Wait()
		sdclogger.SDCLoggerInstance.Printf("Close [output] channel")
		close(outChan)
	}()

	// Handle PCResponse
	nProcessed := 0
	nSucceeded := 0
	for resp := range outChan {
		nProcessed++
		if resp.err != nil {
			sdclogger.SDCLoggerInstance.Printf("Failed to process work item %s. Error %s", resp.wi.ToString(), resp.err)
			pw.wim.OnProcessError(resp.wi, resp.err)
		} else {
			nSucceeded++
		}

		fmt.Printf("Total %d, Processed %d, Succeeded %d\n", nAll, nProcessed, nSucceeded)
	}

	sdclogger.SDCLoggerInstance.Println(summary)
	sdclogger.SDCLoggerInstance.Println(pw.wim.Summary())

	return nil
}
