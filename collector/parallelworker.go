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
	wb       IWorkerBuilder
	wim      IWorkItemManager
	nThreads int
}

type IWorkItem interface {
	str() string
}

type IWorkItemManager interface {
	Next() (IWorkItem, error)
	Remove(IWorkItem) error
	Size() int
	HandleError(error)
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

	pw.wb.WithLogger(logger)
	worker, err := pw.wb.NewWorker()

	for err == nil {

		if err := worker.Init(); err != nil {
			logMessage(err.Error())
			outChan <- Response{nil, err}
			return
		}

		complete := false
		for {
			var r Request
			select {
			case r = <-inChan:
				logMessage("Begin processing [" + r.wi.str() + "]")
			default:
				logMessage("All work items are processed")
				complete = true
			}

			if complete {
				break
			}

			if err := worker.Do(r.wi); err != nil {
				logMessage(err.Error())
				outChan <- Response{r.wi, err}
				logMessage("End processing [" + r.wi.str() + "].")
				break
			} else {
				outChan <- Response{r.wi, nil}
				logMessage("End processing [" + r.wi.str() + "]. Succeeded.")
				continue
			}
		}

		if err := worker.Done(); err != nil {
			outChan <- Response{nil, err}
		}

		if !complete {
			worker, err = pw.wb.NewWorker()
		}

		if complete {
			break
		}
	}

	logMessage("Finish")
}
func (pw *ParallelWorker) Execute(parallel int) error {

	nAll := pw.wim.Size()
	summary := "\nResults Summary:\n"

	var wg sync.WaitGroup
	inChan := make(chan Request, 1000*1000)
	outChan := make(chan Response, 1000*1000)

	// Push workitem to channel
	go func() {
		for {
			wi, err := pw.wim.Next()
			if err != nil {
				break // Exit on error
			}

			sdclogger.SDCLoggerInstance.Printf("Push %s into [input] channel.", wi.str())
			inChan <- Request{wi}
		}
	}()
	defer close(inChan) // Close the inChan when done

	// Start goroutine
	i := 0
	for ; i < parallel; i++ {
		wg.Add(1)
		go pw.workerRoutine(strconv.Itoa(i), inChan, outChan, &wg)
	}

	// Cleanup
	go func() {
		wg.Wait()
		close(outChan)
	}()

	// Handle PCResponse
	nProcessed := 0
	nSucceeded := 0
	for resp := range outChan {
		nProcessed++
		if resp.err != nil {
			sdclogger.SDCLoggerInstance.Printf("Failed to process work item %s. Error %s", resp.wi.str(), resp.err)
			pw.wim.HandleError(resp.err)
		} else {
			nSucceeded++
		}

		fmt.Printf("Total %d, Processed %d, succeeded %d\n", nAll, nProcessed, nSucceeded)
	}

	sdclogger.SDCLoggerInstance.Println(summary)
	sdclogger.SDCLoggerInstance.Println(pw.wim.Summary())

	return nil
}
