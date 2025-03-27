package collector

import (
	"log"
)

type IWorker interface {
	Init() error
	Do(IWorkItem) error
	Retry(error) bool
	Done() error
}

type IWorkerFactory interface {
	MakeWorker(*log.Logger) IWorker
}
