package collector_test

import (
	"log"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/wayming/sdc/dbloader"
)

var saTestLogger *log.Logger
var saMockCtl *gomock.Controller
var saDBMock *dbloader.MockDBLoader

func TestSACollector_MapRedirectedSymbol(t *testing.T) {

}

// func TestMain(m *testing.M) {
// }
