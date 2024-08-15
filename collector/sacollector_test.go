package collector_test

import (
	"log"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/wayming/sdc/dbloader"
	testcommon "github.com/wayming/sdc/utils"
)

var saTestLogger *log.Logger
var saMockCtl *gomock.Controller
var saDBMock *dbloader.MockDBLoader

func TestSACollector_MapRedirectedSymbol(t *testing.T) {
	t.Run(tt.name, func(t *testing.T) {
		c := &SACollector{
			loader:        tt.fields.loader,
			reader:        tt.fields.reader,
			logger:        tt.fields.logger,
			htmlParser:    tt.fields.htmlParser,
			metricsFields: tt.fields.metricsFields,
			thisSymbol:    tt.fields.thisSymbol,
		}
		got, err := c.MapRedirectedSymbol(tt.args.symbol)
		if (err != nil) != tt.wantErr {
			t.Errorf("SACollector.MapRedirectedSymbol() error = %v, wantErr %v", err, tt.wantErr)
			return
		}
		if got != tt.want {
			t.Errorf("SACollector.MapRedirectedSymbol() = %v, want %v", got, tt.want)
		}
	})

}

func setupSATest(t *testing.T) {
	saTestLogger = testcommon.TestLogger(t.Name())

	saMockCtl = gomock.NewController(t)
	yfDBMock = dbloader.NewMockDBLoader(saMockCtl)

}

func teardownSATest(t *testing.T) {
	saMockCtl.Finish()
}

// func TestMain(m *testing.M) {
// }
