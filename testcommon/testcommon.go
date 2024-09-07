package testcommon

import (
	"bytes"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/collector"
	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

// Simple test fixture with a logger only
type TestFixture struct {
	logger *log.Logger
}

func NewTestFixture(t *testing.T) *TestFixture {
	var f TestFixture
	f.Setup(t)
	return &f
}

func (f *TestFixture) Setup(t *testing.T) {
	f.logger = TestLogger(t.Name())
}

func (f *TestFixture) Teardown(t *testing.T) {
	f.logger.Printf("teardown test %s", t.Name())
}
func (f *TestFixture) Logger() *log.Logger {
	return f.logger
}

// Text fixture with pg db loader
type PGTestFixture struct {
	loader dbloader.DBLoader
	schema string
	TestFixture
}

func NewPGTestFixture(t *testing.T) *PGTestFixture {
	f := PGTestFixture{schema: "sdc_test"}
	f.Setup(t)
	return &f
}

func (f *PGTestFixture) Setup(t *testing.T) {
	f.logger = TestLogger(t.Name())
	f.logger.Printf("Test setup - %s", t.Name())
	f.loader = dbloader.NewPGLoader(f.schema, f.logger)
	f.loader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	f.loader.DropSchema(f.schema)
	f.loader.CreateSchema(f.schema)
}

func (f *PGTestFixture) Teardown(t *testing.T) {
	f.logger.Printf("Test teardown - %s", t.Name())

	defer f.loader.Disconnect()
	f.logger.Printf("Drop schema %s if exists.", f.schema)
	f.loader.DropSchema(f.schema)
}

func (f *PGTestFixture) Loader() dbloader.DBLoader {
	return f.loader
}

// Text fixture with db and cache mock
type MockTestFixture struct {
	mockCtl   *gomock.Controller
	dbMock    *dbloader.MockDBLoader
	cacheMock *cache.MockICacheManager
	logger    *log.Logger
	reader    collector.IHttpReader
	exporter  collector.IDataExporter
}

func NewMockTestFixture(t *testing.T) *MockTestFixture {
	var f MockTestFixture
	f.Setup(t)
	return &f
}

func (f *MockTestFixture) Setup(t *testing.T) {
	f.logger = TestLogger(t.Name())
	f.logger.Printf("setup test %s", t.Name())
	f.mockCtl = gomock.NewController(t)
	f.dbMock = dbloader.NewMockDBLoader(f.mockCtl)

	f.dbMock.EXPECT().CreateSchema(config.SchemaName).AnyTimes()
	f.dbMock.EXPECT().
		Exec(NewStringPatternMatcher(strings.ToLower("SET search_path TO " + config.SchemaName))).
		AnyTimes()
	f.dbMock.EXPECT().Disconnect().AnyTimes()

	f.cacheMock = cache.NewMockICacheManager(f.mockCtl)
	f.cacheMock.EXPECT().Connect().AnyTimes()
	f.cacheMock.EXPECT().Disconnect().AnyTimes()

	f.reader = collector.NewHttpReader(collector.NewLocalClient())

	f.exporter = collector.NewDBExporter(f.dbMock, config.SchemaName)
}
func (f *MockTestFixture) Teardown(t *testing.T) {
	f.logger.Printf("teardown test %s", t.Name())
	f.mockCtl.Finish()
}
func (f *MockTestFixture) DBExpect() *dbloader.MockDBLoaderMockRecorder {
	return f.dbMock.EXPECT()
}
func (f *MockTestFixture) CacheExpect() *cache.MockICacheManagerMockRecorder {
	return f.cacheMock.EXPECT()
}
func (m *MockTestFixture) DBMock() *dbloader.MockDBLoader {
	return m.dbMock
}
func (m *MockTestFixture) CacheMock() *cache.MockICacheManager {
	return m.cacheMock
}
func (m *MockTestFixture) Logger() *log.Logger {
	return m.logger
}
func (m *MockTestFixture) Reader() collector.IHttpReader {
	return m.reader
}
func (m *MockTestFixture) Exporter() collector.IDataExporter {
	return m.exporter
}

func SetupTest(testName string) {
}

func TeardownTest() {
}

func TestLogger(testName string) *log.Logger {
	logFile := "logs/" + testName + ".log"
	os.Remove(logFile)
	file, _ := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	testLogger := log.New(file, "commontest: ", log.Ldate|log.Ltime)
	// Redirect stdout and stderr to log file
	os.Stdout = file
	os.Stderr = file
	sdclogger.SDCLoggerInstance = sdclogger.NewSDCLoggerByFile(file)
	return testLogger
}

func RunReidsCliCommand(redisCmd string) {
	// Create the command
	cmd := exec.Command("redis-cli", "-h", os.Getenv("REDISHOST"))

	// Create a pipe to write commands to redis-cli
	stdin, err := cmd.StdinPipe()
	if err != nil {
		sdclogger.SDCLoggerInstance.Fatalf("Failed to create stdin pipe: %v", err)
	}

	// Create a buffer to capture the output
	var out bytes.Buffer
	cmd.Stdout = &out

	// Start the command
	if err := cmd.Start(); err != nil {
		sdclogger.SDCLoggerInstance.Fatalf("Failed to start command: %v", err)
	}

	// Write commands to the pipe
	_, err = stdin.Write([]byte(redisCmd))
	if err != nil {
		sdclogger.SDCLoggerInstance.Fatalf("Failed to write to stdin: %v", err)
	}
	stdin.Close() // Close stdin to indicate that we are done sending input

	// Wait for the command to finish
	if err := cmd.Wait(); err != nil {
		sdclogger.SDCLoggerInstance.Fatalf("Command failed: %v", err)
	}

	// Print the output
	sdclogger.SDCLoggerInstance.Printf("Output:\n%s\n", out.String())
}

func GetProxy() (string, error) {
	proxies, err := os.ReadFile(os.Getenv("SDC_HOME") + "/data/proxies100.txt")
	if err != nil {
		return "", err
	}
	return strings.Split(string(proxies), "\n")[0], nil
}
