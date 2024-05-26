package collector

import (
	"fmt"
	"os/exec"
	"regexp"
	"strconv"

	"github.com/wayming/sdc/sdclogger"
)

const WGET_EXIT_CODE_SEVER_ERROR = 8

var WGET_EXIT_CODE_MAP = map[int]string{
	WGET_EXIT_CODE_SEVER_ERROR: "Server Error",
}

const HTTP_ERROR_NOT_FOUND = 404
const HTTP_ERROR_REDIRECTED = 301

var HTTP_ERROR_MAP = map[int]string{
	HTTP_ERROR_NOT_FOUND:  "Not Found",
	HTTP_ERROR_REDIRECTED: "Moved Permanently",
}

type WgetCmd struct {
	*exec.Cmd
	cmdOutput    string
	cmdError     error
	cmdExitCode  int
	httpCode     int
	errorMessage string
}

func NewWgetCmd(arg ...string) *WgetCmd {
	cmd := exec.Command("wget", arg...)
	cmdOutput, cmdError := cmd.CombinedOutput()
	cmdExitCode := cmd.ProcessState.ExitCode()
	httpCode := getHttpCode(string(cmdOutput))
	errorMessage := fmt.Sprintf(
		"wget command %s. Error: %s. Exit code: %d. Http Code: %d",
		cmd.String(), cmdError.Error(), cmdExitCode, httpCode)
	return &WgetCmd{
		cmd,
		string(cmdOutput),
		cmdError,
		cmdExitCode,
		httpCode,
		errorMessage,
	}
}
func getHttpCode(output string) int {
	pattern := "\\s*HTTP/[0-9]\\.[0-9]\\s*([0-9]+)\\s+.*"
	regExp, err := regexp.Compile(pattern)
	if err != nil {
		sdclogger.SDCLoggerInstance.Panicf("Failed to compile regular expression %s", pattern)
	}
	match := regExp.FindStringSubmatch(output)
	if len(match) <= 0 {
		sdclogger.SDCLoggerInstance.Println()
	}
	if code, err := strconv.Atoi(match[1]); err != nil {
		return 0
	} else {
		return code
	}
}

func (c *WgetCmd) RedirectedUrl() (string, error) {
	if c.cmdError != nil {
		if c.cmdExitCode == WGET_EXIT_CODE_SEVER_ERROR {
			// Has redirect
			pattern := "\\s*Location:\\s*(.*)"
			regExp, err := regexp.Compile(pattern)
			if err != nil {
				return "", err
			}
			match := regExp.FindStringSubmatch(c.cmdOutput)
			if len(match) <= 0 {
				return "", fmt.Errorf("failed to find patthern %s from output %s", pattern, c.cmdOutput)
			}
			return match[1], nil
		}
		return "", fmt.Errorf("failed to run command %s. Error: %s. Exit code: %s", c.Cmd.String(), c.cmdError.Error(), WGET_EXIT_CODE_MAP[c.cmdExitCode])
	}
	return "", fmt.Errorf("no redirected url found. wget command returns %d", c.cmdExitCode)

}

func (c *WgetCmd) GetErrorMessage() string {
	return c.errorMessage
}

func (c *WgetCmd) GetWgetError() error {
	return c.cmdError
}

func (c *WgetCmd) HasServerError() bool {
	return c.cmdExitCode == WGET_ERROR_CODE_SERVER_ERROR
}

func (c *WgetCmd) HasNetworkError() bool {
	return c.cmdExitCode == WGET_ERROR_CODE_NETWORK
}

func (c *WgetCmd) HasServerNotFoundError() bool {
	return c.httpCode == HTTP_ERROR_NOT_FOUND
}

func (c *WgetCmd) HasServerRedirectedError() bool {
	return c.httpCode == HTTP_ERROR_REDIRECTED
}
