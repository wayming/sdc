package collector

import (
	"fmt"
	"os/exec"
	"regexp"
	"strconv"

	"github.com/wayming/sdc/sdclogger"
)

const WGET_EXIT_CODE_SEVER_ERROR = 8
const WGET_EXIT_CODE_SUCCESSFUL = 0

var WGET_EXIT_CODE_MAP = map[int]string{
	WGET_EXIT_CODE_SUCCESSFUL:  "Download Successful",
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

func Ternary[T any](condition bool, trueVal, falseVal T) T {
	if condition {
		return trueVal
	}
	return falseVal
}

func NewWgetCmd(arg ...string) *WgetCmd {
	cmd := exec.Command("wget", arg...)
	cmdOutput, cmdError := cmd.CombinedOutput()
	var cmdErrorStr string
	if cmdError != nil {
		cmdErrorStr = cmdError.Error()
	}
	cmdExitCode := cmd.ProcessState.ExitCode()
	httpCode := getHttpCode(string(cmdOutput))
	errorMessage := fmt.Sprintf(
		"wget command %s. Error: %s. Exit code: %d. Http Code: %d",
		cmd.String(), cmdErrorStr, cmdExitCode, httpCode)
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
	pattern := "\\s*HTTP\\/[0-9]\\.[0-9]\\s*([0-9]+)\\s+.*"
	regExp, err := regexp.Compile(pattern)
	if err != nil {
		sdclogger.SDCLoggerInstance.Panicf("Failed to compile regular expression %s", pattern)
	}
	matches := regExp.FindAllStringSubmatch(output, -1)
	if len(matches) <= 0 {
		sdclogger.SDCLoggerInstance.Printf("No match for patern %s from output %s", pattern, output)
		return 0
	}

	lastMatch := matches[len(matches)-1]
	if code, err := strconv.Atoi(lastMatch[1]); err != nil {
		return 0
	} else {
		return code
	}
}

// Get the redirected url
// Return empty string if redirected url is not found.
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
				return "", fmt.Errorf("failed to find pattern %s from output %s", pattern, c.cmdOutput)
			}
			return match[1], nil
		}
		return "", c.WgetError()
	} else {
		return "", fmt.Errorf("no redirected url found")
	}
}

func (c *WgetCmd) GetErrorMessage() string {
	return c.errorMessage
}

func (c *WgetCmd) WgetError() error {
	return NewWgetError(c.errorMessage, c.cmdExitCode)
}

func (c *WgetCmd) HttpServerError() error {
	return NewHttpServerError(c.httpCode, nil, c.errorMessage)
}

func (c *WgetCmd) Succeeded() bool {
	return c.cmdExitCode == WGET_EXIT_CODE_SUCCESSFUL
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
