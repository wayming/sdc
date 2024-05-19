package collector

import "errors"

const WGET_ERROR_CODE_NETWORK = int(4)
const WGET_ERROR_CODE_SERVER_ERROR = int(8)

// WgetError represents a custom error type with a status code.
type WgetError struct {
	text   string
	status int
}

// NewWgetError creates a new WgetError instance with the given error message and status code.
func NewWgetError(errorMsg string, wgetStatusCode int) WgetError {
	return WgetError{
		text:   errorMsg,
		status: wgetStatusCode,
	}
}

// Error returns the message body associated with the WgetError instance.
func (e WgetError) Error() string {
	return e.text
}

// StatusCode returns the status code associated with the WgetError instance.
func (e WgetError) StatusCode() int {
	return e.status
}

// HttpServerError represents a custom error type with a status code.
type HttpServerError struct {
	text   string
	status int
}

// NewHttpServerError creates a new HttpServerError instance with the given error message and status code.
func NewHttpServerError(errorMsg string, httpStatusCode int) HttpServerError {
	return HttpServerError{
		text:   errorMsg,
		status: httpStatusCode,
	}
}

// Error returns the message body associated with the HttpServerError instance.
func (e HttpServerError) Error() string {
	return e.text
}

// StatusCode returns the status code associated with the HttpServerError instance.
func (e HttpServerError) StatusCode() int {
	return e.status
}

func NewCollectorError(e error, msg string) error {
	switch etype := e.(type) {
	case WgetError:
		return NewWgetError(msg, etype.StatusCode())
	case HttpServerError:
		return NewHttpServerError(msg, etype.StatusCode())
	default:
		return errors.New(msg + " Error: " + e.Error())
	}
}
