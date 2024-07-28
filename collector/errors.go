package collector

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
	hd     map[string][]string
}

// NewHttpServerError creates a new HttpServerError instance with the given error message and status code.
func NewHttpServerError(status int, header map[string][]string, errorMsg string) HttpServerError {
	return HttpServerError{
		text:   errorMsg,
		status: status,
		hd:     header,
	}
}

// StatusCode returns the status code associated with the HttpServerError instance.
func (e HttpServerError) StatusCode() int {
	return e.status
}

// StatusCode returns the status code associated with the HttpServerError instance.
func (e HttpServerError) ResponseHeader() map[string][]string {
	return e.hd
}

// Error returns the message body associated with the HttpServerError instance.
func (e HttpServerError) Error() string {
	return e.text
}

// func NewCollectorError(e error, msg string) error {
// 	fullMessage := msg + " Error: " + e.Error()
// 	switch etype := e.(type) {
// 	case WgetError:
// 		return NewWgetError(fullMessage, etype.StatusCode())
// 	case HttpServerError:
// 		return NewHttpServerError(etype.StatusCode(), etype.ResponseHeader(), fullMessage)
// 	default:
// 		return errors.New(fullMessage)
// 	}
// }
