package testcommon

import (
	"regexp"
	"strings"
)

// StringPatternMatcher is a custom matcher for matching strings against a regex pattern.
type StringPatternMatcher struct {
	pattern *regexp.Regexp
}

func (spm *StringPatternMatcher) Matches(x interface{}) bool {
	str, ok := x.(string)
	return ok && spm.pattern.MatchString(strings.ToLower(str))
}

func (spm *StringPatternMatcher) String() string {
	return "matches string pattern: " + spm.pattern.String()
}

// NewStringPatternMatcher creates a new StringPatternMatcher for the given pattern.
func NewStringPatternMatcher(pattern string) *StringPatternMatcher {
	re := regexp.MustCompile(pattern)
	return &StringPatternMatcher{pattern: re}
}
