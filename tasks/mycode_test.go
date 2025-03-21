// mycode.go
package mycode

import (
	"fmt"
	"testing"
)

func Function1() {
	fmt.Println("Executing Function1")
}

func Function2() {
	fmt.Println("Executing Function2")
}

func Function3() {
	fmt.Println("Executing Function3")
}

func TestFunction1(t *testing.T) {
	Function1()
}

func TestFunction2(t *testing.T) {
	Function2()
}

func TestFunction3(t *testing.T) {
	Function3()
}
