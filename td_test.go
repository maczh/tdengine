package tdengine

import (
	"fmt"
	"testing"
)

func TestCase(t *testing.T) {
	s := NewCase().Case("status").When(1, "正常").When(2, "暂停").When(3, "已过期").Else("状态错误").ToString()
	fmt.Println(s)
}
