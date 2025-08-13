package other_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestOther(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Other Suite")
}
