package omap_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// TestOmap runs the Ginkgo test suite for the omap package.
func TestOmap(t *testing.T) {
	// RegisterFailHandler connects Ginkgo's Fail function to Gomega.
	// When a Gomega assertion fails, Ginkgo's Fail function will be called.
	RegisterFailHandler(Fail)
	// RunSpecs runs the Ginkgo specs found in this package.
	RunSpecs(t, "Omap Suite")
}
