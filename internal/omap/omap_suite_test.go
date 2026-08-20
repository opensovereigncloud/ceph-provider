// Copyright 2026 IronCore authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
