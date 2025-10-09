// SPDX-FileCopyrightText: 2023 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"fmt"

	"github.com/go-logr/logr"
)

// Recover handles panics and logs the error with the full stack trace.
func Recover(log logr.Logger, panicCatcher string) {
	if r := recover(); r != nil {
		LogPanic(log, r, panicCatcher)
	}
}

func LogPanic(log logr.Logger, r interface{}, panicCatcher string) {
	log.Error(fmt.Errorf("%v", r), "caught panic", "panicCatcher", panicCatcher)
}
