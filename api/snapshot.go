// SPDX-FileCopyrightText: 2023 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	apiutils "github.com/ironcore-dev/provider-utils/apiutils/api"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Snapshot struct {
	apiutils.Metadata `json:"metadata,omitempty"`

	Source SnapshotSource `json:"source"`

	Status SnapshotStatus `json:"status"`
}

func (s *Snapshot) SetDeletionTimestamp(time *metav1.Time) {
	if time == nil {
		s.DeletedAt = nil // If the provided timestamp is nil, clear the DeletedAt field
		return
	}
	s.DeletedAt = &time.Time
}

type SnapshotState string

const (
	SnapshotStatePending   SnapshotState = "Pending"
	SnapshotStatePopulated SnapshotState = "Populated"
)

type SnapshotStatus struct {
	State             SnapshotState `json:"state"`
	Digest            string        `json:"digest"`
	Size              uint64        `json:"size"`
	LastPopulatedTime metav1.Time   `json:"lastPopulatedTime,omitempty"`
}

type SnapshotSource struct {
	IronCoreImage string `json:"ironcoreImage"`
}
