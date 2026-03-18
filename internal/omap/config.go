// SPDX-FileCopyrightText: 2023 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package omap

const (
	NameVolumes   = "ironcore.csi.volumes"
	NameSnapshots = "ironcore.csi.snapshots"
	// LabelsAnnotationKey is the annotation key used to store volume labels in Ceph OMAP
	LabelsAnnotationKey = "labels.ironcore.ceph/volume"

	// Performance and resource constants
	GetAllOmapValuesLimit = 1000000 // GetAllOmapValuesLimit prevents memory issues when dealing with a large number of objects and defines the number of OMAP values to fetch in a single request during a list operation.
	DefaultIteratorSize   = 1024    // This is the maximum number of objects to fetch in a single call.
	EventBufferSize       = 100     // The size of the event channel buffer.
)
