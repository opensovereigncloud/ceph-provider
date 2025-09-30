// SPDX-FileCopyrightText: 2023 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package controllers

const (
	ImageRBDIDPrefix    = "img_"
	SnapshotRBDIDPrefix = "ironcore_snap_"

	ImageSnapshotVersion = "v1"

	LogKeyImageID     = "imageID"
	LogKeyReconcileID = "reconcileID"
	LogKeySnapshotID  = "snapshotID"
)

func ImageIDToRBDID(imageID string) string {
	return ImageRBDIDPrefix + imageID
}

func SnapshotIDToRBDID(snapshotID string) string {
	return SnapshotRBDIDPrefix + snapshotID
}
