// SPDX-FileCopyrightText: 2023 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"errors"
	"fmt"

	"github.com/ceph/go-ceph/rados"
	librbd "github.com/ceph/go-ceph/rbd"
	"github.com/go-logr/logr"
	providerapi "github.com/ironcore-dev/ceph-provider/api"
)

const (
	ImageRBDIDPrefix    = "img_"
	SnapshotRBDIDPrefix = "snap_"

	ImageSnapshotVersion = "v1"

	ImageRBDContentHashKey = "ceph.snapshot.contenthash"

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

func getSnapshotSourceDetails(snapshot *providerapi.Snapshot) (parentName string, snapName string, err error) {
	switch {
	case snapshot.Source.IronCoreImage != "":
		parentName = SnapshotIDToRBDID(snapshot.ID)
		snapName = ImageSnapshotVersion
	case snapshot.Source.VolumeImageID != "":
		parentName = ImageIDToRBDID(snapshot.Source.VolumeImageID)
		snapName = snapshot.ID
	default:
		return "", "", fmt.Errorf("snapshot source is not present")
	}
	return parentName, snapName, nil
}

func closeImage(log logr.Logger, img *librbd.Image) {
	if closeErr := img.Close(); closeErr != nil && !errors.Is(closeErr, librbd.ErrImageNotOpen) {
		log.Error(closeErr, "failed to close image")
	}
}

func openImage(ioCtx *rados.IOContext, imageName string) (*librbd.Image, error) {
	img, err := librbd.OpenImage(ioCtx, imageName, librbd.NoSnapshot)
	if err != nil {
		if !errors.Is(err, librbd.ErrNotFound) {
			return nil, fmt.Errorf("failed to open image %s: %w", imageName, err)
		}
		return nil, err
	}
	return img, nil
}

func flattenImage(log logr.Logger, conn *rados.Conn, pool string, imageName string) error {
	log.V(2).Info("Flatten cloned image", "clonedImageId", imageName)

	ioCtx, err := conn.OpenIOContext(pool)
	if err != nil {
		return fmt.Errorf("unable to open io context for pool %s: %w", pool, err)
	}
	defer ioCtx.Destroy()

	img, err := openImage(ioCtx, imageName)
	if err != nil {
		if errors.Is(err, librbd.ErrNotFound) {
			log.V(2).Info("Cloned image not found, assuming it was trashed", "clonedImageId", imageName)
			return nil
		}
		return err
	}
	defer closeImage(log, img)

	if err := img.Flatten(); err != nil {
		return fmt.Errorf("failed to flatten cloned image %s: %w", imageName, err)
	}
	log.V(2).Info("Flattened cloned image", "clonedImageId", imageName)
	return nil
}

func createSnapshot(log logr.Logger, ioCtx *rados.IOContext, snapshotName string, imageName string) error {
	img, err := openImage(ioCtx, imageName)
	if err != nil {
		return err
	}
	defer closeImage(log, img)

	imgSnap, err := img.CreateSnapshot(snapshotName)
	if err != nil {
		if errors.Is(err, librbd.ErrExist) {
			log.V(2).Info("Snapshot creation failed with 'File exists', assuming it was created concurrently.", "snapshotName", snapshotName)
			imgSnap = img.GetSnapshot(snapshotName)
		} else {
			return fmt.Errorf("unable to create snapshot %s: %w", snapshotName, err)
		}

	}
	log.Info("Snapshot created")

	if err := imgSnap.Protect(); err != nil {
		return fmt.Errorf("unable to protect snapshot %s: %w", snapshotName, err)
	}

	if err := img.SetSnapshot(snapshotName); err != nil {
		return fmt.Errorf("failed to set snapshot %s for image %s: %w", snapshotName, imageName, err)
	}
	return nil
}

func removeSnapshot(snapshot *librbd.Snapshot) error {
	isProtected, err := snapshot.IsProtected()
	if err != nil {
		return fmt.Errorf("unable to check if snapshot is protected: %w", err)
	}

	if isProtected {
		if err := snapshot.Unprotect(); err != nil {
			return fmt.Errorf("unable to unprotect snapshot: %w", err)
		}
	}

	if err := snapshot.Remove(); err != nil {
		return fmt.Errorf("unable to remove snapshot: %w", err)
	}
	return nil
}

func flattenChildImages(log logr.Logger, conn *rados.Conn, img *librbd.Image) error {
	pools, childImgs, err := img.ListChildren()
	if err != nil {
		return fmt.Errorf("unable to list children: %w", err)
	}
	log.V(2).Info("Snapshot references", "pools", len(pools), "rbd-images", len(childImgs))

	for i, snapChildImgName := range childImgs {
		if err := flattenImage(log, conn, pools[i], snapChildImgName); err != nil {
			return err
		}
	}
	return nil
}

func removeTrashedChildImages(log logr.Logger, ioCtx *rados.IOContext, img *librbd.Image) error {
	_, childImgs, err := img.ListChildren()
	if err != nil {
		return fmt.Errorf("unable to list children: %w", err)
	}
	if len(childImgs) <= 0 {
		return nil
	}

	trashList, err := librbd.GetTrashList(ioCtx)
	if err != nil {
		return fmt.Errorf("unable to get trash list: %w", err)
	}
	log.V(2).Info("Removing trashed children of image", "childCount", len(childImgs), "trashCount", len(trashList))

	for _, snapChildImgName := range childImgs {
		for _, trashImg := range trashList {
			if snapChildImgName == trashImg.Name {
				log.V(2).Info("Removing trashed child image", "childImageName", snapChildImgName)
				if err := librbd.TrashRemove(ioCtx, trashImg.Id, true); err != nil {
					return fmt.Errorf("unable to remove trashed child image %s: %w", snapChildImgName, err)
				}
			}
		}
	}
	return nil
}

func snapshotExistsAndProtected(log logr.Logger, ioCtx *rados.IOContext, imageName string, snapshotName string) (bool, bool, error) {
	img, err := openImage(ioCtx, imageName)
	if err != nil {
		return false, false, err
	}
	defer closeImage(log, img)

	snapshot := img.GetSnapshot(snapshotName)
	if isProtected, err := snapshot.IsProtected(); err != nil {
		if !errors.Is(err, librbd.ErrNotFound) {
			return false, false, fmt.Errorf("failed to check if snapshot %s is protected: %w", snapshotName, err)
		}
		return false, false, nil
	} else if !isProtected {
		log.V(2).Info("Snapshot exists but is not protected", "snapshotId", snapshotName)
		return true, false, nil
	}
	log.V(2).Info("Snapshot already exists and is protected", "snapshotId", snapshotName)
	return true, true, nil
}

func protectSnapshot(log logr.Logger, ioCtx *rados.IOContext, imageName string, snapshotName string) error {
	img, err := openImage(ioCtx, imageName)
	if err != nil {
		return err
	}
	defer closeImage(log, img)

	snapshot := img.GetSnapshot(snapshotName)
	if err := snapshot.Protect(); err != nil {
		return fmt.Errorf("unable to protect existing snapshot %s: %w", snapshotName, err)
	}
	if err := img.SetSnapshot(snapshotName); err != nil {
		return fmt.Errorf("failed to set snapshot %s for image %s: %w", snapshotName, imageName, err)
	}
	log.V(2).Info("Successfully protected snapshot", "snapshotId", snapshotName)
	return nil
}
