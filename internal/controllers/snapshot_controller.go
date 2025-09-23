// SPDX-FileCopyrightText: 2023 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/ceph/go-ceph/rados"
	librbd "github.com/ceph/go-ceph/rbd"
	"github.com/go-logr/logr"
	providerapi "github.com/ironcore-dev/ceph-provider/api"
	"github.com/ironcore-dev/ceph-provider/internal/rater"
	"github.com/ironcore-dev/ceph-provider/internal/round"
	"github.com/ironcore-dev/ceph-provider/internal/utils"
	ironcoreimage "github.com/ironcore-dev/ironcore-image"
	"github.com/ironcore-dev/ironcore-image/oci/image"
	"github.com/ironcore-dev/provider-utils/eventutils/event"
	"github.com/ironcore-dev/provider-utils/storeutils/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"
)

type SnapshotReconcilerOptions struct {
	Pool                string
	PopulatorBufferSize int64
	InactivityTimeout   time.Duration
}

func NewSnapshotReconciler(
	log logr.Logger,
	conn *rados.Conn,
	registry image.Source,
	store store.Store[*providerapi.Snapshot],
	events event.Source[*providerapi.Snapshot],
	opts SnapshotReconcilerOptions,
) (*SnapshotReconciler, error) {
	if conn == nil {
		return nil, fmt.Errorf("must specify conn")
	}

	if registry == nil {
		return nil, fmt.Errorf("must specify registry")
	}

	if store == nil {
		return nil, fmt.Errorf("must specify store")
	}

	if events == nil {
		return nil, fmt.Errorf("must specify events")
	}

	if opts.Pool == "" {
		return nil, fmt.Errorf("must specify pool")
	}

	if opts.PopulatorBufferSize == 0 {
		opts.PopulatorBufferSize = 5 * 1024 * 1024
	}

	if opts.InactivityTimeout < 0 {
		return nil, fmt.Errorf("InactivityTimeout must not be negative")
	}

	return &SnapshotReconciler{
		log:                 log,
		conn:                conn,
		registry:            registry,
		queue:               workqueue.NewTypedRateLimitingQueue[string](workqueue.DefaultTypedControllerRateLimiter[string]()),
		store:               store,
		events:              events,
		pool:                opts.Pool,
		populatorBufferSize: opts.PopulatorBufferSize,
		inactivityTimeout:   opts.InactivityTimeout,
	}, nil
}

type SnapshotReconciler struct {
	log  logr.Logger
	conn *rados.Conn

	registry image.Source
	queue    workqueue.TypedRateLimitingInterface[string]

	store  store.Store[*providerapi.Snapshot]
	events event.Source[*providerapi.Snapshot]

	pool                string
	populatorBufferSize int64
	inactivityTimeout   time.Duration
}

func (r *SnapshotReconciler) openSnapshotSource(ctx context.Context, src providerapi.SnapshotSource) (io.ReadCloser, uint64, string, error) {
	switch {
	case src.IronCoreImage != "":

		img, err := r.registry.Resolve(ctx, src.IronCoreImage)
		if err != nil {
			return nil, 0, "", fmt.Errorf("failed to resolve image ref in registry: %w", err)
		}

		ironcoreImage, err := ironcoreimage.ResolveImage(ctx, img)
		if err != nil {
			return nil, 0, "", fmt.Errorf("failed to resolve ironcore image: %w", err)
		}

		rootFS := ironcoreImage.RootFS
		if rootFS == nil {
			return nil, 0, "", fmt.Errorf("image has no root fs")
		}

		content, err := rootFS.Content(ctx)
		if err != nil {
			return nil, 0, "", fmt.Errorf("failed to get root fs content: %w", err)
		}

		return content, uint64(rootFS.Descriptor().Size), img.Descriptor().Digest.String(), nil
	default:
		return nil, 0, "", fmt.Errorf("unrecognized image source %#v", src)
	}
}

func (r *SnapshotReconciler) Start(ctx context.Context) error {
	log := r.log

	//todo make configurable
	workerSize := 15

	reg, err := r.events.AddHandler(event.HandlerFunc[*providerapi.Snapshot](func(event event.Event[*providerapi.Snapshot]) {
		r.queue.Add(event.Object.ID)
	}))
	if err != nil {
		return err
	}
	defer func() {
		_ = r.events.RemoveHandler(reg)
	}()

	go func() {
		<-ctx.Done()
		r.queue.ShutDown()
	}()

	var wg sync.WaitGroup
	for i := 0; i < workerSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r.processNextWorkItem(ctx, log) {
			}
		}()
	}

	wg.Wait()
	return nil
}

func (r *SnapshotReconciler) processNextWorkItem(ctx context.Context, log logr.Logger) bool {
	id, shutdown := r.queue.Get()
	if shutdown {
		return false
	}
	defer r.queue.Done(id)

	reconcileID, err := utils.GenerateUUIDv7()
	if err != nil {
		log.Error(err, "failed to generate reconcile ID")
	}
	log = log.WithValues("snapshotID", id, LogKeyReconcileID, reconcileID)
	ctx = logr.NewContext(ctx, log)

	if err := r.reconcileSnapshot(ctx, log, id); err != nil {
		log.Error(err, "failed to reconcile snapshot")
		r.queue.AddRateLimited(id)
		return true
	}

	r.queue.Forget(id)
	return true
}

const (
	SnapshotFinalizer = "snapshot"
)

func (r *SnapshotReconciler) cleanupSnapshotResources(log logr.Logger, img *librbd.Image) error {
	pools, imgs, err := img.ListChildren()
	if err != nil {
		return fmt.Errorf("unable to list children: %w", err)
	}
	log.V(2).Info("Snapshot references", "pools", len(pools), "rbd-images", len(imgs))

	if len(pools) != 0 || len(imgs) != 0 {
		return fmt.Errorf("unable to delete snapshot: still in use")
	}

	snaps, err := img.GetSnapshotNames()
	if err != nil {
		return fmt.Errorf("unable to list snapshots: %w", err)
	}
	log.V(2).Info("Found snapshots", "count", len(snaps))

	for _, snapInfo := range snaps {
		log.V(2).Info("Start to delete snapshot", "name", snapInfo.Name)

		snap := img.GetSnapshot(snapInfo.Name)
		isProtected, err := snap.IsProtected()
		if err != nil {
			return fmt.Errorf("unable to chek if snapshot is protected: %w", err)
		}

		if isProtected {
			if err := snap.Unprotect(); err != nil {
				return fmt.Errorf("unable to unprotect snapshot: %w", err)
			}
		}

		if err := snap.Remove(); err != nil {
			return fmt.Errorf("unable to remove snapshot snapshot: %w", err)
		}
	}

	return nil
}

func (r *SnapshotReconciler) deleteSnapshot(ctx context.Context, log logr.Logger, ioCtx *rados.IOContext, snapshot *providerapi.Snapshot, rbdImageID string) error {
	if !slices.Contains(snapshot.Finalizers, SnapshotFinalizer) {
		log.V(1).Info("snapshot has no finalizer: done")
		return nil
	}

	img, err := librbd.OpenImage(ioCtx, rbdImageID, librbd.NoSnapshot)
	if err != nil {
		if !errors.Is(err, librbd.ErrNotFound) {
			return fmt.Errorf("failed to fetch snapshot: %w", err)
		}

		snapshot.Finalizers = utils.DeleteSliceElement(snapshot.Finalizers, SnapshotFinalizer)
		if _, err := r.store.Update(ctx, snapshot); store.IgnoreErrNotFound(err) != nil {
			return fmt.Errorf("failed to update snapshot metadata: %w", err)
		}

		log.V(2).Info("Removed snapshot finalizer")
		return nil

	}

	if err := r.cleanupSnapshotResources(log, img); err != nil {
		if closeErr := img.Close(); closeErr != nil {
			return errors.Join(err, fmt.Errorf("unable to close snapshot: %w", closeErr))
		}
		return fmt.Errorf("failed to cleanup snapshot resources: %w", err)
	}

	if err := img.Close(); err != nil {
		return fmt.Errorf("unable to close snapshot: %w", err)
	}

	log.V(2).Info("Remove snapshot", "rbdImageID", rbdImageID)
	if err := img.Remove(); err != nil {
		return fmt.Errorf("unable to remove snapshot: %w", err)
	}
	log.V(2).Info("Snapshot deleted", "rbdImageID", rbdImageID)

	snapshot.Finalizers = utils.DeleteSliceElement(snapshot.Finalizers, SnapshotFinalizer)
	if _, err := r.store.Update(ctx, snapshot); store.IgnoreErrNotFound(err) != nil {
		return fmt.Errorf("failed to update snapshot metadata: %w", err)
	}

	log.V(2).Info("Deleted snapshot")
	return nil
}

func (r *SnapshotReconciler) isSnapshotInUse(ctx context.Context, snapshot *providerapi.Snapshot, ioCtx *rados.IOContext, rbdImageID string) (bool, error) {
	log := logr.FromContextOrDiscard(ctx)
	img, err := librbd.OpenImage(ioCtx, rbdImageID, librbd.NoSnapshot)
	if err != nil {
		if errors.Is(err, librbd.ErrNotFound) {
			log.V(2).Info("RBD image not found, not in use.", "rbdImage", rbdImageID)
			return false, nil
		}
		return false, fmt.Errorf("failed to open RBD image %s to check usage: %w", rbdImageID, err)
	}
	defer func() {
		if closeErr := img.Close(); closeErr != nil {
			log.Error(closeErr, "failed to close RBD image after usage check")
		}
	}()

	_, childrenImgs, err := img.ListChildren()
	if err != nil {
		return false, fmt.Errorf("failed to list children for RBD image %s: %w", rbdImageID, err)
	}

	if len(childrenImgs) > 0 {
		log.V(2).Info("RBD image is in use (has children)", "rbdImageID", rbdImageID, "childrenImageCount", len(childrenImgs))
		return true, nil
	}

	log.V(2).Info("RBD image is not in use (no children)")
	return false, nil
}

// setLastPopulatedTimeIfZero sets the snapshot's LastPopulatedTime if it is currently its zero value (i.e., unset).
// This timestamp marks the point of initial successful population, and is not continuously updated
// during subsequent reconciliations to preserve its meaning as the original ready time.
func (r *SnapshotReconciler) setLastPopulatedTimeIfZero(ctx context.Context, log logr.Logger, snapshot *providerapi.Snapshot, now metav1.Time) error {
	if snapshot.Status.LastPopulatedTime.IsZero() {
		log.V(1).Info("Populated snapshot missing LastPopulatedTime, setting it.")
		snapshot.Status.LastPopulatedTime = now
		if _, err := r.store.Update(ctx, snapshot); err != nil {
			return fmt.Errorf("failed to set LastPopulatedTime: %w", err)
		}
	}
	return nil
}

func (r *SnapshotReconciler) reconcileSnapshot(ctx context.Context, log logr.Logger, id string) error {
	ioCtx, err := r.conn.OpenIOContext(r.pool)
	if err != nil {
		return fmt.Errorf("unable to get io context: %w", err)
	}
	defer ioCtx.Destroy()

	snapshot, err := r.store.Get(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			log.V(2).Info("Snapshot object not found in store, skipping reconciliation")
			return nil
		}
		return fmt.Errorf("failed to fetch snapshot from store: %w", err)
	}
	rbdImageID := SnapshotIDToRBDID(snapshot.ID)

	if snapshot.DeletedAt != nil {
		log.V(1).Info("Snapshot has DeletedAt timestamp, initiating deletion", "deletedAt", snapshot.DeletedAt)
		// Pass the computed rbdImageID to the deleteSnapshot helper
		if err := r.deleteSnapshot(ctx, log, ioCtx, snapshot, rbdImageID); err != nil {
			return fmt.Errorf("failed to delete snapshot: %w", err)
		}
		return nil
	}

	if !slices.Contains(snapshot.Finalizers, SnapshotFinalizer) {
		log.V(1).Info("Adding finalizer to snapshot")
		snapshot.Finalizers = append(snapshot.Finalizers, SnapshotFinalizer)
		if _, err := r.store.Update(ctx, snapshot); err != nil {
			return fmt.Errorf("failed to set finalizers: %w", err)
		}
		return nil
	}

	// Handle Populated Snapshots
	if snapshot.Status.State == providerapi.SnapshotStatePopulated {
		log.V(1).Info("Snapshot is populated")

		now := metav1.Now()
		if err := r.setLastPopulatedTimeIfZero(ctx, log, snapshot, now); err != nil {
			return fmt.Errorf("failed to ensure LastPopulatedTime is set: %w", err)
		}

		// Proceed with inactivity cleanup if timeout is configured.
		if r.inactivityTimeout <= 0 {
			log.V(2).Info("Inactivity timeout is 0 or negative; skipping inactivity check for populated snapshot.")
			return nil
		}

		populatedTime := snapshot.Status.LastPopulatedTime.Time
		expirationTime := populatedTime.Add(r.inactivityTimeout)
		if !expirationTime.Before(now.Time) {
			requeueAfter := time.Until(expirationTime)
			if requeueAfter > 0 {
				log.V(2).Info("Snapshot populated but within inactivity timeout, re-queueing.",
					"remainingTime", requeueAfter)
				r.queue.AddAfter(id, requeueAfter)
			}
			return nil
		}
		log.V(1).Info("Snapshot is past its inactivity timeout, checking if in use.",
			"populatedTime", populatedTime, "inactivityTimeout", r.inactivityTimeout)

		inUse, err := r.isSnapshotInUse(ctx, snapshot, ioCtx, rbdImageID)
		if err != nil {
			return fmt.Errorf("failed to determine if snapshot is in use for inactivity check: %w", err)
		}
		if !inUse {
			log.V(1).Info("Snapshot is unused and past inactivity timeout, marking for deletion.")
			snapshot.DeletedAt = &now.Time
			if _, err := r.store.Update(ctx, snapshot); err != nil {
				return fmt.Errorf("failed to mark unused snapshot for deletion: %w", err)
			}
			return nil
		}

		log.V(2).Info("Snapshot is in use despite inactivity timeout, not marking for deletion.")
		return nil
	}

	// This part handles initial population for non-populated snapshots.
	rc, snapshotSize, digest, err := r.openSnapshotSource(ctx, snapshot.Source)
	if err != nil {
		return fmt.Errorf("failed to open snapshot source: %w", err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Error(err, "failed to close snapshot source")
		}
	}()

	options := librbd.NewRbdImageOptions()
	defer options.Destroy()

	//TODO: different pool for OS images?
	if err := options.SetString(librbd.RbdImageOptionDataPool, r.pool); err != nil {
		return fmt.Errorf("failed to set data pool: %w", err)
	}
	log.V(2).Info("Configured pool", "pool", r.pool)

	snapshot.Status.Size = round.OffBytes(snapshotSize)

	// Use the cached rbdImageID
	rbdImg, err := librbd.OpenImage(ioCtx, rbdImageID, librbd.NoSnapshot)
	if err != nil {
		if !errors.Is(err, librbd.ErrNotFound) {
			return fmt.Errorf("failed to open rbd image %s: %w", rbdImageID, err)
		}
		if err = librbd.CreateImage(ioCtx, rbdImageID, snapshot.Status.Size, options); err != nil {
			return fmt.Errorf("failed to create os rbd image %s: %w", rbdImageID, err)
		}
		log.V(2).Info("Created rbd image", "bytes", snapshot.Status.Size, "rbdImageID", rbdImageID)
		rbdImg, err = librbd.OpenImage(ioCtx, rbdImageID, librbd.NoSnapshot)
	}
	if err != nil {
		return fmt.Errorf("failed to open rbd image %s: %w", rbdImageID, err)
	}

	if err := r.prepareSnapshotContent(log, rbdImg, rc); err != nil {
		if closeErr := rbdImg.Close(); closeErr != nil {
			return errors.Join(err, fmt.Errorf("unable to close snapshot: %w", closeErr))
		}
		return fmt.Errorf("failed to prepare snapshot content: %w", err)
	}

	if err := rbdImg.Close(); err != nil {
		if !errors.Is(err, librbd.ErrImageNotOpen) {
			return fmt.Errorf("unable to close snapshot: %w", err)
		}
	}
	nowAtPopulated := metav1.Now()
	snapshot.Status.Digest = digest
	snapshot.Status.State = providerapi.SnapshotStatePopulated
	snapshot.Status.LastPopulatedTime = nowAtPopulated

	log.V(1).Info("Snapshot populated successfully, recording LastPopulatedTime.", "rbdImageID", rbdImageID)

	if _, err = r.store.Update(ctx, snapshot); err != nil {
		return fmt.Errorf("failed to update snapshot metadata: %w", err)
	}

	return nil
}

func (r *SnapshotReconciler) prepareSnapshotContent(log logr.Logger, rbdImg *librbd.Image, rc io.ReadCloser) error {
	currentSnap := rbdImg.GetSnapshot(ImageSnapshotVersion)

	isProtected, err := currentSnap.IsProtected()
	if err != nil {
		if !errors.Is(err, librbd.ErrNotFound) {
			return fmt.Errorf("failed to check if snapshot %s is protected: %w", ImageSnapshotVersion, err)
		}
	}

	if isProtected {
		log.V(2).Info("Snapshot already exists and is protected, skipping creation and protection.", "snapshotName", ImageSnapshotVersion)
		return nil
	}

	if err := r.populateImage(log, rbdImg, rc); err != nil {
		return fmt.Errorf("failed to populate os image: %w", err)
	}
	log.V(2).Info("Populated os image on rbd image")

	log.V(1).Info("Attempting to create snapshot", "snapshotName", ImageSnapshotVersion)
	newSnap, err := rbdImg.CreateSnapshot(ImageSnapshotVersion)
	if err != nil {
		if errors.Is(err, librbd.ErrExist) {
			log.V(2).Info("Snapshot creation failed with 'File exists', assuming it was created concurrently.", "snapshotName", ImageSnapshotVersion)
			newSnap = rbdImg.GetSnapshot(ImageSnapshotVersion)
		} else {
			return fmt.Errorf("unable to create snapshot %s: %w", ImageSnapshotVersion, err)
		}
	}

	log.V(2).Info("Protecting snapshot.", "snapshotName", ImageSnapshotVersion)
	if err := newSnap.Protect(); err != nil {
		return fmt.Errorf("unable to protect snapshot %s: %w", ImageSnapshotVersion, err)
	}

	return nil
}

func (r *SnapshotReconciler) populateImage(log logr.Logger, dst io.WriteCloser, src io.Reader) error {
	throughputReader := rater.NewRater(src)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				log.Info("Populating", "rate", throughputReader.String())
			case <-done:
				return
			}
		}
	}()
	defer func() { close(done) }()

	buffer := make([]byte, r.populatorBufferSize)
	_, err := io.CopyBuffer(dst, throughputReader, buffer)
	if err != nil {
		return fmt.Errorf("failed to populate image: %w", err)
	}
	log.Info("Successfully populated image")

	return nil
}
