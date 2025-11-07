// SPDX-FileCopyrightText: 2023 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ceph/go-ceph/rados"
	librbd "github.com/ceph/go-ceph/rbd"
	"github.com/containerd/containerd/reference"
	"github.com/go-logr/logr"

	providerapi "github.com/ironcore-dev/ceph-provider/api"
	"github.com/ironcore-dev/ceph-provider/internal/encryption"
	"github.com/ironcore-dev/ceph-provider/internal/round"
	"github.com/ironcore-dev/ceph-provider/internal/utils"

	"github.com/ironcore-dev/ironcore-image/oci/image"
	apiutils "github.com/ironcore-dev/provider-utils/apiutils/api"
	"github.com/ironcore-dev/provider-utils/eventutils/event"
	eventrecorder "github.com/ironcore-dev/provider-utils/eventutils/recorder"
	"github.com/ironcore-dev/provider-utils/storeutils/store"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"
)

const (
	LimitMetadataPrefix = "conf_"
	WWNKey              = "wwn"
	imageDigestLabel    = "image-digest"
)

type ImageReconcilerOptions struct {
	Monitors   string
	Client     string
	Pool       string
	WorkerSize int
}

var (
	ErrImageSizeIsSmallerAsSnapshotSize = errors.New("image size is lower as snapshot size")
)

func NewImageReconciler(
	log logr.Logger,
	conn *rados.Conn,
	registry image.Source,
	images store.Store[*providerapi.Image],
	snapshots store.Store[*providerapi.Snapshot],
	eventRecorder eventrecorder.EventRecorder,
	imageEvents event.Source[*providerapi.Image],
	snapshotEvents event.Source[*providerapi.Snapshot],
	keyEncryption encryption.Encryptor,
	opts ImageReconcilerOptions,
) (*ImageReconciler, error) {
	if conn == nil {
		return nil, fmt.Errorf("must specify conn")
	}

	if registry == nil {
		return nil, fmt.Errorf("must specify registry")
	}

	if images == nil {
		return nil, fmt.Errorf("must specify image store")
	}

	if snapshots == nil {
		return nil, fmt.Errorf("must specify snapshots store")
	}

	if imageEvents == nil {
		return nil, fmt.Errorf("must specify image events")
	}

	if snapshotEvents == nil {
		return nil, fmt.Errorf("must specify snapshot events")
	}

	if keyEncryption == nil {
		return nil, fmt.Errorf("must specify key encryption")
	}

	if opts.Pool == "" {
		return nil, fmt.Errorf("must specify pool")
	}

	if opts.Monitors == "" {
		return nil, fmt.Errorf("must specify monitors")
	}

	if opts.Client == "" {
		return nil, fmt.Errorf("must specify ceph client")
	}

	return &ImageReconciler{
		log:            log,
		conn:           conn,
		registry:       registry,
		queue:          workqueue.NewTypedRateLimitingQueue[string](workqueue.DefaultTypedControllerRateLimiter[string]()),
		images:         images,
		snapshots:      snapshots,
		EventRecorder:  eventRecorder,
		imageEvents:    imageEvents,
		snapshotEvents: snapshotEvents,
		monitors:       opts.Monitors,
		client:         opts.Client,
		pool:           opts.Pool,
		keyEncryption:  keyEncryption,
		workerSize:     opts.WorkerSize,
	}, nil
}

type ImageReconciler struct {
	log  logr.Logger
	conn *rados.Conn

	registry image.Source
	queue    workqueue.TypedRateLimitingInterface[string]

	images    store.Store[*providerapi.Image]
	snapshots store.Store[*providerapi.Snapshot]

	eventrecorder.EventRecorder
	imageEvents    event.Source[*providerapi.Image]
	snapshotEvents event.Source[*providerapi.Snapshot]

	monitors string
	client   string
	pool     string

	keyEncryption encryption.Encryptor

	workerSize int
}

func (r *ImageReconciler) Start(ctx context.Context) error {
	log := r.log

	log.V(2).Info("Register image events handler")
	imgEventReg, err := r.imageEvents.AddHandler(event.HandlerFunc[*providerapi.Image](func(evt event.Event[*providerapi.Image]) {
		log.V(2).Info("Add image for processing by image event", LogKeyImageID, evt.Object.GetID())
		r.queue.Add(evt.Object.ID)
	}))
	if err != nil {
		return err
	}
	defer func() {
		_ = r.imageEvents.RemoveHandler(imgEventReg)
	}()

	log.V(2).Info("Register snapshot events handler")
	snapEventReg, err := r.snapshotEvents.AddHandler(event.HandlerFunc[*providerapi.Snapshot](func(evt event.Event[*providerapi.Snapshot]) {
		localLog := log.WithValues(LogKeySnapshotID, evt.Object.ID)
		localLog.V(2).Info("Check snapshot event state and type")
		if evt.Type != event.TypeUpdated || evt.Object.Status.State != providerapi.SnapshotStateReady {
			return
		}

		imageList, err := r.images.List(ctx)
		if err != nil {
			localLog.Error(err, "failed to list images")
			return
		}
		localLog.V(2).Info("List all images", "imageCount", len(imageList))

		for _, img := range imageList {
			if snapshotRef := img.Spec.SnapshotRef; snapshotRef != nil && *snapshotRef == evt.Object.ID {
				r.Eventf(img.Metadata, corev1.EventTypeNormal, "ImagePullSucceeded", "Pulled image %s", *img.Spec.SnapshotRef)
				localLog.V(2).Info("Add image for processing by snapshot event", LogKeyImageID, img.ID)
				r.queue.Add(img.ID)
			}
		}
	}))
	if err != nil {
		return err
	}
	defer func() {
		_ = r.snapshotEvents.RemoveHandler(snapEventReg)
	}()

	go func() {
		// Panic recovery for the image reconciler shutdown handler
		defer utils.Recover(log, "image-reconciler-shutdown-handler")
		<-ctx.Done()
		r.queue.ShutDown()
	}()

	var wg sync.WaitGroup
	for i := 0; i < r.workerSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.V(2).Info("starting worker")
			for r.processNextWorkItem(ctx, log) {
				// Inner function to wrap the unit of work and its panic recovery
				shouldContinue := func() bool {
					// Panic recovery for the snapshot reconciler worker
					defer utils.Recover(log, "image-reconciler-worker")
					// Process the next work item. Returns false if the queue is shutting down.
					return r.processNextWorkItem(ctx, log)
				}()
				// If r.processNextWorkItem signaled shutdown, exit the worker goroutine.
				if !shouldContinue {
					return
				}
			}
		}()
	}

	wg.Wait()
	return nil
}

func (r *ImageReconciler) processNextWorkItem(ctx context.Context, log logr.Logger) bool {
	id, shutdown := r.queue.Get()
	if shutdown {
		log.V(2).Info("Can't process image. Worker is shutdown", LogKeyImageID, id)
		return false
	}
	defer r.queue.Done(id)

	log = log.WithValues(LogKeyImageID, id)

	log.V(2).Info("Process id from queue")
	reconcileID, err := utils.GenerateUUIDv7()
	if err != nil {
		log.Error(err, "failed to generate reconcile ID")
	}

	log = log.WithValues(LogKeyReconcileID, reconcileID)
	ctx = logr.NewContext(ctx, log)

	if err := r.reconcileImage(ctx, log, id); err != nil {
		log.Error(err, "failed to reconcile image")
		r.queue.AddRateLimited(id)
		return true
	}

	log.V(1).Info("Remove id from queue")
	r.queue.Forget(id)
	return true
}

const (
	ImageFinalizer = "image"
)

func (r *ImageReconciler) deleteImage(ctx context.Context, log logr.Logger, ioCtx *rados.IOContext, image *providerapi.Image) error {
	if !slices.Contains(image.Finalizers, ImageFinalizer) {
		log.V(1).Info("image has no finalizer: done")
		return nil
	}

	log.V(2).Info("Open image")
	imgExists := true
	img, err := librbd.OpenImage(ioCtx, ImageIDToRBDID(image.ID), librbd.NoSnapshot)
	if err != nil {
		if !errors.Is(err, librbd.ErrNotFound) {
			return fmt.Errorf("failed to open image: %w", err)
		} else {
			imgExists = false
		}
	}

	log.V(2).Info("Check if image exists", "imgExists", imgExists)
	if img != nil {
		defer img.Close()
	}

	if imgExists {
		log.V(2).Info("Marshal image")
		data, err := json.Marshal(image)
		if err != nil {
			return fmt.Errorf("failed to marshal image obj: %w", err)
		}

		log.V(2).Info("Set image metadata")
		err = img.SetMetadata("onmetal-omap-backup", string(data))
		if err != nil {
			return err
		}

		// TODO make trash timeout configurable
		if err := img.Trash(7 * 24 * time.Hour); err != nil && !errors.Is(err, librbd.ErrNotFound) {
			return fmt.Errorf("failed to remove rbd image: %w", err)
		}

		log.V(2).Info("Rbd image marked for deletion")
	} else {
		log.V(2).Info("Rbd image not found, it was probably already deleted")
	}

	snapshotID := image.Spec.SnapshotRef
	if image.Spec.Image != "" && snapshotID != nil && *snapshotID != "" {
		log.V(2).Info("Deleting volume os-image")
		if err := r.snapshots.Delete(ctx, *snapshotID); err != nil {
			if !errors.Is(err, utils.ErrSnapshotNotFound) {
				return fmt.Errorf("error deleting os-image: %w", err)
			}
			return nil
		}
	}

	image.Finalizers = utils.DeleteSliceElement(image.Finalizers, ImageFinalizer)
	if _, err := r.images.Update(ctx, image); store.IgnoreErrNotFound(err) != nil {
		return fmt.Errorf("failed to update image metadata: %w", err)
	}
	r.Eventf(image.Metadata, corev1.EventTypeNormal, "ImageDeletionSucceeded", "Deleted image")
	log.V(2).Info("Removed Finalizers")

	return nil
}

type fetchAuthResponse struct {
	Key string `json:"key"`
}

func (r *ImageReconciler) fetchAuth(log logr.Logger) (string, string, error) {
	cmd1, err := json.Marshal(map[string]string{
		"prefix": "auth get-key",
		"entity": r.client,
		"format": "json",
	})
	if err != nil {
		return "", "", fmt.Errorf("unable to marshal command: %w", err)
	}

	log.V(3).Info("Try to fetch client", "name", r.client)
	data, _, err := r.conn.MonCommand(cmd1)
	if err != nil {
		return "", "", fmt.Errorf("failed to execute mon command: %w", err)
	}

	response := fetchAuthResponse{}
	if err := json.Unmarshal(data, &response); err != nil {
		return "", "", fmt.Errorf("unable to unmarshal response: %w", err)
	}

	return strings.TrimPrefix(r.client, "client."), response.Key, nil
}

func (r *ImageReconciler) reconcileSnapshot(ctx context.Context, log logr.Logger, img *providerapi.Image) error {
	if img.Spec.Image == "" || img.Spec.SnapshotRef != nil {
		return nil
	}

	log.V(2).Info("Parse image reference", "Image", img.Spec.Image)
	spec, err := reference.Parse(img.Spec.Image)
	if err != nil {
		return fmt.Errorf("failed to parse image reference: %w", err)
	}

	log.V(2).Info("Resolve image reference")
	resolvedImg, err := r.registry.Resolve(ctx, img.Spec.Image)
	if err != nil {
		return fmt.Errorf("failed to resolve image ref in registry: %w", err)
	}

	snapshotDigest := resolvedImg.Descriptor().Digest.String()
	resolvedImageName := fmt.Sprintf("%s@%s", spec.Locator, snapshotDigest)

	// TODO select later by label
	snap, err := r.snapshots.Get(ctx, snapshotDigest)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			log.V(2).Info("Create image snapshot", "SnapshotID", snapshotDigest)
			r.Eventf(img.Metadata, corev1.EventTypeNormal, "CreateImageSnapshotInitiated", "Image snapshot was not found. Creating new snapshot")
			snap, err = r.snapshots.Create(ctx, &providerapi.Snapshot{
				Metadata: apiutils.Metadata{
					ID: snapshotDigest,
					Labels: map[string]string{
						imageDigestLabel: snapshotDigest,
					},
				},
				Source: providerapi.SnapshotSource{
					IronCoreImage: resolvedImageName,
				},
			})
			if err != nil {
				r.Eventf(img.Metadata, corev1.EventTypeWarning, "CreateImageSnapshotFailed", "Failed to create image snapshot: %s", err)
				return fmt.Errorf("failed to create snapshot: %w", err)
			}
			r.Eventf(img.Metadata, corev1.EventTypeNormal, "CreateImageSnapshotSucceeded", "Created image snapshot")
		default:
			return fmt.Errorf("failed to get snapshot: %w", err)
		}
	}

	img.Spec.SnapshotRef = ptr.To(snap.ID)

	log.V(2).Info("Update snapshot reference in image store")
	if _, err := r.images.Update(ctx, img); err != nil {
		return fmt.Errorf("failed to update image snapshot ref: %w", err)
	}

	r.Eventf(img.Metadata, corev1.EventTypeNormal, "UpdateImageSnapshotRefSucceeded", "Updated image snapshot ref: %s", *img.Spec.SnapshotRef)
	return nil
}

func (r *ImageReconciler) isImageExisting(ioCtx *rados.IOContext, image *providerapi.Image) (bool, error) {
	images, err := librbd.GetImageNames(ioCtx)
	if err != nil {
		return false, fmt.Errorf("failed to list images: %w", err)
	}

	for _, img := range images {
		if ImageIDToRBDID(image.ID) == img {
			return true, nil
		}
	}

	return false, nil
}

func (r *ImageReconciler) updateImage(ctx context.Context, log logr.Logger, ioCtx *rados.IOContext, image *providerapi.Image) (err error) {
	log.V(2).Info("Updating image")
	img, err := librbd.OpenImage(ioCtx, ImageIDToRBDID(image.ID), librbd.NoSnapshot)
	if err != nil {
		return fmt.Errorf("failed to open image: %w", err)
	}
	defer closeImage(log, img)

	currentImageSize, err := img.GetSize()
	if err != nil {
		return fmt.Errorf("failed to get image size: %w", err)
	}

	requestedSize := round.OffBytes(image.Spec.Size)

	switch {
	case currentImageSize == requestedSize:
		log.V(2).Info("No update needed: Old and new image size same")
		return nil
	case requestedSize < currentImageSize:
		r.Eventf(image.Metadata, corev1.EventTypeWarning, "UpdateImageSizeFailed", "Image shrink not supported")
		return fmt.Errorf("failed to shrink image: not supported")
	}

	if err := img.Resize(requestedSize); err != nil {
		r.Eventf(image.Metadata, corev1.EventTypeWarning, "UpdateImageSizeFailed", "Failed to resize image: %s", err)
		return fmt.Errorf("failed to resize image: %w", err)
	}

	image.Status.Size = requestedSize
	if _, err = r.images.Update(ctx, image); err != nil {
		return fmt.Errorf("failed to update size information of image: %w", err)
	}
	r.Eventf(image.Metadata, corev1.EventTypeNormal, "UpdateImageSizeSucceeded", "Updated image size. requestedSize: %d currentSize: %d", requestedSize, currentImageSize)
	log.V(1).Info("Updated image", "requestedSize", requestedSize, "currentSize", currentImageSize)
	return nil
}

func (r *ImageReconciler) reconcileImage(ctx context.Context, log logr.Logger, id string) error {
	log.V(2).Info("Reconciling image")
	ioCtx, err := r.conn.OpenIOContext(r.pool)
	if err != nil {
		return fmt.Errorf("unable to get io context: %w", err)
	}
	defer ioCtx.Destroy()

	log.V(2).Info("get image from store if exists")
	img, err := r.images.Get(ctx, id)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("failed to fetch image from store: %w", err)
		}
		return nil
	}

	log.V(2).Info("Check if image is marked for deletion")
	if img.DeletedAt != nil {
		log.V(2).Info("Delete image as its marked for deletion")
		if err := r.deleteImage(ctx, log, ioCtx, img); err != nil {
			return fmt.Errorf("failed to delete image: %w", err)
		}
		log.V(1).Info("Successfully deleted image")
		return nil
	}

	log.V(2).Info("Check if image has finalizer")
	if !slices.Contains(img.Finalizers, ImageFinalizer) {
		log.V(2).Info("Image dont have finalizer, add finalizer to image")
		img.Finalizers = append(img.Finalizers, ImageFinalizer)
		if _, err := r.images.Update(ctx, img); err != nil {
			return fmt.Errorf("failed to set finalizers: %w", err)
		}
		return nil
	}

	log.V(2).Info("Reconcile snapshot of image")
	if err := r.reconcileSnapshot(ctx, log, img); err != nil {
		return fmt.Errorf("failed to reconcile snapshot: %w", err)
	}

	log.V(2).Info("Check if image already exist")
	imageExists, err := r.isImageExisting(ioCtx, img)
	if err != nil {
		return fmt.Errorf("failed to check image existence: %w", err)
	}
	log.V(1).Info("Checked image existence", "imageExists", imageExists)

	if imageExists {
		log.V(2).Info("Check if image state is available")
		if img.Status.State == providerapi.ImageStateAvailable {
			if err := r.updateImage(ctx, log, ioCtx, img); err != nil {
				return fmt.Errorf("failed to update image: %w", err)
			}
			return nil
		}
	} else {
		log.V(2).Info("Create new image options")
		options := librbd.NewRbdImageOptions()
		defer options.Destroy()
		if err := options.SetString(librbd.ImageOptionDataPool, r.pool); err != nil {
			return fmt.Errorf("failed to set data pool: %w", err)
		}
		log.V(2).Info("Configured pool", "pool", r.pool)

		switch {
		case img.Spec.SnapshotRef != nil:
			snapshotRef := img.Spec.SnapshotRef
			log.V(2).Info("Creating image from snapshot", "snapshotRef", snapshotRef)
			ok, err := r.createImageFromSnapshot(ctx, log, ioCtx, img, *snapshotRef, options)
			if err != nil {
				return fmt.Errorf("failed to create image from snapshot: %w", err)
			}
			if !ok {
				return nil
			}

		default:
			log.V(2).Info("Creating empty image")
			if err := r.createEmptyImage(log, ioCtx, img, options); err != nil {
				return fmt.Errorf("failed to create empty image: %w", err)
			}
		}
	}

	if err := r.setWWN(log, ioCtx, img); err != nil {
		return fmt.Errorf("failed to set wwn: %w", err)
	}

	if err := r.setEncryptionHeader(ctx, log, ioCtx, img); err != nil {
		r.Eventf(img.Metadata, corev1.EventTypeWarning, "ConfigureEncryptionFailed", "Failed to configure encryption header: %s", err)
		return fmt.Errorf("failed to set encryption header: %w", err)
	}

	if err := r.setImageLimits(log, ioCtx, img); err != nil {
		return fmt.Errorf("failed to set limits: %w", err)
	}

	user, key, err := r.fetchAuth(log)
	if err != nil {
		return fmt.Errorf("failed to fetch credentials: %w", err)
	}

	img.Status.Access = &providerapi.ImageAccess{
		Monitors: r.monitors,
		Handle:   fmt.Sprintf("%s/%s", r.pool, ImageIDToRBDID(img.ID)),
		User:     user,
		UserKey:  key,
	}
	img.Status.State = providerapi.ImageStateAvailable
	img.Status.Size = round.OffBytes(img.Spec.Size)
	if _, err = r.images.Update(ctx, img); err != nil {
		return fmt.Errorf("failed to update image metadate: %w", err)
	}

	log.V(1).Info("Successfully reconciled image")

	return nil
}

func (r *ImageReconciler) setImageLimits(log logr.Logger, ioCtx *rados.IOContext, image *providerapi.Image) error {
	if len(image.Spec.Limits) <= 0 {
		return nil
	}

	log.V(1).Info("Configuring limits")
	img, err := librbd.OpenImage(ioCtx, ImageIDToRBDID(image.ID), librbd.NoSnapshot)
	if err != nil {
		return fmt.Errorf("failed to open rbd image: %w", err)
	}
	defer closeImage(log, img)

	for limit, value := range image.Spec.Limits {
		if err := img.SetMetadata(fmt.Sprintf("%s%s", LimitMetadataPrefix, limit), strconv.FormatInt(value, 10)); err != nil {
			r.Eventf(image.Metadata, corev1.EventTypeWarning, "SetImageLimitFailed", "Failed to set image limit: %s", err)
			return fmt.Errorf("failed to set limit (%s): %w", limit, err)
		}
		r.Eventf(image.Metadata, corev1.EventTypeNormal, "SetImageLimitSucceeded", "Image limit set. limit: %s value: %d", limit, value)
		log.V(3).Info("Set image limit", "limit", limit, "value", value)
	}

	return nil
}

func (r *ImageReconciler) setWWN(log logr.Logger, ioCtx *rados.IOContext, image *providerapi.Image) error {
	log.V(1).Info("Setting WWN")
	img, err := librbd.OpenImage(ioCtx, ImageIDToRBDID(image.ID), librbd.NoSnapshot)
	if err != nil {
		return fmt.Errorf("failed to open rbd image: %w", err)
	}
	defer closeImage(log, img)

	if err := img.SetMetadata(WWNKey, image.Spec.WWN); err != nil {
		return fmt.Errorf("failed to set wwn (%s): %w", image.Spec.WWN, err)
	}
	log.V(3).Info("Set image wwn", "wwn", image.Spec.WWN)

	return nil
}

func (r *ImageReconciler) setEncryptionHeader(ctx context.Context, log logr.Logger, ioCtx *rados.IOContext, image *providerapi.Image) error {
	if image.Spec.Encryption.Type == "" || image.Spec.Encryption.Type == providerapi.EncryptionTypeUnencrypted || image.Status.Encryption == providerapi.EncryptionStateHeaderSet {
		return nil
	}

	log.V(1).Info("Configuring encryption")
	passphrase, err := r.keyEncryption.Decrypt(image.Spec.Encryption.EncryptedPassphrase)
	if err != nil {
		return fmt.Errorf("failed to decrypt passphrase: %w", err)
	}

	img, err := librbd.OpenImage(ioCtx, ImageIDToRBDID(image.ID), librbd.NoSnapshot)
	if err != nil {
		return fmt.Errorf("failed to open rbd image: %w", err)
	}
	defer closeImage(log, img)

	if err := img.EncryptionFormat(librbd.EncryptionOptionsLUKS2{
		Alg:        librbd.EncryptionAlgorithmAES256,
		Passphrase: passphrase,
	}); err != nil {
		return fmt.Errorf("failed to set encryption format: %w", err)
	}

	image.Status.Encryption = providerapi.EncryptionStateHeaderSet
	if _, err = r.images.Update(ctx, image); err != nil {
		return fmt.Errorf("failed to update image encryption state: %w", err)
	}
	r.Eventf(image.Metadata, corev1.EventTypeNormal, "ConfigureEncryptionSucceeded", "Encryption header configured")

	return nil
}

func (r *ImageReconciler) createEmptyImage(log logr.Logger, ioCtx *rados.IOContext, image *providerapi.Image, options *librbd.ImageOptions) error {
	if err := librbd.CreateImage(ioCtx, ImageIDToRBDID(image.ID), round.OffBytes(image.Spec.Size), options); err != nil {
		r.Eventf(image.Metadata, corev1.EventTypeWarning, "EmptyImageCreationFailed", "Empty image creation failed: %s", err)
		return fmt.Errorf("failed to create rbd image: %w", err)
	}
	r.Eventf(image.Metadata, corev1.EventTypeNormal, "EmptyImageCreationSucceeded", "Created empty image. bytes: %d", image.Spec.Size)
	log.V(2).Info("Created empty image", "bytes", image.Spec.Size)

	return nil
}

func (r *ImageReconciler) createImageFromSnapshot(ctx context.Context, log logr.Logger, ioCtx *rados.IOContext, image *providerapi.Image, snapshotRef string, options *librbd.ImageOptions) (bool, error) {
	snapshot, err := r.snapshots.Get(ctx, snapshotRef)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return false, fmt.Errorf("failed to get snapshot: %w", err)
		}

		log.V(1).Info("snapshot not found", "snapshotID", snapshotRef)
		return false, nil
	}

	if snapshot.Status.Size > image.Spec.Size {
		r.Eventf(image.Metadata, corev1.EventTypeWarning, "ClonedImage", "image size has to be bigger as ironcore image %s: %d < %d", snapshot.Source.IronCoreImage, image.Spec.Size, snapshot.Status.Size)
		return false, fmt.Errorf("snapshot ironcore image %s: %w (%d < %d)", snapshot.Source.IronCoreImage, ErrImageSizeIsSmallerAsSnapshotSize, image.Spec.Size, snapshot.Status.Size)
	}

	if snapshot.Status.State != providerapi.SnapshotStateReady {
		log.V(1).Info("snapshot is not populated", "state", snapshot.Status.State)
		return false, nil
	}

	parentName, snapName, err := getSnapshotSourceDetails(snapshot)
	if err != nil {
		return false, fmt.Errorf("failed to get snapshot source details: %w", err)
	}

	ioCtx2, err := r.conn.OpenIOContext(r.pool)
	if err != nil {
		return false, fmt.Errorf("unable to get io context: %w", err)
	}
	defer ioCtx2.Destroy()

	if err = librbd.CloneImage(ioCtx2, parentName, snapName, ioCtx, ImageIDToRBDID(image.ID), options); err != nil {
		r.Eventf(image.Metadata, corev1.EventTypeWarning, "CreateImageFromSnapshotFailed", "Failed to clone rbd image: %s", err)
		return false, fmt.Errorf("failed to clone rbd image: %w", err)
	}
	log.V(2).Info("Cloned image")

	img, err := librbd.OpenImage(ioCtx, ImageIDToRBDID(image.ID), librbd.NoSnapshot)
	if err != nil {
		return false, fmt.Errorf("failed to open rbd image: %w", err)
	}
	defer closeImage(log, img)

	if err := img.Resize(round.OffBytes(image.Spec.Size)); err != nil {
		return false, fmt.Errorf("failed to resize rbd image: %w", err)
	}
	log.V(2).Info("Resized cloned image", "bytes", image.Spec.Size)

	r.Eventf(image.Metadata, corev1.EventTypeNormal, "ClonedImage", "Cloned image from snapshot. bytes:%d", image.Spec.Size)
	return true, nil
}
