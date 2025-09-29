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
	LogKeyReconcileID   = "reconcileID"
)

type ImageReconcilerOptions struct {
	Monitors string
	Client   string
	Pool     string
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
}

func (r *ImageReconciler) Start(ctx context.Context) error {
	log := r.log

	// todo make configurable
	workerSize := 15

	log.V(2).Info("Check image events")
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

	log.V(2).Info("Check snapshot events")
	snapEventReg, err := r.snapshotEvents.AddHandler(event.HandlerFunc[*providerapi.Snapshot](func(evt event.Event[*providerapi.Snapshot]) {
		log.V(2).Info("Check snapshot event state and type")
		if evt.Type != event.TypeUpdated || evt.Object.Status.State != providerapi.SnapshotStatePopulated {
			return
		}

		imageList, err := r.images.List(ctx)
		if err != nil {
			log.Error(err, "failed to list images")
			return
		}
		log.V(2).Info("List all images", "image count", len(imageList))

		for _, img := range imageList {
			if snapshotRef := img.Spec.SnapshotRef; snapshotRef != nil && *snapshotRef == evt.Object.ID {
				r.Eventf(img.Metadata, corev1.EventTypeNormal, "PulledImage", "Pulled image %s", *img.Spec.SnapshotRef)
				log.V(2).Info("Add image for processing by snapshot event", LogKeyImageID, img.ID)
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
		<-ctx.Done()
		r.queue.ShutDown()
	}()

	var wg sync.WaitGroup
	for i := 0; i < workerSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.V(2).Info("Processing next work item")
			for r.processNextWorkItem(ctx, log) {
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

	image.Finalizers = utils.DeleteSliceElement(image.Finalizers, ImageFinalizer)
	if _, err := r.images.Update(ctx, image); store.IgnoreErrNotFound(err) != nil {
		return fmt.Errorf("failed to update image metadata: %w", err)
	}
	r.Eventf(image.Metadata, corev1.EventTypeNormal, "CompletedDeletion", "Image deletion completed")
	log.V(2).Info("Removed Finalizers")

	return nil
}

type fetchAuthResponse struct {
	Key string `json:"key"`
}

func (r *ImageReconciler) fetchAuth(ctx context.Context, log logr.Logger) (string, string, error) {
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

	log.V(2).Info("Parse image url")
	spec, err := reference.Parse(img.Spec.Image)
	if err != nil {
		return fmt.Errorf("failed to parse image reference: %w", err)
	}

	log.V(2).Info("Resolve image url")
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
			r.Eventf(img.Metadata, corev1.EventTypeNormal, "CreateImageSnapshot", "Image snapshot was not found. Creating new snapshot")
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
				r.Eventf(img.Metadata, corev1.EventTypeWarning, "CreateImageSnapshot", "Create image snapshot failed with error: %s", err)
				return fmt.Errorf("failed to create snapshot: %w", err)
			}
		default:
			return fmt.Errorf("failed to get snapshot: %w", err)
		}
	}

	img.Spec.SnapshotRef = ptr.To(snap.ID)

	log.V(2).Info("Update snapshotRef of image in store")
	if _, err := r.images.Update(ctx, img); err != nil {
		return fmt.Errorf("failed to update image snapshot ref: %w", err)
	}

	r.Eventf(img.Metadata, corev1.EventTypeNormal, "UpdatedImageSnapshotRef", "Updated image snapshot ref: %s", *img.Spec.SnapshotRef)
	return nil
}

func (r *ImageReconciler) isImageExisting(ctx context.Context, log logr.Logger, ioCtx *rados.IOContext, image *providerapi.Image) (bool, error) {
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
	defer func() {
		if err = img.Close(); err != nil {
			log.Error(err, "failed to close image source")
		}
	}()

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
		r.Eventf(image.Metadata, corev1.EventTypeWarning, "UpdateImageSize", "Failed to shrink image: not supported")
		return fmt.Errorf("failed to shrink image: not supported")
	}

	if err := img.Resize(requestedSize); err != nil {
		return fmt.Errorf("failed to resize image: %w", err)
	}

	image.Status.Size = requestedSize
	if _, err = r.images.Update(ctx, image); err != nil {
		return fmt.Errorf("failed to update size information of image: %w", err)
	}
	r.Eventf(image.Metadata, corev1.EventTypeNormal, "UpdatedImageSize", "Image size changed. requestedSize: %d currentSize: %d", requestedSize, currentImageSize)
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
	imageExists, err := r.isImageExisting(ctx, log, ioCtx, img)
	if err != nil {
		return fmt.Errorf("failed to check image existence: %w", err)
	}
	log.V(1).Info("Checked image existence", "imageExists", imageExists)

	if imageExists {
		log.V(2).Info("Check if image state is available")
		if img.Status.State == providerapi.ImageStateAvailable {
			log.V(2).Info("Update image in store")
			if err := r.updateImage(ctx, log, ioCtx, img); err != nil {
				return fmt.Errorf("failed to update image: %w", err)
			}
			return nil
		}
	} else {
		log.V(2).Info("Create new image options")
		options := librbd.NewRbdImageOptions()
		defer options.Destroy()
		if err := options.SetString(librbd.RbdImageOptionDataPool, r.pool); err != nil {
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
			if err := r.createEmptyImage(ctx, log, ioCtx, img, options); err != nil {
				return fmt.Errorf("failed to create empty image: %w", err)
			}
		}
	}

	if err := r.setWWN(ctx, log, ioCtx, img); err != nil {
		return fmt.Errorf("failed to set wwn: %w", err)
	}

	if err := r.setEncryptionHeader(ctx, log, ioCtx, img); err != nil {
		r.Eventf(img.Metadata, corev1.EventTypeWarning, "SetEncryptionFormat", "Set encryption header failed with error: %s", err)
		return fmt.Errorf("failed to set encryption header: %w", err)
	}

	if err := r.setImageLimits(ctx, log, ioCtx, img); err != nil {
		return fmt.Errorf("failed to set limits: %w", err)
	}

	user, key, err := r.fetchAuth(ctx, log)
	if err != nil {
		return fmt.Errorf("failed to fetch credentials: %w", err)
	}

	log.V(1).Info("Update image status in store")
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

func (r *ImageReconciler) setImageLimits(ctx context.Context, log logr.Logger, ioCtx *rados.IOContext, image *providerapi.Image) error {
	if len(image.Spec.Limits) <= 0 {
		return nil
	}

	log.V(1).Info("Configuring limits")
	img, err := librbd.OpenImage(ioCtx, ImageIDToRBDID(image.ID), librbd.NoSnapshot)
	if err != nil {
		return fmt.Errorf("failed to open rbd image: %w", err)
	}

	for limit, value := range image.Spec.Limits {
		if err := img.SetMetadata(fmt.Sprintf("%s%s", LimitMetadataPrefix, limit), strconv.FormatInt(value, 10)); err != nil {
			if closeErr := img.Close(); closeErr != nil {
				return errors.Join(err, fmt.Errorf("unable to close image: %w", closeErr))
			}
			return fmt.Errorf("failed to set limit (%s): %w", limit, err)
		}
		r.Eventf(image.Metadata, corev1.EventTypeNormal, "SetImageLimit", "Image limit set. limit: %s value: %d", limit, value)
		log.V(3).Info("Set image limit", "limit", limit, "value", value)
	}

	if err := img.Close(); err != nil {
		return fmt.Errorf("failed to close rbd image: %w", err)
	}

	return nil
}

func (r *ImageReconciler) setWWN(ctx context.Context, log logr.Logger, ioCtx *rados.IOContext, image *providerapi.Image) error {
	log.V(1).Info("Setting WWN")
	img, err := librbd.OpenImage(ioCtx, ImageIDToRBDID(image.ID), librbd.NoSnapshot)
	if err != nil {
		return fmt.Errorf("failed to open rbd image: %w", err)
	}

	if err := img.SetMetadata(WWNKey, image.Spec.WWN); err != nil {
		if closeErr := img.Close(); closeErr != nil {
			return errors.Join(err, fmt.Errorf("unable to close image: %w", closeErr))
		}
		return fmt.Errorf("failed to set wwn (%s): %w", image.Spec.WWN, err)
	}
	log.V(3).Info("Set image wwn", "wwn", image.Spec.WWN)

	if err := img.Close(); err != nil {
		return fmt.Errorf("failed to close rbd image: %w", err)
	}

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

	if err := img.EncryptionFormat(librbd.EncryptionOptionsLUKS2{
		Alg:        librbd.EncryptionAlgorithmAES256,
		Passphrase: passphrase,
	}); err != nil {
		if closeErr := img.Close(); closeErr != nil {
			return errors.Join(err, fmt.Errorf("unable to close image: %w", closeErr))
		}
		return fmt.Errorf("failed to set encryption format: %w", err)
	}

	if err := img.Close(); err != nil {
		return fmt.Errorf("failed to close rbd image: %w", err)
	}

	image.Status.Encryption = providerapi.EncryptionStateHeaderSet
	if _, err = r.images.Update(ctx, image); err != nil {
		return fmt.Errorf("failed to update image encryption state: %w", err)
	}
	r.Eventf(image.Metadata, corev1.EventTypeNormal, "ConfiguredEncryption", "Configured encryption")

	return nil
}

func (r *ImageReconciler) createEmptyImage(ctx context.Context, log logr.Logger, ioCtx *rados.IOContext, image *providerapi.Image, options *librbd.ImageOptions) error {
	if err := librbd.CreateImage(ioCtx, ImageIDToRBDID(image.ID), round.OffBytes(image.Spec.Size), options); err != nil {
		return fmt.Errorf("failed to create rbd image: %w", err)
	}
	r.Eventf(image.Metadata, corev1.EventTypeNormal, "CreatedImage", "Created image. bytes: %d", image.Spec.Size)
	log.V(2).Info("Created image", "bytes", image.Spec.Size)

	return nil
}

func (r *ImageReconciler) createImageFromSnapshot(ctx context.Context, log logr.Logger, ioCtx *rados.IOContext, image *providerapi.Image, snapshotRef string, options *librbd.ImageOptions) (bool, error) {
	snapshot, err := r.snapshots.Get(ctx, snapshotRef)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return false, fmt.Errorf("failed to get snapshot: %w", err)
		}

		log.V(1).Info("snapshot not found", "snapshotID", snapshot.ID)

		return false, nil
	}

	if snapshot.Status.Size > image.Spec.Size {
		r.Eventf(image.Metadata, corev1.EventTypeWarning, "ClonedImage", "image size has to be bigger as ironcore image %s: %d < %d", snapshot.Source.IronCoreImage, image.Spec.Size, snapshot.Status.Size)
		return false, fmt.Errorf("snapshot ironcore image %s: %w (%d < %d)", snapshot.Source.IronCoreImage, ErrImageSizeIsSmallerAsSnapshotSize, image.Spec.Size, snapshot.Status.Size)
	}

	if snapshot.Status.State != providerapi.SnapshotStatePopulated {
		log.V(1).Info("snapshot is not populated", "state", snapshot.Status.State)
		return false, nil
	}

	ioCtx2, err := r.conn.OpenIOContext(r.pool)
	if err != nil {
		return false, fmt.Errorf("unable to get io context: %w", err)
	}
	defer ioCtx2.Destroy()

	if err = librbd.CloneImage(ioCtx2, SnapshotIDToRBDID(snapshot.ID), ImageSnapshotVersion, ioCtx, ImageIDToRBDID(image.ID), options); err != nil {
		return false, fmt.Errorf("failed to clone rbd image: %w", err)
	}

	img, err := librbd.OpenImage(ioCtx, ImageIDToRBDID(image.ID), librbd.NoSnapshot)
	if err != nil {
		return false, fmt.Errorf("failed to open rbd image: %w", err)
	}

	if err := img.Resize(round.OffBytes(image.Spec.Size)); err != nil {
		if closeErr := img.Close(); closeErr != nil {
			return false, errors.Join(err, fmt.Errorf("unable to close image: %w", closeErr))
		}
		return false, fmt.Errorf("failed to resize rbd image: %w", err)
	}
	log.V(2).Info("Resized cloned image", "bytes", image.Spec.Size)

	if err := img.Close(); err != nil {
		return false, fmt.Errorf("failed to close rbd image: %w", err)
	}

	r.Eventf(image.Metadata, corev1.EventTypeNormal, "ClonedImage", "Cloned image from snapshot. bytes:%d", image.Spec.Size)
	log.V(2).Info("Cloned image")
	return true, nil
}
