// Copyright 2023 OnMetal authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/ceph/go-ceph/rados"
	librbd "github.com/ceph/go-ceph/rbd"
	"github.com/containerd/containerd/reference"
	"github.com/go-logr/logr"
	"github.com/onmetal/cephlet/pkg/api"
	"github.com/onmetal/cephlet/pkg/event"
	"github.com/onmetal/cephlet/pkg/round"
	"github.com/onmetal/cephlet/pkg/store"
	"github.com/onmetal/cephlet/pkg/utils"
	"github.com/onmetal/onmetal-api/broker/common/idgen"
	"github.com/onmetal/onmetal-image/oci/image"
	"golang.org/x/exp/slices"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/pointer"
)

const (
	LimitMetadataPrefix = "conf_"
	imageDigestLabel    = "image-digest"
)

type ImageReconcilerOptions struct {
	Monitors string
	Client   string
	Pool     string
}

func NewImageReconciler(
	log logr.Logger,
	conn *rados.Conn,
	registry image.Source,
	images store.Store[*api.Image],
	snapshots store.Store[*api.Snapshot],
	imageEvents event.Source[*api.Image],
	snapshotEvents event.Source[*api.Snapshot],
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
		wwnGen:         idgen.NewIDGen(rand.Reader, 16),
		registry:       registry,
		queue:          workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		images:         images,
		snapshots:      snapshots,
		imageEvents:    imageEvents,
		snapshotEvents: snapshotEvents,
		monitors:       opts.Monitors,
		client:         opts.Client,
		pool:           opts.Pool,
	}, nil
}

type ImageReconciler struct {
	log  logr.Logger
	conn *rados.Conn

	wwnGen idgen.IDGen

	registry image.Source
	queue    workqueue.RateLimitingInterface

	images    store.Store[*api.Image]
	snapshots store.Store[*api.Snapshot]

	imageEvents    event.Source[*api.Image]
	snapshotEvents event.Source[*api.Snapshot]

	monitors string
	client   string
	pool     string
}

func (r *ImageReconciler) Start(ctx context.Context) error {
	log := r.log

	//todo make configurable
	workerSize := 15

	imgEventReg, err := r.imageEvents.AddHandler(event.HandlerFunc[*api.Image](func(evt event.Event[*api.Image]) {
		if evt.Type == event.TypeUpdated {
			return
		}
		r.queue.Add(evt.Object.ID)
	}))
	if err != nil {
		return err
	}
	defer func() {
		_ = r.imageEvents.RemoveHandler(imgEventReg)
	}()

	snapEventReg, err := r.snapshotEvents.AddHandler(event.HandlerFunc[*api.Snapshot](func(evt event.Event[*api.Snapshot]) {
		if evt.Type != event.TypeUpdated || evt.Object.Status.State != api.SnapshotStatePopulated {
			return
		}

		imageList, err := r.images.List(ctx)
		if err != nil {
			log.Error(err, "failed to list images")
			return
		}

		for _, img := range imageList {
			if snapshotRef := img.Spec.SnapshotRef; snapshotRef != nil && *snapshotRef == evt.Object.ID {
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
			for r.processNextWorkItem(ctx, log) {
			}
		}()
	}

	wg.Wait()
	return nil
}

func (r *ImageReconciler) processNextWorkItem(ctx context.Context, log logr.Logger) bool {
	item, shutdown := r.queue.Get()
	if shutdown {
		return false
	}
	defer r.queue.Done(item)

	id := item.(string)
	log = log.WithValues("imageId", id)
	ctx = logr.NewContext(ctx, log)

	if err := r.reconcileImage(ctx, id); err != nil {
		log.Error(err, "failed to reconcile image")
		r.queue.AddRateLimited(item)
		return true
	}

	r.queue.Forget(item)
	return true
}

const (
	ImageFinalizer = "image"
)

func (r *ImageReconciler) deleteImage(ctx context.Context, log logr.Logger, ioCtx *rados.IOContext, image *api.Image) error {
	if !slices.Contains(image.Finalizers, ImageFinalizer) {
		log.V(1).Info("image has no finalizer: done")
		return nil
	}

	if err := librbd.RemoveImage(ioCtx, ImageIDToRBDID(image.ID)); err != nil && !errors.Is(err, librbd.ErrNotFound) {
		return fmt.Errorf("failed to remove rbd image: %w", err)
	}
	log.V(2).Info("Rbd image deleted")

	image.Finalizers = utils.DeleteSliceElement(image.Finalizers, ImageFinalizer)
	if _, err := r.images.Update(ctx, image); store.IgnoreErrNotFound(err) != nil {
		return fmt.Errorf("failed to update image metadata: %w", err)
	}
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

func (r *ImageReconciler) reconcileSnapshot(ctx context.Context, log logr.Logger, img *api.Image) error {
	if !(img.Spec.Image != "" && img.Spec.SnapshotRef == nil) {
		return nil
	}

	spec, err := reference.Parse(img.Spec.Image)
	if err != nil {
		return fmt.Errorf("failed to parse image reference: %w", err)
	}

	resolvedImg, err := r.registry.Resolve(ctx, img.Spec.Image)
	if err != nil {
		return fmt.Errorf("failed to resolve image ref in registry: %w", err)
	}

	snapshotDigest := resolvedImg.Descriptor().Digest.String()
	resolvedImageName := fmt.Sprintf("%s@%s", spec.Locator, snapshotDigest)

	//TODO select later by label
	snap, err := r.snapshots.Get(ctx, snapshotDigest)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			snap, err = r.snapshots.Create(ctx, &api.Snapshot{
				Metadata: api.Metadata{
					ID: snapshotDigest,
					Labels: map[string]string{
						imageDigestLabel: snapshotDigest,
					},
				},
				Source: api.SnapshotSource{
					OnmetalImage: resolvedImageName,
				},
			})
			if err != nil {
				return fmt.Errorf("failed to create snapshot: %w", err)
			}
		default:
			return fmt.Errorf("failed to get snapshot: %w", err)
		}
	}

	img.Spec.SnapshotRef = pointer.String(snap.ID)
	if _, err := r.images.Update(ctx, img); err != nil {
		return fmt.Errorf("failed to update image snapshot ref: %w", err)
	}

	return nil
}

func (r *ImageReconciler) reconcileImage(ctx context.Context, id string) error {
	log := logr.FromContextOrDiscard(ctx)
	ioCtx, err := r.conn.OpenIOContext(r.pool)
	if err != nil {
		return fmt.Errorf("unable to get io context: %w", err)
	}
	defer ioCtx.Destroy()

	img, err := r.images.Get(ctx, id)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("failed to fetch image from store: %w", err)
		}

		return nil
	}

	if img.DeletedAt != nil {
		if err := r.deleteImage(ctx, log, ioCtx, img); err != nil {
			return fmt.Errorf("failed to delete image: %w", err)
		}
		log.V(1).Info("Successfully deleted image")
		return nil
	}

	if img.Status.State == api.ImageStateAvailable {
		log.V(1).Info("Image already provisioned")
		return nil
	}

	if !slices.Contains(img.Finalizers, ImageFinalizer) {
		img.Finalizers = append(img.Finalizers, ImageFinalizer)
		if _, err := r.images.Update(ctx, img); err != nil {
			return fmt.Errorf("failed to set finalizers: %w", err)
		}
	}

	if err := r.reconcileSnapshot(ctx, log, img); err != nil {
		return fmt.Errorf("failed to reconcile snapshot: %w", err)
	}

	options := librbd.NewRbdImageOptions()
	defer options.Destroy()
	if err := options.SetString(librbd.RbdImageOptionDataPool, r.pool); err != nil {
		return fmt.Errorf("failed to set data pool: %w", err)
	}
	log.V(2).Info("Configured pool", "pool", r.pool)

	switch {
	case img.Spec.SnapshotRef != nil:
		snapshotRef := img.Spec.SnapshotRef
		ok, err := r.createImageFromSnapshot(ctx, log, ioCtx, img, *snapshotRef, options)
		if err != nil {
			return fmt.Errorf("failed to create image from snapshot: %w", err)
		}
		if !ok {
			return nil
		}

	default:
		if err := r.createEmptyImage(ctx, log, ioCtx, img, options); err != nil {
			return fmt.Errorf("failed to create empty image: %w", err)
		}
	}

	if len(img.Spec.Limits) > 0 {
		if err := r.setImageLimits(ctx, log, ioCtx, img); err != nil {
			return fmt.Errorf("failed to set limits: %w", err)
		}
	}

	user, key, err := r.fetchAuth(ctx, log)
	if err != nil {
		return fmt.Errorf("failed to fetch credentials: %w", err)
	}

	img.Status.Access = &api.ImageAccess{
		Monitors: r.monitors,
		Handle:   fmt.Sprintf("%s/%s", r.pool, img.ID),
		User:     user,
		UserKey:  key,
		WWN:      r.wwnGen.Generate(),
	}
	img.Status.State = api.ImageStateAvailable
	if _, err = r.images.Update(ctx, img); err != nil {
		return fmt.Errorf("failed to update image metadate: %w", err)
	}

	log.V(1).Info("Successfully created image")

	return nil
}

func (r *ImageReconciler) setImageLimits(ctx context.Context, log logr.Logger, ioCtx *rados.IOContext, image *api.Image) error {
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
		log.V(3).Info("Set image limit", "limit", limit, "value", value)
	}

	if err := img.Close(); err != nil {
		return fmt.Errorf("failed to close rbd image: %w", err)
	}

	return nil
}

func (r *ImageReconciler) createEmptyImage(ctx context.Context, log logr.Logger, ioCtx *rados.IOContext, image *api.Image, options *librbd.ImageOptions) error {
	if err := librbd.CreateImage(ioCtx, ImageIDToRBDID(image.ID), round.OffBytes(image.Spec.Size), options); err != nil {
		return fmt.Errorf("failed to create rbd image: %w", err)
	}
	log.V(2).Info("Created image", "bytes", image.Spec.Size)

	return nil
}

func (r *ImageReconciler) createImageFromSnapshot(ctx context.Context, log logr.Logger, ioCtx *rados.IOContext, image *api.Image, snapshotRef string, options *librbd.ImageOptions) (bool, error) {
	snapshot, err := r.snapshots.Get(ctx, snapshotRef)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return false, fmt.Errorf("failed to get snapshot: %w", err)
		}

		log.V(1).Info("snapshot not found", "snapshotID", snapshot.ID)

		return false, nil
	}

	if snapshot.Status.State != api.SnapshotStatePopulated {
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

	log.V(2).Info("Cloned image")
	return true, nil
}
