// SPDX-FileCopyrightText: 2023 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package omap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ceph/go-ceph/rados"
	"github.com/go-logr/logr"
	"github.com/ironcore-dev/ceph-provider/internal/utils"

	apiutils "github.com/ironcore-dev/provider-utils/apiutils/api"
	"github.com/ironcore-dev/provider-utils/storeutils/store"
	"k8s.io/apimachinery/pkg/util/sets"
	//"k8s.io/apimachinery/pkg/util/wait"
)

type CreateStrategy[E apiutils.Object] interface {
	PrepareForCreate(obj E)
}

type CacheItem struct {
	ID      string
	RawData []byte
}

type sizedLabel struct {
	ids  sets.Set[string]
	size int
}

var (
	ErrResourceVersionNotLatest = errors.New("resourceVersion is not latest")

	ErrInitializedAlready     = errors.New("cache is already initialized")
	ErrInitializedNotExecuted = errors.New("cache initialization wasn't executed")
)

type Options[E apiutils.Object] struct {
	OmapName       string
	NewFunc        func() E
	CreateStrategy CreateStrategy[E]
}

func New[E apiutils.Object](conn RadosConnection, pool string, log logr.Logger, opts Options[E]) (*Store[E], error) {
	if conn == nil {
		return nil, fmt.Errorf("must specify conn")
	}

	if pool == "" {
		return nil, fmt.Errorf("must specify pool")
	}

	if opts.OmapName == "" {
		return nil, fmt.Errorf("must specify opts.OmapName")
	}

	if opts.NewFunc == nil {
		return nil, fmt.Errorf("must specify opts.NewFunc")
	}

	return &Store[E]{

		conn:     conn,
		pool:     pool,
		omapName: opts.OmapName,
		// Initialize cache map to store raw bytes
		cache: make(map[string][]byte),
		// Initialize the label index
		labelIndex: make(map[string]sets.Set[string]),
		watches:    sets.New[*watch[E]](),

		newFunc:        opts.NewFunc,
		createStrategy: opts.CreateStrategy,
		log:            log,
	}, nil
}

type Store[E apiutils.Object] struct {
	conn     RadosConnection
	pool     string
	omapName string
	log      logr.Logger

	newFunc        func() E
	createStrategy CreateStrategy[E]

	// Cache related fields
	cacheMu     sync.RWMutex
	cache       map[string][]byte           // Cache now stores raw byte data
	labelIndex  map[string]sets.Set[string] // Add label index field (labelKey=labelValue -> Set[objectID])
	initialized atomic.Bool

	// Watch related fields
	watchesMu sync.RWMutex
	watches   sets.Set[*watch[E]]
}

// --- Internal Label Index Helper ---
func formatLabel(key, value string) string {
	return fmt.Sprintf("%s=%s", key, value)
}

// updateLabelIndex updates the index for a single object based on its labels.
func (s *Store[E]) updateLabelIndex(objID string, oldLabels, newLabels map[string]string) {
	s.log.V(2).Info("Updating label index", "id", objID)
	oldLabelSet := sets.New[string]()
	for k, v := range oldLabels {
		oldLabelSet.Insert(formatLabel(k, v))
	}

	newLabelSet := sets.New[string]()
	for k, v := range newLabels {
		newLabelSet.Insert(formatLabel(k, v))
	}

	// Labels to remove objID from
	for label := range oldLabelSet.Difference(newLabelSet) {
		if ids, ok := s.labelIndex[label]; ok {
			ids.Delete(objID)
			if ids.Len() == 0 {
				delete(s.labelIndex, label)
			}
		}
	}

	for label := range newLabelSet.Difference(oldLabelSet) {
		if _, ok := s.labelIndex[label]; !ok {
			s.labelIndex[label] = sets.New[string]()
		}
		s.labelIndex[label].Insert(objID)
	}
	s.log.V(2).Info("Label index updated", "id", objID)
}

// removeFromLabelIndex removes an object entirely from the label index.
func (s *Store[E]) removeFromLabelIndex(objID string, labels map[string]string) {
	s.log.V(2).Info("Removing object from label index", "id", objID)
	for k, v := range labels {
		label := formatLabel(k, v)
		if ids, ok := s.labelIndex[label]; ok {
			ids.Delete(objID)
			if ids.Len() == 0 {
				delete(s.labelIndex, label)
			}
		}
	}
	s.log.V(2).Info("Object removed from label index", "id", objID)
}

func (s *Store[O]) InitializeCache() error {
	s.log.Info("Initializing cache from backend")
	err := s.initializeCache()
	if err != nil {
		return fmt.Errorf("failed to initialize '%s' cache: %w", s.omapName, err)
	}

	return nil
}

// initializeCache performs a one-time, thread-safe initialization of the in-memory cache.
func (s *Store[E]) initializeCache() error {
	// The initialization role using atomic Compare-And-Swap.
	if !s.initialized.CompareAndSwap(false, true) {
		return ErrInitializedAlready
	}
	s.log.V(1).Info("Starting OMAP cache initialization")

	// Open Context
	ioCtx, err := s.conn.OpenIOContext(s.pool)
	if err != nil {
		return fmt.Errorf("failed to open IO context: %w", err)
	}
	defer ioCtx.Destroy()

	// Fetch OMAP Values
	omapValues, err := ioCtx.GetAllOmapValues(s.omapName, "", "", DefaultIteratorSize)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			s.log.V(1).Info("No keys found in OMAP, starting with empty cache and index.")
			return nil // Success, empty cache. finalErr is nil.
		}
		return fmt.Errorf("failed to get all omap values: %w", err)
	}

	// Populate Cache and Index (Local)
	s.log.V(1).Info("Starting initialization of label index")

	s.cache = make(map[string][]byte, len(omapValues))
	s.labelIndex = make(map[string]sets.Set[string])

	for k, v := range omapValues {
		s.cache[k] = v
		obj := s.newFunc()
		if err := json.Unmarshal(v, &obj); err != nil {
			// Log the error but DO NOT return it. Allow initialization to continue.
			s.log.Error(err, "Failed to unmarshal object during cache init for label indexing, skipping index update", "id", k)
			continue // Skip indexing for this object
		}

		labels := obj.GetLabels()
		if len(labels) == 0 {
			s.log.V(2).Info("Object has no labels", "objectID", obj.GetID())
		}
		for labelKey, labelValue := range labels {
			label := formatLabel(labelKey, labelValue)
			if _, ok := s.labelIndex[label]; !ok {
				s.labelIndex[label] = sets.New[string]()
			}
			s.labelIndex[label].Insert(k)
		}
	}

	s.log.V(1).Info("OMAP cache and label index initialized successfully", "indexedLabels", len(s.labelIndex))
	return nil
}

// enqueue sends the *unmarshaled* object to watchers.
func (s *Store[E]) enqueue(evt store.WatchEvent[E]) {
	s.watchesMu.RLock()
	watchers := s.watches.UnsortedList()
	s.watchesMu.RUnlock()

	for _, handler := range watchers {
		select {
		case handler.events <- evt:
		default:
			s.log.V(1).Info("Watch channel buffer full, dropping event", "event", evt)
		}
	}
}

// --- Internal OMAP Helper Methods ---

// getSingleOmapValue retrieves only one value, maxReturn=1 is correct here.
func (s *Store[E]) getSingleOmapValue(ioCtx RadosIOContext, omapName, key string) ([]byte, error) {
	// Use maxReturn=1 because we only want *this specific key* if it exists.
	omap, err := ioCtx.GetAllOmapValues(omapName, "", key, 1) // Fetch starting from key, limit 1
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return nil, rados.ErrNotFound
		}
		return nil, err
	}
	value, ok := omap[key]
	if !ok {
		return nil, rados.ErrNotFound
	}
	return value, nil
}

func (s *Store[E]) deleteOmapValue(ioCtx RadosIOContext, omapName, key string) error {
	if err := ioCtx.RmOmapKeys(omapName, []string{key}); err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return err
		}
		return fmt.Errorf("unable to delete omap value for key %q: %w", key, err)
	}
	return nil
}

func (s *Store[E]) setOmapValue(ioCtx RadosIOContext, omapName, key string, value []byte) error {
	if err := ioCtx.SetOmap(omapName, map[string][]byte{
		key: value,
	}); err != nil {
		return fmt.Errorf("unable to set omap values: %w", err)
	}

	return nil
}

// --- Internal OMAP Operation Wrappers ---

// set performs the OMAP write operation.
func (s *Store[E]) set(ioCtx RadosIOContext, obj E) (E, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return utils.Zero[E](), fmt.Errorf("failed to marshal obj: %w", err)
	}
	if err := s.setOmapValue(ioCtx, s.omapName, obj.GetID(), data); err != nil {
		return utils.Zero[E](), fmt.Errorf("failed to put os object mapping: %w", err)
	}
	return obj, nil
}

// getFromOmap retrieves a single object directly from OMAP (used internally before writes).
func (s *Store[E]) getFromOmap(ioCtx RadosIOContext, id string) (E, []byte, error) {
	s.log.V(2).Info("Directly loading object from omap", "id", id)
	rawData, err := s.getSingleOmapValue(ioCtx, s.omapName, id)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return utils.Zero[E](), nil, fmt.Errorf("object with id %q %w", id, store.ErrNotFound)
		}
		return utils.Zero[E](), nil, fmt.Errorf("failed to fetch omap value for id %q: %w", id, err)
	}
	obj := s.newFunc()
	if err := json.Unmarshal(rawData, &obj); err != nil {
		return utils.Zero[E](), rawData, fmt.Errorf("failed to unmarshal object data for id %q from omap during internal check: %w", id, err)

	}
	return obj, rawData, nil
}

// deleteFromOmap performs the physical OMAP delete operation.
func (s *Store[E]) deleteFromOmap(ioCtx RadosIOContext, id string) error {
	if err := s.deleteOmapValue(ioCtx, s.omapName, id); err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			s.log.V(1).Info("Attempted to delete non-existent key from omap", "id", id)
			return store.ErrNotFound
		}
		return fmt.Errorf("failed to delete object %q from omap: %w", id, err)
	}
	return nil
}

// --- Public Methods ---

func (s *Store[E]) Create(ctx context.Context, obj E) (E, error) {
	if !s.initialized.Load() {
		return utils.Zero[E](), ErrInitializedNotExecuted
	}

	objID := obj.GetID()
	s.log.V(1).Info("Creating object", "id", objID)

	ioCtx, err := s.conn.OpenIOContext(s.pool)
	if err != nil {
		return utils.Zero[E](), fmt.Errorf("unable to get io context for create check: %w", err)
	}
	defer ioCtx.Destroy()

	// Check OMAP directly for existence before create
	_, _, err = s.getFromOmap(ioCtx, objID) // Ignore obj and rawData
	switch {
	case err == nil:
		return utils.Zero[E](), fmt.Errorf("object with id %q %w", objID, store.ErrAlreadyExists)
	case errors.Is(err, store.ErrNotFound):
		break
	default:
		if strings.Contains(err.Error(), "failed to unmarshal") {
			s.log.Error(err, "Corrupted data found in OMAP during create check, proceeding cautiously", "id", objID)
		} else {
			return utils.Zero[E](), fmt.Errorf("failed to check object existence in omap for id %q: %w", objID, err)
		}
	}

	if s.createStrategy != nil {
		s.createStrategy.PrepareForCreate(obj)
	}
	obj.SetCreatedAt(time.Now().UTC().Truncate(time.Microsecond))
	obj.IncrementResourceVersion()

	rawData, err := json.Marshal(obj)
	if err != nil {
		return utils.Zero[E](), fmt.Errorf("failed to marshal object for cache: %w", err)
	}

	persistedObject, err := s.set(ioCtx, obj)
	if err != nil {
		return utils.Zero[E](), fmt.Errorf("failed to set object in omap: %w", err)
	}

	currentLabels := persistedObject.GetLabels()

	s.cacheMu.Lock()

	s.cache[persistedObject.GetID()] = rawData
	s.updateLabelIndex(objID, nil, currentLabels)
	s.cacheMu.Unlock()

	s.enqueue(store.WatchEvent[E]{
		Type:   store.WatchEventTypeCreated,
		Object: obj,
	})

	s.log.V(1).Info("Object created successfully", "id", objID, "resourceVersion", obj.GetResourceVersion())
	return obj, nil
}

func (s *Store[E]) Delete(ctx context.Context, id string) error {
	if !s.initialized.Load() {
		return ErrInitializedNotExecuted
	}

	s.log.V(1).Info("Deleting object", "id", id)

	ioCtx, err := s.conn.OpenIOContext(s.pool)
	if err != nil {
		return fmt.Errorf("unable to get io context for delete: %w", err)
	}
	defer ioCtx.Destroy()

	obj, rawDataBeforeDelete, err := s.getFromOmap(ioCtx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			s.log.V(1).Info("Object not found in OMAP, ensuring removal from cache and index", "id", id)
			s.cacheMu.Lock()
			if oldRawData, exists := s.cache[id]; exists {
				oldObjForLabels := s.newFunc()
				_ = json.Unmarshal(oldRawData, &oldObjForLabels)
				oldLabels := oldObjForLabels.GetLabels()
				s.removeFromLabelIndex(id, oldLabels)
				delete(s.cache, id)
			}
			s.cacheMu.Unlock()
			return nil
		}
		if strings.Contains(err.Error(), "failed to unmarshal") {
			s.log.Error(err, "Corrupted data found in OMAP during delete check, attempting physical delete", "id", id)
		} else {
			return fmt.Errorf("failed to get object from omap for delete check: %w", err)
		}
	}

	labelsBeforeDelete := obj.GetLabels()

	if obj.GetID() != "" && len(obj.GetFinalizers()) > 0 {
		if obj.GetDeletedAt() != nil {
			s.log.V(1).Info("Object already marked for deletion", "id", id)
			s.cacheMu.Lock()
			s.cache[id] = rawDataBeforeDelete
			s.cacheMu.Unlock()
			return nil
		}

		s.log.V(1).Info("Marking object for deletion (soft delete)", "id", id)
		now := time.Now().UTC().Truncate(time.Microsecond)
		obj.SetDeletedAt(&now)
		obj.IncrementResourceVersion()

		_, err := s.set(ioCtx, obj)
		if err != nil {
			return fmt.Errorf("failed to set object metadata for soft deletion in omap: %w", err)
		}

		rawDataAfterSoftDelete, err := json.Marshal(obj)
		if err != nil {
			return fmt.Errorf("failed to marshal object for cache: %w", err)
		}

		s.cacheMu.Lock()
		s.cache[id] = rawDataAfterSoftDelete
		s.cacheMu.Unlock()

		s.enqueue(store.WatchEvent[E]{
			Type:   store.WatchEventTypeDeleted,
			Object: obj,
		})
		s.log.V(1).Info("Object marked for deletion (soft delete) completed", "id", id, "resourceVersion", obj.GetResourceVersion())
		return nil
	}

	s.log.V(1).Info("Physically deleting object", "id", id)
	if err := s.deleteFromOmap(ioCtx, id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			s.log.V(1).Info("Object already physically deleted from OMAP", "id", id)
		} else {
			return fmt.Errorf("failed to physically delete object from omap: %w", err)
		}
	}

	s.cacheMu.Lock()
	delete(s.cache, id)
	s.removeFromLabelIndex(id, labelsBeforeDelete)
	s.cacheMu.Unlock()

	s.enqueue(store.WatchEvent[E]{
		Type:   store.WatchEventTypeDeleted,
		Object: obj,
	})

	s.log.V(1).Info("Object physically deleted successfully", "id", id)
	return nil
}
func (s *Store[E]) Get(ctx context.Context, id string) (E, error) {
	if !s.initialized.Load() {
		return utils.Zero[E](), ErrInitializedNotExecuted
	}

	s.log.V(2).Info("Getting object from cache", "id", id)
	s.cacheMu.RLock()
	rawData, found := s.cache[id]
	s.cacheMu.RUnlock()

	if !found {
		s.log.V(1).Info("Object not found in cache", "id", id)
		return utils.Zero[E](), fmt.Errorf("object with id %q %w", id, store.ErrNotFound)
	}

	obj := s.newFunc()
	if err := json.Unmarshal(rawData, &obj); err != nil {
		s.log.Error(err, "Failed to unmarshal object data from cache", "id", id)
		// Consider removing corrupted data from cache
		s.cacheMu.Lock()
		delete(s.cache, id)
		s.cacheMu.Unlock()
		return utils.Zero[E](), fmt.Errorf("failed to unmarshal object data for id %q: %w", id, err)
	}
	s.log.V(2).Info("Object found and unmarshaled from cache", "id", id)
	return obj, nil
}

func (s *Store[E]) Update(ctx context.Context, obj E) (E, error) {
	if !s.initialized.Load() {
		return utils.Zero[E](), ErrInitializedNotExecuted
	}

	objID := obj.GetID()
	s.log.V(1).Info("Updating object", "id", objID, "inputResourceVersion", obj.GetResourceVersion())

	ioCtx, err := s.conn.OpenIOContext(s.pool)
	if err != nil {
		return utils.Zero[E](), fmt.Errorf("unable to get io context for update: %w", err)
	}
	defer ioCtx.Destroy()

	oldObj, _, err := s.getFromOmap(ioCtx, objID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			s.log.V(1).Info("Object not found in OMAP during update check", "id", objID)
			s.cacheMu.Lock()
			delete(s.cache, objID)
			s.cacheMu.Unlock()
			return utils.Zero[E](), err
		}
		if strings.Contains(err.Error(), "failed to unmarshal") {
			s.log.Error(err, "Corrupted data found in OMAP during update check", "id", objID)
			return utils.Zero[E](), fmt.Errorf("cannot update object %q due to existing corrupted data in OMAP: %w", objID, err)
		}
		return utils.Zero[E](), fmt.Errorf("failed to get existing object from omap for update check: %w", err)
	}

	oldLabels := oldObj.GetLabels()
	newLabels := obj.GetLabels()

	// Begin OMAP Update Logic
	deleted := false
	var rawDataAfterUpdate []byte

	if obj.GetDeletedAt() != nil && len(obj.GetFinalizers()) == 0 {
		s.log.V(1).Info("Update triggers physical deletion", "id", objID)
		if oldObj.GetResourceVersion() != obj.GetResourceVersion() {
			s.log.V(1).Info("ResourceVersion mismatch during update-triggered delete", "id", objID, "expected", oldObj.GetResourceVersion(), "got", obj.GetResourceVersion())
			return utils.Zero[E](), fmt.Errorf("failed to delete object during update: %w", ErrResourceVersionNotLatest)
		}

		if err := s.deleteFromOmap(ioCtx, objID); err != nil {
			if !errors.Is(err, store.ErrNotFound) {
				return utils.Zero[E](), fmt.Errorf("failed to delete object from omap during update: %w", err)
			}
			s.log.V(1).Info("Object already physically deleted from OMAP during update", "id", objID)
		}
		deleted = true
	} else {
		s.log.V(1).Info("Performing standard update", "id", objID)
		if oldObj.GetResourceVersion() != obj.GetResourceVersion() {
			s.log.V(1).Info("ResourceVersion mismatch during update", "id", objID, "expected", oldObj.GetResourceVersion(), "got", obj.GetResourceVersion())
			return utils.Zero[E](), fmt.Errorf("failed to update object: %w", ErrResourceVersionNotLatest)
		}
		obj.IncrementResourceVersion()

		_, err = s.set(ioCtx, obj)
		if err != nil {
			return utils.Zero[E](), fmt.Errorf("failed to set object in omap during update: %w", err)
		}

		rawDataAfterUpdate, err = json.Marshal(obj)
		if err != nil {
			return utils.Zero[E](), fmt.Errorf("failed to marshal object for cache update: %w", err)
		}

		s.log.V(1).Info("Object updated in OMAP", "id", objID, "newResourceVersion", obj.GetResourceVersion())
	}

	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	var eventType store.WatchEventType
	if deleted {
		delete(s.cache, objID)
		s.removeFromLabelIndex(objID, oldLabels)
		eventType = store.WatchEventTypeDeleted
		s.log.V(1).Info("Object removed from cache and index", "id", objID)
	} else {
		s.cache[objID] = rawDataAfterUpdate
		s.updateLabelIndex(objID, oldLabels, newLabels)
		eventType = store.WatchEventTypeUpdated
		s.log.V(1).Info("Object updated in cache and index", "id", objID)
	}

	s.enqueue(store.WatchEvent[E]{
		Type:   eventType,
		Object: obj,
	})

	s.log.V(1).Info("Update operation completed", "id", objID, "deleted", deleted, "finalResourceVersion", obj.GetResourceVersion())
	return obj, nil
}

type watch[E apiutils.Object] struct {
	store  *Store[E]
	events chan store.WatchEvent[E]
}

func (w *watch[E]) Stop() {
	w.store.watchesMu.Lock()
	defer w.store.watchesMu.Unlock()

	if w.store.watches.Has(w) {
		w.store.watches.Delete(w)
		close(w.events)
		w.store.log.V(1).Info("Stopped and closed watch channel")
	} else {
		w.store.log.V(1).Info("Attempted to stop a watch that was already stopped or removed")
	}
}

func (w *watch[E]) Events() <-chan store.WatchEvent[E] {
	return w.events
}

func (s *Store[E]) Watch(ctx context.Context) (store.Watch[E], error) {
	if !s.initialized.Load() {
		return nil, ErrInitializedNotExecuted
	}

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled or timed out before Watch creation: %w", err)
	}

	s.watchesMu.Lock()
	defer s.watchesMu.Unlock()

	w := &watch[E]{
		store:  s,
		events: make(chan store.WatchEvent[E], EventBufferSize),
	}

	s.watches.Insert(w)
	s.log.V(1).Info("Watch created")
	return w, nil
}

func (s *Store[E]) List(ctx context.Context) ([]E, error) {
	if !s.initialized.Load() {
		return nil, ErrInitializedNotExecuted
	}

	s.log.V(1).Info("Attempting to list objects from in-memory cache", "count", len(s.cache))
	s.cacheMu.RLock()

	// Pre-allocate the list capacity to avoid memory re-allocations
	itemsList := make([]CacheItem, 0, len(s.cache))

	// Create a snapshot (deep copy) while holding the lock
	for id, data := range s.cache {
		// Deep copy the []byte data. This prevents data corruption
		// if an external source mutates the underlying array after the lock is released.
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)

		itemsList = append(itemsList, CacheItem{
			ID:      id,
			RawData: dataCopy,
		})
	}

	s.cacheMu.RUnlock()

	objs := make([]E, 0, len(itemsList))
	for _, item := range itemsList {
		obj := s.newFunc()

		if err := json.Unmarshal(item.RawData, &obj); err != nil {
			s.log.Error(err, "Failed to unmarshal object data from cache during List", "id", item.ID)
			return nil, fmt.Errorf("failed to unmarshal object data for id %q during list: %w", item.ID, err)
		}
		objs = append(objs, obj)
	}

	s.log.V(2).Info("Successfully listed objects from cache", "count", len(objs))
	return objs, nil
}

func (s *Store[E]) ListByLabels(ctx context.Context, labelSelector map[string]string) ([]E, error) {
	if !s.initialized.Load() {
		return nil, ErrInitializedNotExecuted
	}
	if len(labelSelector) == 0 {
		s.log.V(1).Info("Empty label selector provided, returning all items (like List)")
		return s.List(ctx)
	}

	s.log.V(1).Info("Listing objects by labels from index", "selector", labelSelector)
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()

	// pre-allocate the slice to avoid extra memory allocations
	labelSelect := make([]sizedLabel, 0, len(labelSelector))
	var intersection sets.Set[string]

	// 1 .Gather label set sizes and check for immediate non-matches.
	for key, value := range labelSelector {
		label := formatLabel(key, value)
		ids, found := s.labelIndex[label]
		if !found {
			s.log.V(1).Info("Label not found in index, no objects match the full selector", "label", label, "selector", labelSelector)
			return []E{}, nil
		}

		labelSelect = append(labelSelect, sizedLabel{
			ids:  ids,
			size: ids.Len(),
		})
	}
	if len(labelSelect) > 1 {
		// 2. Sort the labels by the size of their matching set (smallest first).
		sort.Slice(labelSelect, func(i, j int) bool {
			return labelSelect[i].size < labelSelect[j].size
		})
	}

	var isFirstLabel = true

	// 3. Iterate over the sorted slice (labelsForSort) to compute the intersection.
	for _, info := range labelSelect {
		ids := info.ids
		if isFirstLabel {
			// Use the smallest set to initialize the intersection (copy to avoid modifying the index set).
			intersection = ids.Clone()
			s.log.V(1).Info("Initialized intersection set with label", "initial_count", intersection.Len())
			isFirstLabel = false
		} else {
			// Intersect the current result with the next smallest set.
			prevCount := intersection.Len()
			intersection = intersection.Intersection(ids)
			s.log.V(1).Info("Computed intersection", "previous_count", prevCount, "new_count", intersection.Len())
		}

		if intersection.Len() == 0 {
			s.log.V(1).Info("Intersection of label matches became empty, returning empty list", "selector", labelSelector)
			return []E{}, nil
		}
	}

	objs := make([]E, 0, intersection.Len())
	for id := range intersection {
		rawData, found := s.cache[id]
		if !found {
			err := fmt.Errorf("object ID %q found in label index but not in cache during ListByLabels", id)
			s.log.Error(err, "Cache inconsistency detected")

			return nil, err
		}

		obj := s.newFunc()
		if err := json.Unmarshal(rawData, &obj); err != nil {
			s.log.Error(err, "Failed to unmarshal object data from cache during ListByLabels", "id", id)
			unmarshalErr := fmt.Errorf("failed to unmarshal object data for id %q during ListByLabels: %w", id, err)
			return nil, unmarshalErr
		}

		objs = append(objs, obj)
	}

	s.log.V(1).Info("Successfully retrieved objects matching label selector", "count", len(objs), "selector", labelSelector)
	return objs, nil
}
