package omap_test

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ceph/go-ceph/rados"
	"github.com/ironcore-dev/ceph-provider/internal/omap"
	"github.com/ironcore-dev/provider-utils/apiutils/api"
)

// mockObject implements api.Object for testing purposes.
type mockObject struct {
	ID              string            `json:"id"`
	Metadata        map[string]string `json:"metadata,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	Annotations     map[string]string `json:"annotations,omitempty"`
	Generation      int64             `json:"generation,omitempty"`
	ResourceVersion uint64            `json:"resourceVersion"`
	Spec            string            `json:"spec"`
	Status          string            `json:"status,omitempty"`
	CreatedAt       time.Time         `json:"createdAt,omitempty"`
	DeletedAt       *time.Time        `json:"deletedAt,omitempty"`
	Finalizers      []string          `json:"finalizers,omitempty"`
}

// Ensure mockObject implements api.Object
var _ api.Object = (*mockObject)(nil)

// --- Getters ---
func (m *mockObject) GetID() string                     { return m.ID }
func (m *mockObject) GetMetadata() map[string]string    { return m.Metadata }
func (m *mockObject) GetLabels() map[string]string      { return m.Labels }
func (m *mockObject) GetAnnotations() map[string]string { return m.Annotations }
func (m *mockObject) GetGeneration() int64              { return m.Generation }
func (m *mockObject) GetFinalizers() []string           { return m.Finalizers }
func (m *mockObject) GetCreatedAt() time.Time           { return m.CreatedAt }
func (m *mockObject) GetDeletedAt() *time.Time          { return m.DeletedAt }
func (m *mockObject) GetResourceVersion() uint64        { return m.ResourceVersion }

// --- Setters ---
func (m *mockObject) SetID(id string)                              { m.ID = id }
func (m *mockObject) SetMetadata(metadata map[string]string)       { m.Metadata = metadata }
func (m *mockObject) SetLabels(labels map[string]string)           { m.Labels = labels }
func (m *mockObject) SetAnnotations(annotations map[string]string) { m.Annotations = annotations }
func (m *mockObject) SetGeneration(generation int64)               { m.Generation = generation }
func (m *mockObject) SetFinalizers(finalizers []string)            { m.Finalizers = finalizers }
func (m *mockObject) SetCreatedAt(createdAt time.Time)             { m.CreatedAt = createdAt }
func (m *mockObject) SetDeletedAt(deletedAt *time.Time)            { m.DeletedAt = deletedAt }
func (m *mockObject) SetResourceVersion(rv uint64)                 { m.ResourceVersion = rv }

// --- Mutators ---
func (m *mockObject) IncrementResourceVersion() {
	m.ResourceVersion++
}

// newMockObject helper function initializes ResourceVersion to 0
func newMockObject(id, spec string) *mockObject {
	return &mockObject{
		ID:              id,
		Spec:            spec,
		Labels:          make(map[string]string),
		Annotations:     make(map[string]string),
		Metadata:        make(map[string]string),
		ResourceVersion: 0,
	}
}

// --- Mock Implementation of Rados Interfaces ---

// mockRadosConnection implements omap.RadosConnection for testing.
// It now holds the single mutex protecting the shared omaps data.
type mockRadosConnection struct {
	mu           sync.RWMutex // Single mutex for connection and shared omap data
	omaps        map[string]map[string][]byte
	failOpenCtx  error
	ioctxFailOps map[string]error // Failures configured *before* OpenIOContext is called
}

var _ omap.RadosConnection = (*mockRadosConnection)(nil)

func newMockRadosConnection() *mockRadosConnection {
	return &mockRadosConnection{
		omaps:        make(map[string]map[string][]byte),
		ioctxFailOps: make(map[string]error),
	}
}

// mockRadosIOContext implements omap.RadosIOContext for testing.
// It no longer has its own mutex, uses the connection's mutex.
type mockRadosIOContext struct {
	connMu    *sync.RWMutex                // Reference to the connection's mutex
	omaps     map[string]map[string][]byte // Reference to shared omap data store
	failOps   map[string]error             // Operation-specific failures for this instance
	failNext  map[string]bool
	destroyed bool
}

var _ omap.RadosIOContext = (*mockRadosIOContext)(nil)

// OpenIOContext creates a mock IOContext that uses the connection's mutex.
func (m *mockRadosConnection) OpenIOContext(_ string) (omap.RadosIOContext, error) {
	m.mu.RLock() // Lock connection mutex to read failOpenCtx and ioctxFailOps
	defer m.mu.RUnlock()

	if m.failOpenCtx != nil {
		return nil, m.failOpenCtx
	}

	// Create IOContext with a reference to the connection's mutex and data
	mockIoCtx := &mockRadosIOContext{
		connMu:   &m.mu,   // Pass reference to the connection's mutex
		omaps:    m.omaps, // Pass reference to the shared map
		failOps:  make(map[string]error),
		failNext: make(map[string]bool),
	}

	// Copy pre-configured failures under the read lock
	for op, err := range m.ioctxFailOps {
		mockIoCtx.failOps[op] = err
	}
	return mockIoCtx, nil
}

// --- mockRadosIOContext methods now use connMu ---

func (m *mockRadosIOContext) checkFail(opName string) error {
	// Lock the *connection's* mutex to check failOps for this IOContext instance
	m.connMu.Lock() // Use Write lock as we might modify failNext map
	defer m.connMu.Unlock()
	if err, ok := m.failOps[opName]; ok {
		// fmt.Printf("DEBUG: Mock checkFail triggered for %s: %v\n", opName, err) // DEBUG removed
		if failNext := m.failNext[opName]; failNext {
			delete(m.failNext, opName)
			return err
		}
		return err
	}
	return nil
}

func (m *mockRadosIOContext) SetOmap(omapName string, pairs map[string][]byte) error {
	// Check for failure *before* acquiring write lock
	if err := m.checkFail("SetOmap"); err != nil {
		return err
	}
	m.connMu.Lock()
	defer m.connMu.Unlock()
	if m.destroyed {
		return errors.New("mock IOContext already destroyed")
	}
	if _, ok := m.omaps[omapName]; !ok {
		m.omaps[omapName] = make(map[string][]byte)
	}
	for k, v := range pairs {
		valCopy := make([]byte, len(v))
		copy(valCopy, v)
		m.omaps[omapName][k] = valCopy
	}
	return nil
}

func (m *mockRadosIOContext) RmOmapKeys(omapName string, keys []string) error {
	// Check for failure *before* acquiring write lock
	if err := m.checkFail("RmOmapKeys"); err != nil {
		return err
	}
	m.connMu.Lock()
	defer m.connMu.Unlock()
	if m.destroyed {
		return errors.New("mock IOContext already destroyed")
	}
	om, ok := m.omaps[omapName]
	if !ok {
		return nil
	}
	for _, k := range keys {
		delete(om, k)
	}
	return nil
}

// GetAllOmapValues mock implementation.
// This mock *ignores* maxReturn because the real function iterates internally.
// It only applies startKey and filterPrefix filtering.
func (m *mockRadosIOContext) GetAllOmapValues(omapName, startKey, filterPrefix string, _ int64) (map[string][]byte, error) {
	// Check for failure *before* acquiring read lock
	if err := m.checkFail("GetAllOmapValues"); err != nil {
		return nil, err
	}
	m.connMu.RLock()
	defer m.connMu.RUnlock()
	if m.destroyed {
		return nil, errors.New("mock IOContext already destroyed")
	}
	omapData, ok := m.omaps[omapName]
	if !ok {
		// Return ErrNotFound if the specific omap doesn't exist
		return nil, rados.ErrNotFound
	}
	result := make(map[string][]byte)
	allKeys := make([]string, 0, len(omapData))
	for k := range omapData {
		allKeys = append(allKeys, k)
	}
	sort.Strings(allKeys) // Ensure consistent order for filtering

	for _, k := range allKeys {
		// Apply startKey filtering (inclusive)
		if startKey != "" && k < startKey {
			continue
		}
		// Apply filterPrefix filtering
		if filterPrefix != "" && !strings.HasPrefix(k, filterPrefix) {
			continue
		}

		valCopy := make([]byte, len(omapData[k]))
		copy(valCopy, omapData[k])
		result[k] = valCopy
	}

	// If after filtering, the result map is empty, but the omap itself existed,
	// it means no keys matched the filter. Return an empty map, not ErrNotFound.
	// If the omapData was empty to begin with, this still correctly returns an empty map.
	return result, nil
}

func (m *mockRadosIOContext) Destroy() {
	m.connMu.Lock()
	defer m.connMu.Unlock()
	m.destroyed = true
}

// SetFailOp is specific to the IOContext instance
func (m *mockRadosIOContext) SetFailOp(opName string, err error, failNext bool) {
	m.connMu.Lock() // Use connection mutex to protect failOps/failNext
	defer m.connMu.Unlock()
	if err == nil {
		delete(m.failOps, opName)
		delete(m.failNext, opName)
	} else {
		m.failOps[opName] = err
		m.failNext[opName] = failNext
	}
}

// --- mockRadosConnection helper methods ---

func (m *mockRadosConnection) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.omaps = make(map[string]map[string][]byte)
	m.failOpenCtx = nil
	m.ioctxFailOps = make(map[string]error)
}

func (m *mockRadosConnection) Populate(omapName string, data map[string][]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.omaps[omapName]; !ok {
		m.omaps[omapName] = make(map[string][]byte)
	}
	for k, v := range data {
		valCopy := make([]byte, len(v))
		copy(valCopy, v)
		m.omaps[omapName][k] = valCopy
	}
}

func (m *mockRadosConnection) SetFailOpenIOContext(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failOpenCtx = err
}

// SetIOContextFailOp pre-configures failures for the *next* IOContext created.
func (m *mockRadosConnection) SetIOContextFailOp(opName string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err == nil {
		delete(m.ioctxFailOps, opName)
	} else {
		m.ioctxFailOps[opName] = err
	}
}

// Helper to check current mock state (read lock)
func (m *mockRadosConnection) CheckState(omapName, key string) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	om, omapOk := m.omaps[omapName]
	if !omapOk {
		return nil, false, false
	}
	val, keyOk := om[key]
	return val, true, keyOk
}

// --- Helper Structs/Interfaces for Testing ---

// mockCreateStrategy implements CreateStrategy[*mockObject]
type mockCreateStrategy struct {
	PrepareFunc func(obj *mockObject)
}

func (m *mockCreateStrategy) PrepareForCreate(obj *mockObject) {
	if m.PrepareFunc != nil {
		m.PrepareFunc(obj)
	}
}
