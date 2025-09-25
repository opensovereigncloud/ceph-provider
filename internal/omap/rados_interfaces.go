package omap

import (
	"github.com/ceph/go-ceph/rados"
)

// RadosIOContext defines the subset of rados.IOContext methods used by omap.Store.
// This allows for mocking during testing.
type RadosIOContext interface {
	// SetOmap sets key/value pairs in an object's omap.
	SetOmap(omapName string, pairs map[string][]byte) error
	// RmOmapKeys removes keys from an object's omap.
	RmOmapKeys(omapName string, keys []string) error
	// GetAllOmapValues retrieves key/value pairs from an object's omap.
	// Note: The pagination parameters (startKey, filterPrefix, maxReturn) might need
	// adjustment based on actual usage within the Store's methods.
	// The current Store implementation seems to use it mainly for getting all or single values.
	// Changed maxReturn type to int64 to match go-ceph.
	GetAllOmapValues(omapName, startKey, filterPrefix string, maxReturn int64) (map[string][]byte, error)
	// Destroy releases the resources associated with the IOContext.
	Destroy()
}

// RadosConnection defines the subset of rados.Conn methods used by omap.Store.
// This allows for mocking during testing.
type RadosConnection interface {
	// OpenIOContext opens a context for I/O operations within a specific pool.
	OpenIOContext(pool string) (RadosIOContext, error)
	// Shutdown disconnects from the Ceph cluster. (Add if needed by your application lifecycle)
	// Shutdown()
	// Connect establishes a connection. (Add if needed by your application lifecycle)
	// Connect() error
}

// --- Real Implementation Wrappers ---

// cephRadosIOContext wraps a real *rados.IOContext to implement RadosIOContext.
type cephRadosIOContext struct {
	ioctx *rados.IOContext
}

// Ensure cephRadosIOContext implements RadosIOContext
var _ RadosIOContext = (*cephRadosIOContext)(nil)

func (w *cephRadosIOContext) SetOmap(omapName string, pairs map[string][]byte) error {
	return w.ioctx.SetOmap(omapName, pairs)
}

func (w *cephRadosIOContext) RmOmapKeys(omapName string, keys []string) error {
	return w.ioctx.RmOmapKeys(omapName, keys)
}

func (w *cephRadosIOContext) GetAllOmapValues(omapName, startKey, filterPrefix string, maxReturn int64) (map[string][]byte, error) {
	return w.ioctx.GetAllOmapValues(omapName, startKey, filterPrefix, maxReturn)
}

func (w *cephRadosIOContext) Destroy() {
	w.ioctx.Destroy()
}

// cephRadosConnection wraps a real *rados.Conn to implement RadosConnection.
type cephRadosConnection struct {
	conn *rados.Conn
}

// Ensure cephRadosConnection implements RadosConnection
var _ RadosConnection = (*cephRadosConnection)(nil)

// NewCephRadosConnection creates a wrapper around a real rados.Conn.
func NewCephRadosConnection(conn *rados.Conn) RadosConnection {
	if conn == nil {
		return nil // Or handle error appropriately
	}
	return &cephRadosConnection{conn: conn}
}

func (w *cephRadosConnection) OpenIOContext(pool string) (RadosIOContext, error) {
	ioctx, err := w.conn.OpenIOContext(pool)
	if err != nil {
		return nil, err
	}
	// Wrap the real ioctx in our interface implementation
	return &cephRadosIOContext{ioctx: ioctx}, nil
}

// Add Shutdown() / Connect() wrappers if needed
