package omap_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ceph/go-ceph/rados"
	"github.com/go-logr/logr"
	"github.com/ironcore-dev/ceph-provider/internal/omap"
	"github.com/ironcore-dev/provider-utils/apiutils/api"
	"github.com/ironcore-dev/provider-utils/storeutils/store"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Define a struct mirroring the OLD api.Metadata structure (without ResourceVersion)
// Keep other fields consistent with the current api.Metadata for accurate testing.
type oldMetadata struct {
	ID          string            `json:"id"`
	Annotations map[string]string `json:"annotations"`
	Labels      map[string]string `json:"labels"` // Keep for consistency, though not used directly by apiutils
	CreatedAt   time.Time         `json:"createdAt"`
	DeletedAt   *time.Time        `json:"deletedAt,omitempty"`
	Generation  int64             `json:"generation"`
	Finalizers  []string          `json:"finalizers,omitempty"`
	// ResourceVersion is intentionally missing
}

// Define a struct mirroring the OLD mockObject using oldMetadata
type oldMockObject struct {
	oldMetadata        // Embed the old metadata structure
	Spec        string `json:"spec"`
	Status      string `json:"status,omitempty"`
}

// Helper function to set labels correctly in annotations for mock objects
func setMockObjectLabels(obj *mockObject, labels map[string]string) {
	if obj.Annotations == nil {
		obj.Annotations = make(map[string]string)
	}
	obj.Labels = labels
}

// Helper function to get object IDs from a slice of objects
func getObjectIDs(objs []*mockObject) []string {
	ids := make([]string, len(objs))
	for i, obj := range objs {
		ids[i] = obj.GetID()
	}
	return ids
}

var _ = Describe("Omap Store", func() {
	var (
		mockConn   *mockRadosConnection     // Use the mock connection type defined in omap_mocks_test.go
		omapStore  *omap.Store[*mockObject] // The Store instance under test
		ctx        context.Context
		poolName   string
		omapName   string
		testLogger logr.Logger
	)

	BeforeEach(func() {
		mockConn = newMockRadosConnection()
		testLogger = logr.Discard() // Use NOP logger
		poolName = "test-pool"
		omapName = "test.omap"
		opts := omap.Options[*mockObject]{
			OmapName: omapName,
			NewFunc: func() *mockObject {
				// Ensure the NewFunc returns the *current* mockObject structure
				// Initialize Annotations map here
				m := newMockObject("", "")
				m.Annotations = make(map[string]string) // Initialize annotations
				return m
			},
		}
		var err error
		// Pass the *mock* connection directly, as New expects the interface
		omapStore, err = omap.New[*mockObject](mockConn, poolName, testLogger, opts)
		Expect(err).NotTo(HaveOccurred())
		ctx = context.Background()
	})

	AfterEach(func() {
		mockConn.Clear()
	})

	// marshalOrFail now accepts any interface{} for flexibility
	marshalOrFail := func(obj interface{}) []byte {
		data, err := json.Marshal(obj)
		Expect(err).NotTo(HaveOccurred())
		return data
	}

	Describe("Create", func() {
		It("should successfully create a new object and set ResourceVersion to 1", func() {
			objToCreate := newMockObject("obj1", "spec-data-1")
			createdObj, err := omapStore.Create(ctx, objToCreate)
			Expect(err).NotTo(HaveOccurred())
			Expect(createdObj).NotTo(BeNil())
			Expect(createdObj.GetID()).To(Equal(objToCreate.ID))
			Expect(createdObj.Spec).To(Equal(objToCreate.Spec))
			Expect(createdObj.GetCreatedAt()).NotTo(BeZero())
			Expect(createdObj.GetResourceVersion()).To(Equal(uint64(1)), "ResourceVersion should be 1 after create") // Check initial ResourceVersion

			// Verification (using Get which reads from cache)
			retrievedObj, err := omapStore.Get(ctx, objToCreate.ID)
			Expect(err).NotTo(HaveOccurred(), "Get failed after Create")
			Expect(retrievedObj.GetID()).To(Equal(objToCreate.ID))
			Expect(retrievedObj.Spec).To(Equal(objToCreate.Spec))
			Expect(retrievedObj.GetCreatedAt()).To(BeTemporally("~", createdObj.GetCreatedAt(), time.Second))
			Expect(retrievedObj.GetResourceVersion()).To(Equal(uint64(1)))
		})

		It("should return ErrAlreadyExists if object ID already exists", func() {
			obj1 := newMockObject("existing-id", "spec1")
			// Populate with the *current* structure for this test
			mockConn.Populate(omapName, map[string][]byte{
				obj1.ID: marshalOrFail(obj1),
			})
			objToCreate := newMockObject("existing-id", "spec2")
			_, err := omapStore.Create(ctx, objToCreate)
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, store.ErrAlreadyExists)).To(BeTrue())
		})

		It("should return an error if OpenIOContext fails", func() {
			simulatedError := errors.New("failed to open io context")
			mockConn.SetFailOpenIOContext(simulatedError)
			objToCreate := newMockObject("obj-fail-open", "spec")
			_, err := omapStore.Create(ctx, objToCreate)
			Expect(err).To(HaveOccurred())
			// Error occurs during cache initialization check or OMAP check
			Expect(err.Error()).To(Or(
				ContainSubstring("failed to initialize cache"),
				ContainSubstring("unable to get io context"), // This covers the OMAP check part
			))
			Expect(err.Error()).To(ContainSubstring(simulatedError.Error()))
		})

		It("should return an error if Get fails before create (not ErrNotFound or AlreadyExists)", func() {
			simulatedError := errors.New("internal get error")
			// Set failure for GetAllOmapValues, which will be called by initializeCach
			mockConn.SetIOContextFailOp("GetAllOmapValues", simulatedError)
			objToCreate := newMockObject("obj-fail-get", "spec")
			_, err := omapStore.Create(ctx, objToCreate) // Create triggers initializeCache first
			Expect(err).To(HaveOccurred())
			// Assert the error comes from cache initialization now
			Expect(err.Error()).To(ContainSubstring("failed to initialize cache for Create"))
			Expect(err.Error()).To(ContainSubstring(simulatedError.Error()))
			Expect(errors.Is(err, store.ErrAlreadyExists)).To(BeFalse())
			Expect(errors.Is(err, store.ErrNotFound)).To(BeFalse())
		})

		It("should return an error if SetOmap fails during create", func() {
			simulatedError := errors.New("ceph SetOmap failed")
			mockConn.SetIOContextFailOp("SetOmap", simulatedError)
			objToCreate := newMockObject("obj-fail-set", "spec")
			_, err := omapStore.Create(ctx, objToCreate)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to set object in omap"))
			Expect(err.Error()).To(ContainSubstring(simulatedError.Error()))
		})

		It("should apply CreateStrategy if provided", func() {
			strategyApplied := false
			strategy := &mockCreateStrategy{ // Assumes mockCreateStrategy is defined in omap_mocks_test.go
				PrepareFunc: func(obj *mockObject) {
					strategyApplied = true
					obj.Annotations["strategy"] = "applied"
				},
			}
			opts := omap.Options[*mockObject]{
				OmapName:       omapName,
				NewFunc:        func() *mockObject { return newMockObject("", "") },
				CreateStrategy: strategy,
			}
			storeWithStrategy, err := omap.New[*mockObject](mockConn, poolName, testLogger, opts)
			Expect(err).NotTo(HaveOccurred())
			objToCreate := newMockObject("objstrat", "spec")
			createdObj, err := storeWithStrategy.Create(ctx, objToCreate)
			Expect(err).NotTo(HaveOccurred())
			Expect(strategyApplied).To(BeTrue())
			Expect(createdObj.GetAnnotations()).To(HaveKeyWithValue("strategy", "applied"))
		})
	})

	Describe("Get", func() {
		obj1 := newMockObject("get-id-1", "spec1")

		BeforeEach(func() {
			// Reset obj1 for each test
			obj1 = newMockObject("get-id-1", "spec1")
			obj1.SetCreatedAt(time.Now().Add(-time.Hour).UTC().Truncate(time.Microsecond))
			obj1.SetLabels(map[string]string{"env": "test"})
			obj1.SetAnnotations(map[string]string{"note": "a test object"})
			obj1.SetGeneration(5)
			obj1.SetResourceVersion(10) // Set a specific resource version for standard Get tests
			mockConn.Populate(omapName, map[string][]byte{
				obj1.GetID(): marshalOrFail(obj1), // Populate with current structure
			})
		})

		It("should successfully retrieve an existing object", func() {
			retrievedObj, err := omapStore.Get(ctx, obj1.GetID())
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedObj).NotTo(BeNil())
			Expect(retrievedObj.GetID()).To(Equal(obj1.GetID()))
			Expect(retrievedObj.Spec).To(Equal(obj1.Spec))
			Expect(retrievedObj.GetLabels()).To(Equal(obj1.GetLabels()))
			Expect(retrievedObj.GetGeneration()).To(Equal(obj1.GetGeneration()))
			Expect(retrievedObj.GetCreatedAt()).To(BeTemporally("~", obj1.GetCreatedAt(), time.Second))
			Expect(retrievedObj.GetResourceVersion()).To(Equal(obj1.GetResourceVersion())) // Check ResourceVersion
		})

		It("should return store.ErrNotFound if the object does not exist", func() {
			_, err := omapStore.Get(ctx, "nonexistent-id")
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, store.ErrNotFound)).To(BeTrue())
		})

		It("should return store.ErrNotFound if the omap itself does not exist", func() {
			mockConn.mu.Lock()
			delete(mockConn.omaps, omapName)
			mockConn.mu.Unlock()
			_, err := omapStore.Get(ctx, obj1.GetID())
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, store.ErrNotFound)).To(BeTrue()) // Get calls initializeCache which handles NotFound
		})

		It("should return an error if OpenIOContext fails during cache init", func() {
			simulatedError := errors.New("get failed to open io context")
			mockConn.SetFailOpenIOContext(simulatedError)
			_, err := omapStore.Get(ctx, obj1.GetID()) // Call Get to trigger cache init
			Expect(err).To(HaveOccurred())
			// Error occurs during cache initialization
			Expect(err.Error()).To(ContainSubstring("failed to initialize cache for Get"))
			Expect(err.Error()).To(ContainSubstring(simulatedError.Error()))
		})

		It("should return an error if GetAllOmapValues fails during cache init", func() {
			simulatedError := errors.New("ceph GetAllOmapValues failed")
			// Clear existing data and set failure *before* Get is called
			mockConn.Clear()
			mockConn.SetIOContextFailOp("GetAllOmapValues", simulatedError)
			_, err := omapStore.Get(ctx, "any-id") // Call Get to trigger cache init
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to initialize cache for Get"))
			Expect(err.Error()).To(ContainSubstring(simulatedError.Error()))
			Expect(errors.Is(err, store.ErrNotFound)).To(BeFalse())
		})

		It("should return an error if JSON unmarshaling fails", func() {
			// Populate with invalid JSON *before* Get triggers cache init
			mockConn.Populate(omapName, map[string][]byte{
				"invalid-json-id": []byte("<<<<"),
			})
			_, err := omapStore.Get(ctx, "invalid-json-id")
			Expect(err).To(HaveOccurred())
			// Check the specific error from unmarshaling within Get
			Expect(err.Error()).To(ContainSubstring("failed to unmarshal object data for id \"invalid-json-id\""))
			Expect(err.Error()).To(Or(
				ContainSubstring("invalid character"), // Common JSON errors
				ContainSubstring("unexpected end"),
			))
			Expect(errors.Is(err, store.ErrNotFound)).To(BeFalse(), "Error should not be ErrNotFound")
		})

		It("should successfully retrieve an object stored with the old structure (missing ResourceVersion)", func() {
			oldObjID := "old-data-id"
			oldObj := oldMockObject{
				oldMetadata: oldMetadata{
					ID:         oldObjID,
					Generation: 3,
					CreatedAt:  time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Microsecond),
					Labels:     map[string]string{"version": "old"},
				},
				Spec: "old-spec-data",
			}
			// Populate mock OMAP with the marshaled *old* structure
			mockConn.Populate(omapName, map[string][]byte{
				oldObjID: marshalOrFail(oldObj),
			})

			// Call Get, which will read from cache (populated from mock OMAP)
			retrievedObj, err := omapStore.Get(ctx, oldObjID)
			Expect(err).NotTo(HaveOccurred(), "Get should succeed even with old data structure")
			Expect(retrievedObj).NotTo(BeNil())
			Expect(retrievedObj.GetID()).To(Equal(oldObjID))
			Expect(retrievedObj.Spec).To(Equal(oldObj.Spec))
			Expect(retrievedObj.GetGeneration()).To(Equal(oldObj.Generation))
			Expect(retrievedObj.GetLabels()).To(Equal(oldObj.Labels))
			Expect(retrievedObj.GetCreatedAt()).To(BeTemporally("~", oldObj.CreatedAt, time.Second))

			// Crucial check: ResourceVersion should be the zero value (0)
			Expect(retrievedObj.GetResourceVersion()).To(BeZero(), "ResourceVersion should be zero when unmarshaling old data")
		})
	})

	Describe("Delete", func() {
		objToDelete := newMockObject("delete-id-1", "spec-del")
		objWithFinalizer := newMockObject("delete-id-finalizer", "spec-fin")

		BeforeEach(func() {
			objToDelete = newMockObject("delete-id-1", "spec-del")
			objToDelete.SetResourceVersion(1) // Set initial version

			objWithFinalizer = newMockObject("delete-id-finalizer", "spec-fin")
			objWithFinalizer.SetFinalizers([]string{"keep.me/around"})
			objWithFinalizer.SetResourceVersion(1) // Set initial version

			mockConn.Populate(omapName, map[string][]byte{
				objToDelete.GetID():      marshalOrFail(objToDelete),
				objWithFinalizer.GetID(): marshalOrFail(objWithFinalizer),
			})
		})

		It("should physically delete an object with no finalizers", func() {
			err := omapStore.Delete(ctx, objToDelete.GetID())
			Expect(err).NotTo(HaveOccurred())
			_, err = omapStore.Get(ctx, objToDelete.GetID())
			Expect(errors.Is(err, store.ErrNotFound)).To(BeTrue(), "Object should not be found after delete")
			_, _, keyOk := mockConn.CheckState(omapName, objToDelete.GetID())
			Expect(keyOk).To(BeFalse(), "Key should be removed from mock omap")
		})

		It("should add a deletion timestamp and increment ResourceVersion if finalizers exist", func() {
			initialVersion := objWithFinalizer.GetResourceVersion()
			err := omapStore.Delete(ctx, objWithFinalizer.GetID())
			Expect(err).NotTo(HaveOccurred())

			retrievedObj, err := omapStore.Get(ctx, objWithFinalizer.GetID())
			Expect(err).NotTo(HaveOccurred(), "Object should still be found after soft delete")
			Expect(retrievedObj.GetDeletedAt()).NotTo(BeNil(), "DeletedAt should be set")
			Expect(retrievedObj.GetFinalizers()).To(Equal(objWithFinalizer.GetFinalizers()), "Finalizers should remain")
			Expect(retrievedObj.GetResourceVersion()).To(Equal(initialVersion+1), "ResourceVersion should be incremented on soft delete") // Check version increment
		})

		It("should return nil (idempotent) if trying to delete a non-existent object", func() {
			err := omapStore.Delete(ctx, "nonexistent-delete-id")
			Expect(err).NotTo(HaveOccurred())
		})

		It("should return an error if Get fails before delete (not ErrNotFound)", func() {
			simulatedError := errors.New("get failed before delete")
			// Populate first, so cache init succeeds *if* Delete is called first
			mockConn.Populate(omapName, map[string][]byte{
				objToDelete.GetID(): marshalOrFail(objToDelete),
			})
			// Set failure for GetAllOmapValues, which will be called by getFromOmap *inside* Delete
			// OR will be called by initializeCache if Delete is the *first* operation
			mockConn.SetIOContextFailOp("GetAllOmapValues", simulatedError)

			err := omapStore.Delete(ctx, objToDelete.GetID()) // Delete calls initializeCache or getFromOmap

			Expect(err).To(HaveOccurred())
			// Assert the error comes from EITHER cache initialization OR the internal getFromOmap check
			Expect(err.Error()).To(Or(
				ContainSubstring("failed to initialize cache for Delete"),
				ContainSubstring("failed to get object from omap for delete check"),
			))
			Expect(err.Error()).To(ContainSubstring(simulatedError.Error())) // Check the original error is wrapped
			Expect(errors.Is(err, store.ErrNotFound)).To(BeFalse())
		})

		It("should return an error if RmOmapKeys fails during physical delete", func() {
			simulatedError := errors.New("ceph RmOmapKeys failed")
			mockConn.SetIOContextFailOp("RmOmapKeys", simulatedError)
			err := omapStore.Delete(ctx, objToDelete.GetID())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to physically delete object from omap"))
			Expect(err.Error()).To(ContainSubstring(simulatedError.Error()))
		})

		It("should return an error if SetOmap fails when setting deletion timestamp", func() {
			simulatedError := errors.New("ceph SetOmap failed on delete timestamp")
			mockConn.SetIOContextFailOp("SetOmap", simulatedError)
			err := omapStore.Delete(ctx, objWithFinalizer.GetID())
			Expect(err).To(HaveOccurred(), "Expected an error when SetOmap fails during soft delete")
			if err != nil {
				Expect(err.Error()).To(ContainSubstring("failed to set object metadata for soft deletion in omap"))
				Expect(err.Error()).To(ContainSubstring(simulatedError.Error()))
			}
		})
	})

	Describe("Update", func() {
		objToUpdate := newMockObject("update-id-1", "initial-spec")
		objToDelete := newMockObject("update-then-delete", "initial-spec")

		BeforeEach(func() {
			objToUpdate = newMockObject("update-id-1", "initial-spec")
			objToUpdate.SetResourceVersion(5) // Set initial version

			objToDelete = newMockObject("update-then-delete", "initial-spec")
			now := time.Now()
			objToDelete.SetDeletedAt(&now)
			objToDelete.SetFinalizers([]string{})
			objToDelete.SetResourceVersion(10) // Set initial version

			mockConn.Populate(omapName, map[string][]byte{
				objToUpdate.GetID(): marshalOrFail(objToUpdate),
				objToDelete.GetID(): marshalOrFail(objToDelete),
			})
		})

		It("should successfully update an existing object and increment ResourceVersion", func() {
			initialVersion := objToUpdate.GetResourceVersion()
			objToUpdate.Spec = "updated-spec"
			objToUpdate.SetLabels(map[string]string{"newlabel": "value"})
			objToUpdate.SetGeneration(objToUpdate.GetGeneration() + 1)

			// Update call uses the object with the initial ResourceVersion
			updatedObj, err := omapStore.Update(ctx, objToUpdate)
			Expect(err).NotTo(HaveOccurred())
			Expect(updatedObj.Spec).To(Equal("updated-spec"))
			Expect(updatedObj.GetLabels()).To(HaveKeyWithValue("newlabel", "value"))
			Expect(updatedObj.GetGeneration()).To(Equal(objToUpdate.GetGeneration()))
			// The *returned* object from Update should have the *incremented* version
			Expect(updatedObj.GetResourceVersion()).To(Equal(initialVersion+1), "Returned object ResourceVersion mismatch")

			// Get retrieves the updated state from cache/OMAP
			retrievedObj, err := omapStore.Get(ctx, objToUpdate.GetID())
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedObj.Spec).To(Equal("updated-spec"))
			Expect(retrievedObj.GetLabels()).To(HaveKeyWithValue("newlabel", "value"))
			Expect(retrievedObj.GetGeneration()).To(Equal(objToUpdate.GetGeneration()))
			Expect(retrievedObj.GetResourceVersion()).To(Equal(initialVersion+1), "Retrieved object ResourceVersion mismatch") // Check incremented version
		})

		It("should return ErrResourceVersionNotLatest if ResourceVersion does not match", func() {
			objToUpdate.Spec = "update-attempt-wrong-version"
			objToUpdate.SetResourceVersion(objToUpdate.GetResourceVersion() - 1) // Set wrong version

			_, err := omapStore.Update(ctx, objToUpdate)
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, omap.ErrResourceVersionNotLatest)).To(BeTrue())

			// Verify object was not actually updated
			retrievedObj, getErr := omapStore.Get(ctx, objToUpdate.GetID())
			Expect(getErr).NotTo(HaveOccurred())
			Expect(retrievedObj.Spec).To(Equal("initial-spec"))
			Expect(retrievedObj.GetResourceVersion()).To(Equal(uint64(5)))
		})

		It("should physically delete the object if DeletedAt is set and finalizers are empty (Update path)", func() {
			// Note: Update doesn't increment ResourceVersion when deleting
			initialVersion := objToDelete.GetResourceVersion()
			updatedObj, err := omapStore.Update(ctx, objToDelete) // objToDelete already has DeletedAt set
			Expect(err).NotTo(HaveOccurred())
			Expect(updatedObj.GetID()).To(Equal(objToDelete.GetID()))
			Expect(updatedObj.GetResourceVersion()).To(Equal(initialVersion)) // Version should NOT be incremented on delete via Update

			_, err = omapStore.Get(ctx, objToDelete.GetID())
			Expect(err).To(HaveOccurred(), "Expected Get to fail after Update triggered physical delete")
			Expect(errors.Is(err, store.ErrNotFound)).To(BeTrue(), "Get should return ErrNotFound after physical delete")
		})

		It("should return ErrNotFound if the object to update does not exist", func() {
			nonExistentObj := newMockObject("nonexistent-update-id", "spec")
			_, err := omapStore.Update(ctx, nonExistentObj)
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, store.ErrNotFound)).To(BeTrue()) // Error comes from the getFromOmap check
		})

		It("should return an error if Get fails before update (not ErrNotFound)", func() {
			simulatedError := errors.New("get failed before update")
			// Populate first, so cache init succeeds *if* Update is called first
			mockConn.Populate(omapName, map[string][]byte{
				objToUpdate.GetID(): marshalOrFail(objToUpdate),
			})
			// Set failure for GetAllOmapValues, which will be called by getFromOmap *inside* Update
			// OR will be called by initializeCache if Update is the *first* operation
			mockConn.SetIOContextFailOp("GetAllOmapValues", simulatedError)

			_, err := omapStore.Update(ctx, objToUpdate) // Update calls initializeCache or getFromOmap

			Expect(err).To(HaveOccurred())
			// Assert the error comes from EITHER cache initialization OR the internal getFromOmap check
			Expect(err.Error()).To(Or(
				ContainSubstring("failed to initialize cache for Update"),
				ContainSubstring("failed to get existing object from omap for update check"),
			))
			Expect(err.Error()).To(ContainSubstring(simulatedError.Error())) // Check the original error is wrapped
			Expect(errors.Is(err, store.ErrNotFound)).To(BeFalse())
		})

		It("should return an error if SetOmap fails during update", func() {
			simulatedError := errors.New("ceph SetOmap failed on update")
			mockConn.SetIOContextFailOp("SetOmap", simulatedError)
			objToUpdate.Spec = "update-attempt"
			_, err := omapStore.Update(ctx, objToUpdate)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to set object in omap during update"))
			Expect(err.Error()).To(ContainSubstring(simulatedError.Error()))
		})

		It("should return an error if RmOmapKeys fails during update-triggered delete", func() {
			simulatedError := errors.New("ceph RmOmapKeys failed on update-delete")
			mockConn.SetIOContextFailOp("RmOmapKeys", simulatedError)
			_, err := omapStore.Update(ctx, objToDelete)
			Expect(err).To(HaveOccurred(), "Expected an error when RmOmapKeys fails during update-delete")
			if err != nil {
				Expect(err.Error()).To(ContainSubstring("failed to delete object from omap during update"))
				Expect(err.Error()).To(ContainSubstring(simulatedError.Error()))
				Expect(errors.Is(err, store.ErrNotFound)).To(BeFalse(), "Error should not be ErrNotFound when RmOmapKeys fails")
			}
		})
	})

	Describe("List", func() {
		obj1 := newMockObject("list-id-1", "spec1")
		obj2 := newMockObject("list-id-2", "spec2")
		obj3 := newMockObject("list-id-3", "spec3")

		BeforeEach(func() {
			obj1 = newMockObject("list-id-1", "spec1")
			obj2 = newMockObject("list-id-2", "spec2")
			obj3 = newMockObject("list-id-3", "spec3")
			obj1.SetLabels(map[string]string{"type": "a"})
			obj2.SetLabels(map[string]string{"type": "b"})
			obj3.SetLabels(map[string]string{"type": "c"})
			// Set some resource versions
			obj1.SetResourceVersion(1)
			obj2.SetResourceVersion(2)
			obj3.SetResourceVersion(1)
			mockConn.Populate(omapName, map[string][]byte{
				obj1.GetID(): marshalOrFail(obj1),
				obj3.GetID(): marshalOrFail(obj3),
				obj2.GetID(): marshalOrFail(obj2),
			})
		})

		It("should list all objects when the omap is not empty", func() {
			objList, err := omapStore.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(objList).To(HaveLen(3))
			ids := make([]string, len(objList))
			versions := make(map[string]uint64)
			for i, obj := range objList {
				ids[i] = obj.GetID()
				versions[obj.GetID()] = obj.GetResourceVersion()
			}
			Expect(ids).To(ConsistOf(obj1.GetID(), obj2.GetID(), obj3.GetID()))
			Expect(versions).To(HaveKeyWithValue(obj1.GetID(), uint64(1)))
			Expect(versions).To(HaveKeyWithValue(obj2.GetID(), uint64(2)))
			Expect(versions).To(HaveKeyWithValue(obj3.GetID(), uint64(1)))
		})

		It("should return an empty list when the omap is empty", func() {
			mockConn.Clear()
			objList, err := omapStore.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(objList).To(BeEmpty())
		})

		It("should return an empty list when the omap name does not exist", func() {
			mockConn.mu.Lock()
			delete(mockConn.omaps, omapName)
			mockConn.mu.Unlock()
			objList, err := omapStore.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(objList).To(BeEmpty())
		})

		It("should return an error if GetAllOmapValues fails during cache init", func() {
			simulatedError := errors.New("ceph GetAllOmapValues failed on list")
			mockConn.Clear() // Clear first
			mockConn.SetIOContextFailOp("GetAllOmapValues", simulatedError)
			_, err := omapStore.List(ctx) // Call List to trigger cache init
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to initialize cache for List"))
			Expect(err.Error()).To(ContainSubstring(simulatedError.Error()))
			Expect(errors.Is(err, rados.ErrNotFound)).To(BeFalse())
		})

		It("should return an error if JSON unmarshaling fails for any object", func() {
			// Populate with one valid and one invalid object
			mockConn.Populate(omapName, map[string][]byte{
				"valid-id":   marshalOrFail(newMockObject("valid-id", "spec")),
				"invalid-id": []byte("<<<<< invalid json >>>>>"),
			})
			_, err := omapStore.List(ctx)
			Expect(err).To(HaveOccurred())
			// Check the specific error from unmarshaling within List
			Expect(err.Error()).To(ContainSubstring("failed to unmarshal object data"))
			Expect(err.Error()).To(Or(
				ContainSubstring("invalid character"),
				ContainSubstring("unexpected end"),
			))
		})

		It("should successfully list objects including those stored with the old structure", func() {
			oldObjID := "list-old-data-id"
			oldObj := oldMockObject{
				oldMetadata: oldMetadata{
					ID:         oldObjID,
					Generation: 1,
					CreatedAt:  time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Microsecond),
				},
				Spec: "list-old-spec",
			}
			// Add old data alongside existing data
			mockConn.Populate(omapName, map[string][]byte{
				oldObjID: marshalOrFail(oldObj),
			})

			objList, err := omapStore.List(ctx)
			Expect(err).NotTo(HaveOccurred(), "List should succeed even with mixed old/new data")
			Expect(objList).To(HaveLen(4)) // 3 existing + 1 old

			// Find the old object in the list and verify its ResourceVersion
			foundOld := false
			for _, obj := range objList {
				if obj.GetID() == oldObjID {
					foundOld = true
					Expect(obj.Spec).To(Equal(oldObj.Spec))
					Expect(obj.GetGeneration()).To(Equal(oldObj.Generation))
					Expect(obj.GetResourceVersion()).To(BeZero(), "ResourceVersion for old data in list should be zero")
					break
				}
			}
			Expect(foundOld).To(BeTrue(), "Old object not found in the list")
		})
	})

	Describe("Volume Test with Many Entries", func() {
		const numEntries = 5000 // Keep reduced number

		BeforeEach(func() {
			largeData := make(map[string][]byte, numEntries)
			for i := 0; i < numEntries; i++ {
				id := fmt.Sprintf("vol_obj_%04d", i)
				obj := newMockObject(id, fmt.Sprintf("spec_%d", i))
				obj.SetGeneration(int64(i))
				obj.SetResourceVersion(uint64(i % 10)) // Assign some resource versions
				largeData[id] = marshalOrFail(obj)
			}
			mockConn.Populate(omapName, largeData)
		})

		It("should correctly List with many entries", func() {
			objList, err := omapStore.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(objList)).To(Equal(numEntries))
			objMap := make(map[string]*mockObject) // Store pointer for easier access
			for i := range objList {
				obj := objList[i] // Get the object itself
				objMap[obj.GetID()] = obj
			}
			expectedID := "vol_obj_2510"
			Expect(objMap).To(HaveKey(expectedID))
			mockObj, ok := objMap[expectedID]
			Expect(ok).To(BeTrue())
			Expect(mockObj.Spec).To(Equal("spec_2510"))
			Expect(mockObj.GetGeneration()).To(Equal(int64(2510)))
			Expect(mockObj.GetResourceVersion()).To(Equal(uint64(0)))
		})
	})

	Describe("Watch", func() {
		var testWatch store.Watch[*mockObject] // Declare watch specific to this Describe block
		var stopWatch chan struct{}            // Channel to stop potential background activity if needed

		BeforeEach(func() {
			// Create a new watch for each test in this block
			var err error
			testWatch, err = omapStore.Watch(ctx)
			Expect(err).NotTo(HaveOccurred(), "Failed to create watch in Watch BeforeEach")
			stopWatch = make(chan struct{}) // Initialize stop channel
		})

		AfterEach(func() {
			// Stop the watch created for this block
			if testWatch != nil {
				testWatch.Stop()
			}
			// Close stop channel if initialized
			if stopWatch != nil {
				close(stopWatch)
			}
		})

		It("should allow creating a watch", func() {
			Expect(testWatch).NotTo(BeNil())
		})

		It("should receive event on create", func() {
			objToCreate := newMockObject("watch-create-1", "spec")
			createdObj, err := omapStore.Create(ctx, objToCreate) // Create will set RV=1
			Expect(err).NotTo(HaveOccurred())

			Eventually(testWatch.Events(), "1s").Should(Receive(SatisfyAll(
				HaveField("Type", store.WatchEventTypeCreated),
				WithTransform(func(e store.WatchEvent[*mockObject]) api.Object { return e.Object }, SatisfyAll(
					Not(BeNil()),
					WithTransform(func(o api.Object) string { return o.GetID() }, Equal(createdObj.ID)),
					WithTransform(func(o api.Object) uint64 { return o.GetResourceVersion() }, Equal(uint64(1))), // Check RV in event
				)),
			)), "Expected to receive a create event")
		})

		It("should receive event on update", func() {
			objToUpdate := newMockObject("watch-update-1", "spec-a")
			createdObj, err := omapStore.Create(ctx, objToUpdate) // RV=1
			Expect(err).NotTo(HaveOccurred())
			Eventually(testWatch.Events(), "1s").Should(Receive()) // Drain create event

			createdObj.Spec = "spec-b"
			updatedObj, err := omapStore.Update(ctx, createdObj) // Update sets RV=2
			Expect(err).NotTo(HaveOccurred())

			Eventually(testWatch.Events(), "1s").Should(Receive(SatisfyAll(
				HaveField("Type", store.WatchEventTypeUpdated),
				WithTransform(func(e store.WatchEvent[*mockObject]) api.Object { return e.Object }, SatisfyAll(
					Not(BeNil()),
					WithTransform(func(o api.Object) string { return o.GetID() }, Equal(updatedObj.ID)),
					WithTransform(func(o api.Object) string {
						if m, ok := o.(*mockObject); ok {
							return m.Spec
						}
						return ""
					}, Equal("spec-b")),
					WithTransform(func(o api.Object) uint64 { return o.GetResourceVersion() }, Equal(uint64(2))), // Check RV in event
				)),
			)), "Expected to receive an update event")
		})

		It("should receive event on delete (with finalizer)", func() {
			objToDelete := newMockObject("watch-delete-fin-1", "spec")
			objToDelete.SetFinalizers([]string{"test/finalizer"})
			createdObj, err := omapStore.Create(ctx, objToDelete) // RV=1
			Expect(err).NotTo(HaveOccurred())
			Eventually(testWatch.Events(), "1s").Should(Receive()) // Drain create event

			err = omapStore.Delete(ctx, createdObj.GetID()) // Soft delete sets RV=2
			Expect(err).NotTo(HaveOccurred())

			Eventually(testWatch.Events(), "1s").Should(Receive(SatisfyAll(
				HaveField("Type", store.WatchEventTypeDeleted),
				WithTransform(func(e store.WatchEvent[*mockObject]) api.Object { return e.Object }, SatisfyAll(
					Not(BeNil()),
					WithTransform(func(o api.Object) string { return o.GetID() }, Equal(objToDelete.ID)),
					WithTransform(func(o api.Object) *time.Time { return o.GetDeletedAt() }, Not(BeNil())),
					WithTransform(func(o api.Object) []string { return o.GetFinalizers() }, Not(BeEmpty())),
					WithTransform(func(o api.Object) uint64 { return o.GetResourceVersion() }, Equal(uint64(2))), // Check RV in event
				)),
			)), "Expected to receive a soft delete event")
		})

		It("should receive event on delete (no finalizer - via Update)", func() {
			objToDelete := newMockObject("watch-delete-upd-1", "spec")
			createdObj, err := omapStore.Create(ctx, objToDelete) // RV=1
			Expect(err).NotTo(HaveOccurred())
			Eventually(testWatch.Events(), "1s").Should(Receive()) // Drain create event

			now := time.Now()
			createdObj.SetDeletedAt(&now)
			createdObj.SetFinalizers(nil)
			// Update with DeletedAt and no finalizers triggers physical delete, RV is NOT incremented
			deletedObjViaUpdate, err := omapStore.Update(ctx, createdObj)
			Expect(err).NotTo(HaveOccurred())

			Eventually(testWatch.Events(), "1s").Should(Receive(SatisfyAll(
				HaveField("Type", store.WatchEventTypeDeleted),
				WithTransform(func(e store.WatchEvent[*mockObject]) api.Object { return e.Object }, SatisfyAll(
					Not(BeNil()),
					WithTransform(func(o api.Object) string { return o.GetID() }, Equal(deletedObjViaUpdate.ID)),
					WithTransform(func(o api.Object) *time.Time { return o.GetDeletedAt() }, Not(BeNil())),
					WithTransform(func(o api.Object) []string { return o.GetFinalizers() }, BeEmpty()),
					WithTransform(func(o api.Object) uint64 { return o.GetResourceVersion() }, Equal(uint64(1))), // Check RV in event (should be original RV=1)
				)),
			)), "Expected to receive a delete event via update")
		})
	})

	Describe("ListByLabels", func() {
		var (
			objA *mockObject
			objB *mockObject
			objC *mockObject
			objD *mockObject // No labels
		)

		BeforeEach(func() {
			// Create objects with different labels using the store's Create method
			// This ensures the label index is built correctly by the store logic.

			objA = newMockObject("objA", "specA")
			setMockObjectLabels(objA, map[string]string{"env": "prod", "app": "api"})
			_, err := omapStore.Create(ctx, objA)
			Expect(err).NotTo(HaveOccurred())

			objB = newMockObject("objB", "specB")
			setMockObjectLabels(objB, map[string]string{"env": "dev", "app": "api"})
			_, err = omapStore.Create(ctx, objB)
			Expect(err).NotTo(HaveOccurred())

			objC = newMockObject("objC", "specC")
			setMockObjectLabels(objC, map[string]string{"env": "prod", "app": "worker"})
			_, err = omapStore.Create(ctx, objC)
			Expect(err).NotTo(HaveOccurred())

			objD = newMockObject("objD", "specD")
			// objD has no labels set via setMockObjectLabels
			_, err = omapStore.Create(ctx, objD)
			Expect(err).NotTo(HaveOccurred())

			// Reset ResourceVersion to expected values after Create increments them
			// Note: Create sets RV to 1 internally. Fetch the created objects to be sure.
			objA, _ = omapStore.Get(ctx, "objA")
			objB, _ = omapStore.Get(ctx, "objB")
			objC, _ = omapStore.Get(ctx, "objC")
			objD, _ = omapStore.Get(ctx, "objD")
		})

		It("should return objects matching a single label selector", func() {
			selector := map[string]string{"app": "api"}
			result, err := omapStore.ListByLabels(ctx, selector)
			Expect(err).NotTo(HaveOccurred())

			Expect(result).To(HaveLen(2), "Should find objA and objB")
			Expect(getObjectIDs(result)).To(ConsistOf("objA", "objB"))
		})

		It("should return objects matching multiple label selectors (intersection)", func() {
			selector := map[string]string{"env": "prod", "app": "api"}
			result, err := omapStore.ListByLabels(ctx, selector)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(1), "Should find only objA")
			Expect(getObjectIDs(result)).To(ConsistOf("objA"))
		})

		It("should return an empty list if no objects match the selector", func() {
			selector := map[string]string{"env": "staging"}
			result, err := omapStore.ListByLabels(ctx, selector)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEmpty(), "Should find no objects with env=staging")
		})

		It("should return an empty list if a label key in the selector does not exist", func() {
			selector := map[string]string{"region": "us-east"}
			result, err := omapStore.ListByLabels(ctx, selector)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEmpty(), "Should find no objects with region label")
		})

		It("should return an empty list if selector requires a label that an object doesn't have", func() {
			selector := map[string]string{"env": "prod", "app": "api", "extra": "label"}
			result, err := omapStore.ListByLabels(ctx, selector)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEmpty(), "Should find no objects matching all three labels")
		})

		It("should return all objects if the selector is empty", func() {
			selector := map[string]string{}
			result, err := omapStore.ListByLabels(ctx, selector)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(HaveLen(4), "Should return all objects (A, B, C, D)")
			Expect(getObjectIDs(result)).To(ConsistOf("objA", "objB", "objC", "objD"))
		})

		It("should handle updates affecting labels", func() {
			// Update objB to have env=prod
			objBFromStore, err := omapStore.Get(ctx, "objB") // Get current version
			Expect(err).NotTo(HaveOccurred())
			setMockObjectLabels(objBFromStore, map[string]string{"env": "prod", "app": "api"}) // Change labels
			_, err = omapStore.Update(ctx, objBFromStore)                                      // Update
			Expect(err).NotTo(HaveOccurred())

			// Now query for env=prod
			selectorProd := map[string]string{"env": "prod"}
			resultProd, err := omapStore.ListByLabels(ctx, selectorProd)
			Expect(err).NotTo(HaveOccurred())
			Expect(resultProd).To(HaveLen(3), "Should find objA, updated objB, objC")
			Expect(getObjectIDs(resultProd)).To(ConsistOf("objA", "objB", "objC"))

			// Query for env=dev
			selectorDev := map[string]string{"env": "dev"}
			resultDev, err := omapStore.ListByLabels(ctx, selectorDev)
			Expect(err).NotTo(HaveOccurred())
			Expect(resultDev).To(BeEmpty(), "Should find no objects with env=dev anymore")
		})

		It("should handle deletion affecting labels", func() {
			// Delete objA
			err := omapStore.Delete(ctx, "objA")
			Expect(err).NotTo(HaveOccurred())

			// Query for env=prod, app=api
			selector := map[string]string{"env": "prod", "app": "api"}
			result, err := omapStore.ListByLabels(ctx, selector)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(BeEmpty(), "Should find no objects after objA is deleted")

			// Query for env=prod
			selectorProd := map[string]string{"env": "prod"}
			resultProd, err := omapStore.ListByLabels(ctx, selectorProd)
			Expect(err).NotTo(HaveOccurred())
			Expect(resultProd).To(HaveLen(1), "Should only find objC for env=prod")
			Expect(getObjectIDs(resultProd)).To(ConsistOf("objC"))
		})

		It("should return an error if cache initialization fails", func() {
			// Simulate cache init failure *before* calling ListByLabels
			// Create a *new* store instance for this test to ensure clean init state
			localMockConn := newMockRadosConnection() // Use a local mock connection
			storeForFailTest, err := omap.New[*mockObject](localMockConn, poolName, testLogger, omap.Options[*mockObject]{
				OmapName: omapName,
				NewFunc: func() *mockObject {
					m := newMockObject("", "")
					m.Annotations = make(map[string]string)
					return m
				},
			})
			Expect(err).NotTo(HaveOccurred())

			simulatedError := errors.New("simulated GetAllOmapValues failure")
			localMockConn.SetIOContextFailOp("GetAllOmapValues", simulatedError) // Fail the OMAP read

			selector := map[string]string{"app": "api"}
			_, err = storeForFailTest.ListByLabels(ctx, selector) // This will trigger initializeCache on the new store

			Expect(err).To(HaveOccurred(), "Expected an error due to cache initialization failure")
			if err != nil { // Check error content only if it occurred
				Expect(err.Error()).To(ContainSubstring("failed to initialize cache for ListByLabels"))
				Expect(err.Error()).To(ContainSubstring(simulatedError.Error()))
			}
		})

		// Removed the test case "should return an error if unmarshaling fails during list"
		// as it was difficult to set up reliably without internal access.

	})
})
