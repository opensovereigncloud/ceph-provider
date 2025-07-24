# Snapshot Deletion Precedence and Handling of Timestamp Modifications

This document outlines the standard deletion procedures for snapshots, with a specific focus on how user-initiated deletion interacts with inactivity-based automatic cleanup, and the implications of unexpected changes to `LastPopulatedTime`.

-   **`LastPopulatedTime` - A Fixed Point of Reference:**
    The `snapshot.Status.LastPopulatedTime` is designed to be an immutable timestamp. It is set precisely once, marking the completion of the snapshot's initial population process and its readiness for use. Its constancy is critical for accurately calculating inactivity periods and ensuring predictable lifecycle management. Any change to this timestamp after its initial setting, outside of a re-population event that would logically reset it, is considered an anomaly or data corruption and is not part of the standard operational procedure.

-   **Inactivity-Based Deletion (`DeleteAt`):**
    The `DeleteAt` timestamp, which is derived from `LastPopulatedTime + SnapshotInactivityTimeout`, indicates when a snapshot becomes eligible for automatic cleanup if it is found to be unused (i.e., no dependent images or resources exist). This mechanism serves as a background garbage collector for orphaned snapshots.

-   **User-Initiated Deletion (`metadata.deletionTimestamp` - Overriding All):**
    When a user or a higher-level system explicitly requests the deletion of a snapshot (e.g., via `kubectl delete snapshot <name>`), Kubernetes sets the `metadata.deletionTimestamp` on the snapshot object. This `deletionTimestamp` acts as the **highest-priority command** for deletion. The controller will immediately prioritize and execute the explicit deletion logic (finalization), regardless of the `DeleteAt` timestamp for inactivity-based cleanup.

## Handling Unexpected `LastPopulatedTime` Changes (Deviation from Procedure)

While `LastPopulatedTime` is intended to be constant, if, due to a bug or manual tampering, this timestamp were to change after its initial set:

* The `DeleteAt` calculation would be impacted, potentially resetting the inactivity countdown or causing immediate eligibility if the new `LastPopulatedTime` pushes the `DeleteAt` into the past. This primarily disrupts the *automatic* inactivity-based garbage collection.
* **Crucially, the explicit deletion flow (triggered by `metadata.deletionTimestamp`) remains unaffected and always takes precedence.** An explicit deletion request will still lead to the immediate cleanup of the snapshot, overriding any potentially inconsistent `DeleteAt` calculation caused by a changed `LastPopulatedTime`.
