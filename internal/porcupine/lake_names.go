package porcupine

import (
	"github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/lake"
)

// Names from the analytical store package, kept under their historical
// identifiers while callers move to the lake API.

type (
	preparedLakeProjection      = lake.PreparedLakeProjection
	duckLakeMaintenanceStats    = lake.DuckLakeMaintenanceStats
	projectLake                 = lake.ProjectLake
	EventProfileProperty        = lake.EventProfileProperty
	EventProfile                = lake.EventProfile
	EventProfileInput           = lake.EventProfileInput
	AnalyticsResult             = lake.AnalyticsResult
	AnalyticsResultColumn       = lake.AnalyticsResultColumn
	DeliveryStatus              = lake.DeliveryStatus
	DuckLakeCleanupStatus       = lake.DuckLakeCleanupStatus
	DuckLakeConfig              = lake.DuckLakeConfig
	DuckLakeS3Config            = lake.DuckLakeS3Config
	Event                       = event.Event
	EventCatalog                = lake.EventCatalog
	EventCatalogInput           = lake.EventCatalogInput
	EventProfileActivity        = lake.EventProfileActivity
	NativeBackupConfig          = lake.NativeBackupConfig
	analyticalWindow            = lake.AnalyticalWindow
	backupFile                  = lake.BackupFile
	duckLakeObjectStore         = lake.DuckLakeObjectStore
	duckLakeStore               = lake.DuckLakeStore
	eventProfileOptions         = lake.EventProfileOptions
	eventProfileRead            = lake.EventProfileRead
	eventProfileRow             = lake.EventProfileRow
	eventProjection             = lake.EventProjection
	exportStreamCallbacks       = lake.ExportStreamCallbacks
	exportStreamResult          = lake.ExportStreamResult
	identityLinkSnapshot        = lake.IdentityLinkSnapshot
	identitySnapshot            = lake.IdentitySnapshot
	lakeStore                   = lake.LakeStore
	localNativeBackupReader     = lake.LocalNativeBackupReader
	nativeBackupCatalog         = lake.NativeBackupCatalog
	nativeBackupManifest        = lake.NativeBackupManifest
	nativeBackupReader          = lake.NativeBackupReader
	nativeBackupS3              = lake.NativeBackupS3
	nativeBackupStorageIdentity = lake.NativeBackupStorageIdentity
	nativeCheckpoint            = lake.NativeCheckpoint
	queryExecutionLimits        = lake.QueryExecutionLimits
)

var (
	defaultQueryExecutionLimits         = lake.DefaultQueryExecutionLimits
	pinDuckLakeStorage                  = lake.PinDuckLakeStorage
	prepareLakeProjection               = lake.PrepareLakeProjection
	validateSharedNativeConfig          = lake.ValidateSharedNativeConfig
	sharedNativeCatalogID               = lake.SharedNativeCatalogID
	sharedNativeSchema                  = lake.SharedNativeSchema
	claimLocalNativeBackupOwner         = lake.ClaimLocalNativeBackupOwner
	normalizeEventProfileInput          = lake.NormalizeEventProfileInput
	restoreNativeProject                = lake.RestoreNativeProject
	summarizeEventProfile               = lake.SummarizeEventProfile
	validStoredEventUUID                = lake.ValidStoredEventUUID
	backupS3Error                       = lake.BackupS3Error
	copyCheckpointFile                  = lake.CopyCheckpointFile
	duckLakeQueryError                  = lake.DuckLakeQueryError
	errEventNotFound                    = lake.ErrEventNotFound
	errExportMetadataTooLarge           = lake.ErrExportMetadataTooLarge
	errExportStreamInvalid              = lake.ErrExportStreamInvalid
	errExportValueTooLarge              = lake.ErrExportValueTooLarge
	eventWithMetadata                   = lake.EventWithMetadata
	hashBackupFile                      = lake.HashBackupFile
	lakeLiteral                         = lake.LakeLiteral
	lakePrefixSegmentPattern            = lake.LakePrefixSegmentPattern
	newDuckLake                         = lake.NewDuckLake
	newID                               = event.NewID
	newNativeBackupReader               = lake.NewNativeBackupReader
	newNativeBackupS3                   = lake.NewNativeBackupS3
	newNativeS3Client                   = lake.NewNativeS3Client
	normalizeCatalogInput               = lake.NormalizeCatalogInput
	normalizeDuckLakeCleanup            = lake.NormalizeDuckLakeCleanup
	normalizeDuckLakeS3                 = lake.NormalizeDuckLakeS3
	originalSnapshot                    = lake.OriginalSnapshot
	pruneNativeBackups                  = lake.PruneNativeBackups
	removeIncompleteNativeBackups       = lake.RemoveIncompleteNativeBackups
	snapshotSQLite                      = lake.SnapshotSQLite
	syncBackupDirectory                 = lake.SyncBackupDirectory
	validNativeBackupID                 = lake.ValidNativeBackupID
	validateBackupSQLite                = lake.ValidateBackupSQLite
	validateNativeBackupManifest        = lake.ValidateNativeBackupManifest
	validateNativeBackupStorageIdentity = lake.ValidateNativeBackupStorageIdentity
	verifyNativeBackup                  = lake.VerifyNativeBackup
	verifyNativeBackupPair              = lake.VerifyNativeBackupPair
	writeBackupManifest                 = lake.WriteBackupManifest
)

const (
	sourceEventUUIDPrefix   = lake.SourceEventUUIDPrefix
	analyticsMaxBytes       = lake.AnalyticsMaxBytes
	analyticsMaxRows        = lake.AnalyticsMaxRows
	defaultDuckLakeMemoryMB = lake.DefaultDuckLakeMemoryMB
	eventProfileFieldBytes  = lake.EventProfileFieldBytes
	eventProfileSampleBytes = lake.EventProfileSampleBytes
	eventProfileValueBytes  = lake.EventProfileValueBytes
	maxSQLBytes             = lake.MaxSQLBytes
	nativeBackupFileLimit   = lake.NativeBackupFileLimit
)
