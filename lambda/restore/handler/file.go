package handler

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	log "github.com/sirupsen/logrus"
	"strings"
)

var savepointReplacer = strings.NewReplacer(":", "", "-", "")

func (h *MessageHandler) handleFilePackage(ctx context.Context, orgId int, datasetId int64, restoreInfo models.RestorePackageInfo) error {
	err := h.Store.SQLFactory.ExecStoreTx(ctx, orgId, func(sqlStore store.SQLStore) error {
		// mark any deleted ancestors as restoring
		var ancestors []models.RestorePackageInfo
		if restoreInfo.ParentId != nil {
			if a, err := sqlStore.TransitionAncestorPackageState(ctx, *restoreInfo.ParentId, packageState.Deleted, packageState.Restoring); err != nil {
				return h.errorf("error updating ancestors of %s to %s: %w", restoreInfo.NodeId, packageState.Restoring, err)
			} else {
				for _, p := range a {
					ancestors = append(ancestors, models.NewRestorePackageInfo(p))
				}
			}
		}
		// restore ancestors names
		for _, a := range ancestors {
			if err := h.restoreName(ctx, a, sqlStore); err != nil {
				return h.errorf("error restoring name of ancestor %s of %s: %w", a.NodeId, restoreInfo.NodeId, err)
			}
		}
		// restore name
		if err := h.restoreName(ctx, restoreInfo, sqlStore); err != nil {
			return h.errorf("error restoring name of %s: %w", restoreInfo.NodeId, err)
		}

		// restore S3 and clean up DynamoDB
		deleteMarkerResp, err := h.Store.NoSQL.GetDeleteMarkerVersions(ctx, &restoreInfo)
		if err != nil {
			return h.errorf("error getting delete record of %s: %w", restoreInfo.NodeId, err)
		}
		deleteMarker, ok := deleteMarkerResp[restoreInfo.NodeId]
		if !ok {
			return h.errorf("no delete record found for %v", restoreInfo)
		}
		sqlStore.LogInfoWithFields(log.Fields{"nodeId": restoreInfo.NodeId, "deleteMarker": *deleteMarker}, "delete marker found")
		if deleteResponse, err := h.Store.Object.DeleteObjectsVersion(ctx, *deleteMarker); err != nil {
			return h.errorf("error restoring S3 object %s: %w", *deleteMarker, err)
		} else if len(deleteResponse.AWSErrors) > 0 {
			sqlStore.LogErrorWithFields(log.Fields{"nodeId": restoreInfo.NodeId, "s3Info": *deleteMarker}, "AWS error during S3 restore", deleteResponse.AWSErrors)
			return h.errorf("AWS error restoring S3 object %s: %v", *deleteMarker, deleteResponse.AWSErrors[0])
		}
		if err = h.Store.NoSQL.RemoveDeleteRecords(ctx, []*models.RestorePackageInfo{&restoreInfo}); err != nil {
			// Don't think this should cause the whole restore to fail
			sqlStore.LogErrorWithFields(log.Fields{"nodeId": restoreInfo.NodeId, "error": err}, "error removing delete record")
		}

		// restore dataset storage
		restoredSize := h.parseSize(deleteMarker)
		sqlStore.LogInfo("restored size: ", restoredSize)
		if err = h.restoreStorage(ctx, int64(orgId), datasetId, restoreInfo, restoredSize, sqlStore); err != nil {
			// Don't think this should fail the whole restore
			sqlStore.LogErrorWithFields(log.Fields{"nodeId": restoreInfo.NodeId, "error": err}, "could not update storage")
		}

		// restore states
		stateRestores := make([]*models.RestorePackageInfo, len(ancestors)+1)
		stateRestores[0] = &restoreInfo
		for i, a := range ancestors {
			stateRestores[i+1] = &a
		}
		if err = h.restoreStates(ctx, datasetId, stateRestores, sqlStore); err != nil {
			return err
		}
		return nil
	})
	return err
}

func (h *MessageHandler) restoreName(ctx context.Context, restoreInfo models.RestorePackageInfo, store store.SQLStore) error {
	originalName, err := GetOriginalName(restoreInfo.Name, restoreInfo.NodeId)
	if err != nil {
		return err
	}
	savepoint := fmt.Sprintf("%s_svpt", savepointReplacer.Replace(restoreInfo.NodeId))
	if err = store.NewSavepoint(ctx, savepoint); err != nil {
		return err
	}
	var retryCtx *RetryContex
	err = store.UpdatePackageName(ctx, restoreInfo.Id, originalName)
	for retryCtx = NewRetryContext(originalName, err); retryCtx.TryAgain; retryCtx.Update(err) {
		newName := retryCtx.Parts.Next()
		h.LogDebugWithFields(log.Fields{"previousError": retryCtx.Err, "newName": newName}, "retrying name update")
		if spErr := store.RollbackToSavepoint(ctx, savepoint); spErr != nil {
			return spErr
		}
		err = store.UpdatePackageName(ctx, restoreInfo.Id, newName)
		h.LogDebugWithFields(log.Fields{"error": err, "newName": newName}, "retried name update")
	}
	if err = store.ReleaseSavepoint(ctx, savepoint); err != nil {
		return err
	}
	return retryCtx.Err
}

func (h *MessageHandler) restoreState(ctx context.Context, datasetId int64, restoreInfo models.RestorePackageInfo, store store.SQLStore) error {
	finalState := packageState.Uploaded
	if restoreInfo.Type == packageType.Collection {
		finalState = packageState.Ready
	}
	_, err := store.TransitionPackageState(ctx, datasetId, restoreInfo.NodeId, packageState.Restoring, finalState)
	if err != nil {
		return fmt.Errorf("error restoring state of %s to %s: %w", restoreInfo.NodeId, finalState, err)
	}
	return nil
}

func (h *MessageHandler) restoreStates(ctx context.Context, datasetId int64, restoreInfos []*models.RestorePackageInfo, sqlStore store.SQLStore) error {
	if len(restoreInfos) == 0 {
		return nil
	}
	transitions := make([]store.PackageStateTransition, len(restoreInfos))
	for i, r := range restoreInfos {
		finalState := packageState.Uploaded
		if r.Type == packageType.Collection {
			finalState = packageState.Ready
		}
		transitions[i] = store.PackageStateTransition{NodeId: r.NodeId, Expected: packageState.Restoring, Target: finalState}
	}
	_, err := sqlStore.TransitionPackageStateBulk(ctx, datasetId, transitions)
	if err != nil {
		return h.errorf("error restoring states: %w", err)
	}
	return nil
}

func (h *MessageHandler) restoreStorage(ctx context.Context, organizationId, datasetId int64, restoreInfo models.RestorePackageInfo, restoredSize int64, store store.SQLStore) error {
	if err := store.IncrementPackageStorage(ctx, restoreInfo.Id, restoredSize); err != nil {
		return fmt.Errorf("error incrementing package_storage for package %d by %d: %w", restoreInfo.Id, restoredSize, err)
	}
	if parentId := restoreInfo.ParentId; parentId != nil {
		if err := store.IncrementPackageStorageAncestors(ctx, *parentId, restoredSize); err != nil {
			return fmt.Errorf("error incrementing package_storage for ancestors of package %d by %d: %w", restoreInfo.Id, restoredSize, err)
		}
	}
	if err := store.IncrementDatasetStorage(ctx, datasetId, restoredSize); err != nil {
		return fmt.Errorf("error incrementing dataset_storage for dataset %d by %d: %w", datasetId, restoredSize, err)
	}
	if err := store.IncrementOrganizationStorage(ctx, organizationId, restoredSize); err != nil {
		return fmt.Errorf("error incrementing organization_storage for organization %d by %d: %w", organizationId, restoredSize, err)
	}
	return nil
}

func (h *MessageHandler) restoreStorages(ctx context.Context, organizationId, datasetId int64, fileInfos []RestoreFileInfo, store store.SQLStore) error {
	var totalSize int64
	sizeByParent := map[int64]int64{}
	for _, f := range fileInfos {
		size := h.parseSize(f.S3ObjectInfo)
		totalSize += size
		if f.ParentId != nil {
			sizeByParent[*f.ParentId] += size
		}
		if err := store.IncrementPackageStorage(ctx, f.Id, size); err != nil {
			return fmt.Errorf("error incrementing package_storage for package %d by %d: %w", f.Id, size, err)
		}
	}
	store.LogInfo("restored size: ", totalSize)
	for parentId, byParentSize := range sizeByParent {
		if err := store.IncrementPackageStorageAncestors(ctx, parentId, byParentSize); err != nil {
			return fmt.Errorf("error incrementing package_storage for package %d and ancestors by %d: %w", parentId, sizeByParent, err)
		}
	}
	if err := store.IncrementDatasetStorage(ctx, datasetId, totalSize); err != nil {
		return fmt.Errorf("error incrementing dataset_storage for dataset %d by %d: %w", datasetId, totalSize, err)
	}
	if err := store.IncrementOrganizationStorage(ctx, organizationId, totalSize); err != nil {
		return fmt.Errorf("error incrementing organization_storage for organization %d by %d: %w", organizationId, totalSize, err)
	}
	return nil
}

func (h *MessageHandler) parseSize(objInfo *store.S3ObjectInfo) int64 {
	size, err := objInfo.GetSize()
	if err != nil {
		h.LogErrorWithFields(log.Fields{"nodeId": objInfo.NodeId, "error": err}, "error parsing package size; using zero")
		size = 0
	}
	return size
}

type RetryContex struct {
	Parts    *NameParts
	Err      error
	TryAgain bool
}

func NewRetryContext(name string, err error) *RetryContex {
	retryCtx := &RetryContex{}
	if retryCtx = retryCtx.Update(err); retryCtx.TryAgain {
		retryCtx.Parts = NewNameParts(name)
	}
	return retryCtx
}

func (c *RetryContex) Update(err error) *RetryContex {
	if err != nil {
		if checkedError, ok := err.(models.PackageNameUniquenessError); ok {
			c.TryAgain = c.Parts == nil || c.Parts.More()
			c.Err = checkedError
		} else {
			c.TryAgain = false
			c.Err = err
		}
	} else {
		c.TryAgain = false
		c.Err = nil
	}
	return c
}

func GetOriginalName(deletedName, nodeId string) (string, error) {
	expectedPrefix := fmt.Sprintf("__%s__%s_", packageState.Deleted, nodeId)
	if !strings.HasPrefix(deletedName, expectedPrefix) {
		return "", fmt.Errorf("name: %s does not start with expected prefix: %s", deletedName, expectedPrefix)
	}
	return deletedName[len(expectedPrefix):], nil
}

type NameParts struct {
	Base  string
	Ext   string
	i     int
	limit int
	more  bool
}

func NewNameParts(name string) *NameParts {
	parts := NameParts{limit: 100, more: true}
	i := strings.LastIndexByte(name, '.')
	if i < 0 {
		parts.Base = name
		return &parts
	}
	parts.Base, parts.Ext = name[:i], name[i:]
	return &parts
}

func (p *NameParts) Next() string {
	p.i++
	if p.i < p.limit {
		return fmt.Sprintf("%s-restored_%d%s", p.Base, p.i, p.Ext)
	}
	p.more = false
	return fmt.Sprintf("%s-restored_%s%s", p.Base, uuid.NewString(), p.Ext)
}

func (p *NameParts) More() bool {
	return p.more
}

type RestoreFileInfo struct {
	*models.RestorePackageInfo
	*store.S3ObjectInfo
}

type RestoreFileInfos []RestoreFileInfo

func (fs RestoreFileInfos) AsPackageInfos() []*models.RestorePackageInfo {
	ps := make([]*models.RestorePackageInfo, len(fs))
	for i, f := range fs {
		ps[i] = f.RestorePackageInfo
	}
	return ps
}

func (fs RestoreFileInfos) AsS3ObjectInfos() []*store.S3ObjectInfo {
	os := make([]*store.S3ObjectInfo, len(fs))
	for i, f := range fs {
		os[i] = f.S3ObjectInfo
	}
	return os
}
