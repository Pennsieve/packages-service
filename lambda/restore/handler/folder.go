package handler

import (
	"context"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/changelog"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	log "github.com/sirupsen/logrus"
)

func (h *MessageHandler) handleFolderPackage(ctx context.Context, orgId int, datasetId int64, restoreInfo models.RestorePackageInfo) ([]changelog.PackageRestoreEvent, error) {
	var changelogEvents []changelog.PackageRestoreEvent
	err := h.Store.SQLFactory.ExecStoreTx(ctx, orgId, func(sqlStore store.SQLStore) error {
		// gather ancestors and set to RESTORING
		var ancestors []models.RestorePackageInfo
		if restoreInfo.ParentId != nil {
			if a, err := sqlStore.TransitionAncestorPackageState(ctx, *restoreInfo.ParentId, packageState.Deleted, packageState.Restoring); err != nil {
				return h.errorf("error updating ancestors of %s to RESTORING: %w", restoreInfo.NodeId, err)
			} else {
				for _, p := range a {
					ancestors = append(ancestors, models.NewRestorePackageInfo(p))
				}
			}
		}
		// gather descendants and set to RESTORING
		restoring, err := sqlStore.TransitionDescendantPackageState(ctx, datasetId, restoreInfo.Id, packageState.Deleted, packageState.Restoring)
		if err != nil {
			return h.errorf("unable to set descendants of %s (%s) to RESTORING: %w", restoreInfo.Name, restoreInfo.NodeId, err)
		}

		// restore ancestors names
		for _, a := range ancestors {
			if restoredName, err := h.restoreName(ctx, a, sqlStore); err != nil {
				return h.errorf("error restoring name of ancestor %s of %s: %w", a.NodeId, restoreInfo.NodeId, err)
			} else {
				changelogEvents = append(changelogEvents, changelog.PackageRestoreEvent{
					Id:           a.Id,
					Name:         restoredName.Value,
					OriginalName: restoredName.OriginalName,
					NodeId:       a.NodeId,
				})
			}
		}
		// restore name
		if restoredName, err := h.restoreName(ctx, restoreInfo, sqlStore); err != nil {
			return h.errorf("error restoring name of %s: %w", restoreInfo.NodeId, err)
		} else {
			changelogEvents = append(changelogEvents, changelog.PackageRestoreEvent{
				Id:           restoreInfo.Id,
				Name:         restoredName.Value,
				OriginalName: restoredName.OriginalName,
				NodeId:       restoreInfo.NodeId,
			})
		}

		var folderDescRestoreInfos []*models.RestorePackageInfo
		var nonFolderDescRestoreInfos []*models.RestorePackageInfo
		nonFolderNodeIdToInfos := map[string]*models.RestorePackageInfo{}
		// restore descendant names
		for _, p := range restoring {
			sqlStore.LogDebugWithFields(log.Fields{"nodeId": p.NodeId, "state": p.PackageState}, "restoring descendant package name")
			descRestoreInfo := models.NewRestorePackageInfo(p)
			if restoredName, err := h.restoreName(ctx, descRestoreInfo, sqlStore); err != nil {
				return h.errorf("error restoring descendant %s of %s: %w", p.NodeId, restoreInfo.NodeId, err)
			} else {
				changelogEvents = append(changelogEvents, changelog.PackageRestoreEvent{
					Id:           p.Id,
					Name:         restoredName.Value,
					OriginalName: restoredName.OriginalName,
					NodeId:       p.NodeId,
				})
			}
			if p.PackageType == packageType.Collection {
				folderDescRestoreInfos = append(folderDescRestoreInfos, &descRestoreInfo)
			} else {
				nonFolderDescRestoreInfos = append(nonFolderDescRestoreInfos, &descRestoreInfo)
				nonFolderNodeIdToInfos[descRestoreInfo.NodeId] = &descRestoreInfo
			}
		}

		var s3RestoredFileInfos RestoreFileInfos
		// restore S3 objects and clean up DynamoDB
		if len(nonFolderDescRestoreInfos) > 0 {
			deleteMarkerResp, err := h.Store.NoSQL.GetDeleteMarkerVersions(ctx, nonFolderDescRestoreInfos...)
			if err != nil {
				return h.errorf("error getting delete records for descendants %s: %w", restoreInfo.NodeId, err)
			}
			if len(deleteMarkerResp) < len(nonFolderDescRestoreInfos) {
				return h.errorf("fewer delete records found than expected: %d expected, %d found", len(nonFolderDescRestoreInfos), len(deleteMarkerResp))
			}
			var objectInfos []store.S3ObjectInfo
			for _, objectInfo := range deleteMarkerResp {
				objectInfos = append(objectInfos, *objectInfo)
			}

			if deleteResponse, err := h.Store.Object.DeleteObjectsVersion(ctx, objectInfos...); err != nil {
				return h.errorf("error restoring S3 objects: %w", err)
			} else if len(deleteResponse.AWSErrors) > 0 {
				sqlStore.LogError("AWS errors while restoring S3 objects", deleteResponse.AWSErrors)
				return h.errorf("AWS error restoring S3 objects: %v. More errors may appear in server logs", deleteResponse.AWSErrors[0])
			} else {
				deletedPackages := deleteResponse.Deleted
				for _, deletedPackage := range deletedPackages {
					s3RestoredFileInfos = append(s3RestoredFileInfos, RestoreFileInfo{
						RestorePackageInfo: nonFolderNodeIdToInfos[deletedPackage.NodeId],
						S3ObjectInfo:       deleteMarkerResp[deletedPackage.NodeId],
					})
				}
				if err = h.Store.NoSQL.RemoveDeleteRecords(ctx, s3RestoredFileInfos.AsPackageInfos()); err != nil {
					sqlStore.LogError("error removing delete records from DynamoDB", err)
				}
			}
		}

		// restore dataset_storage
		if err = h.restoreStorages(ctx, int64(orgId), datasetId, s3RestoredFileInfos, sqlStore); err != nil {
			sqlStore.LogErrorWithFields(log.Fields{"nodeId": restoreInfo.NodeId}, "error updating storage", err)
		}
		// restore states
		stateRestores := make([]*models.RestorePackageInfo, len(ancestors)+len(folderDescRestoreInfos)+len(s3RestoredFileInfos)+1)
		// add self
		stateRestores[0] = &restoreInfo
		i := 1
		// add ancestors
		for _, a := range ancestors {
			stateRestores[i] = &a
			i++
		}
		// add descendants
		for _, p := range folderDescRestoreInfos {
			stateRestores[i] = p
			i++
		}
		for _, p := range s3RestoredFileInfos {
			stateRestores[i] = p.RestorePackageInfo
			i++
		}
		if err = h.restoreStates(ctx, datasetId, stateRestores, sqlStore); err != nil {
			return err
		}
		return nil
	})
	return changelogEvents, err
}
