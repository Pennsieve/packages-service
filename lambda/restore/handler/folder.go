package handler

import (
	"context"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	log "github.com/sirupsen/logrus"
)

func (h *MessageHandler) handleFolderPackage(ctx context.Context, orgId int, datasetId int64, restoreInfo models.RestorePackageInfo) error {
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
			if err := h.restoreName(ctx, a, sqlStore); err != nil {
				return h.errorf("error restoring name of ancestor %s of %s: %w", a.NodeId, restoreInfo.NodeId, err)
			}
		}
		// restore name
		err = h.restoreName(ctx, restoreInfo, sqlStore)
		if err != nil {
			return h.errorf("error restoring name of %s: %w", restoreInfo.NodeId, err)
		}

		var folderDescRestoreInfos []*models.RestorePackageInfo
		var nonFolderDescRestoreInfos []*models.RestorePackageInfo
		nonFolderNodeIdToId := map[string]int64{}
		nonFolderNodeIdToInfos := map[string]*models.RestorePackageInfo{}
		// restore descendant names
		for _, p := range restoring {
			sqlStore.LogInfoWithFields(log.Fields{"nodeId": p.NodeId, "state": p.PackageState}, "restoring descendant package name")
			descRestoreInfo := models.NewRestorePackageInfo(p)
			err = h.restoreName(ctx, descRestoreInfo, sqlStore)
			if err != nil {
				return h.errorf("error restoring descendant %s of %s: %w", p.NodeId, restoreInfo.NodeId, err)
			}
			if p.PackageType == packageType.Collection {
				folderDescRestoreInfos = append(folderDescRestoreInfos, &descRestoreInfo)
			} else {
				nonFolderDescRestoreInfos = append(nonFolderDescRestoreInfos, &descRestoreInfo)
				nonFolderNodeIdToId[descRestoreInfo.NodeId] = descRestoreInfo.Id
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
				h.LogInfo("fewer delete markers found than expected:", len(deleteMarkerResp), len(nonFolderDescRestoreInfos))
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
		// restore ancestor state
		for _, a := range ancestors {
			if err := h.restoreState(ctx, datasetId, a, sqlStore); err != nil {
				return h.errorf("error restoring state of %s, ancestor of %s: %w", a.NodeId, restoreInfo.NodeId, err)
			}
		}
		// restore descendant state
		for _, p := range folderDescRestoreInfos {
			sqlStore.LogInfoWithFields(log.Fields{"nodeId": p.NodeId, "type": p.Type}, "restoring descendant folder state")
			if err = h.restoreState(ctx, datasetId, *p, sqlStore); err != nil {
				return h.errorf("error restoring state of %s, folder descendant of %s: %w", p.NodeId, restoreInfo.NodeId, err)
			}
		}
		for _, p := range s3RestoredFileInfos {
			sqlStore.LogInfoWithFields(log.Fields{"nodeId": p.RestorePackageInfo.NodeId, "type": p.Type}, "restoring descendant non-folder state")
			if err = h.restoreState(ctx, datasetId, *p.RestorePackageInfo, sqlStore); err != nil {
				return h.errorf("error restoring state of %s, non-folder descendant of %s: %w", p.RestorePackageInfo.NodeId, restoreInfo.NodeId, err)
			}
		}
		// restore own state
		if err = h.restoreState(ctx, datasetId, restoreInfo, sqlStore); err != nil {
			return h.errorf("error restoring state of %s: %w", restoreInfo.NodeId, err)
		}
		return nil
	})
	return err
}
