package handler

import (
	"context"
	"fmt"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	log "github.com/sirupsen/logrus"
)

func (h *MessageHandler) handleFolderPackage(ctx context.Context, orgId int, datasetId int64, restoreInfo models.RestorePackageInfo) error {
	err := h.Store.SQLFactory.ExecStoreTx(ctx, orgId, func(sqlStore store.SQLStore) error {
		// gather descendants and set to RESTORING
		restoring, err := sqlStore.TransitionDescendantPackageState(ctx, datasetId, restoreInfo.Id, packageState.Deleted, packageState.Restoring)
		if err != nil {
			return fmt.Errorf("unable to set descendants of %s (%s) to RESTORING: %w", restoreInfo.Name, restoreInfo.NodeId, err)
		}

		// restore name
		err = h.restoreName(ctx, restoreInfo, sqlStore)
		if err != nil {
			return err
		}

		var folderDescRestoreInfos []*models.RestorePackageInfo
		var nonFolderDescRestoreInfos []*models.RestorePackageInfo
		nonFolderNodeIdToId := map[string]int64{}
		nonFolderNodeIdToInfos := map[string]*models.RestorePackageInfo{}
		// restore descendant names
		for _, p := range restoring {
			sqlStore.LogInfoWithFields(log.Fields{"nodeId": p.NodeId, "state": p.PackageState}, "restoring descendant package name")
			// using fake size here because we only want to look up
			// sizes of items we find a delete-record for below.
			descRestoreInfo := models.NewRestorePackageInfo(p, 0)
			err = h.restoreName(ctx, descRestoreInfo, sqlStore)
			if err != nil {
				return fmt.Errorf("error restoring descendant %s of %s: %w", p.NodeId, restoreInfo.NodeId, err)
			}
			if p.PackageType == packageType.Collection {
				folderDescRestoreInfos = append(folderDescRestoreInfos, &descRestoreInfo)
			} else {
				nonFolderDescRestoreInfos = append(nonFolderDescRestoreInfos, &descRestoreInfo)
				nonFolderNodeIdToId[descRestoreInfo.NodeId] = descRestoreInfo.Id
				nonFolderNodeIdToInfos[descRestoreInfo.NodeId] = &descRestoreInfo
			}
		}

		var s3RestoredPackageIds []int64
		var s3RestoredInfos []*models.RestorePackageInfo
		sizeByPackage := map[int64]int64{}
		// restore S3 objects and clean up DynamoDB
		if len(nonFolderDescRestoreInfos) > 0 {
			deleteMarkerResp, err := h.Store.NoSQL.GetDeleteMarkerVersions(ctx, nonFolderDescRestoreInfos...)
			if err != nil {
				return err
			}
			if len(deleteMarkerResp) < len(nonFolderDescRestoreInfos) {
				h.LogInfo("fewer delete markers found than expected:", len(deleteMarkerResp), len(nonFolderDescRestoreInfos))
			}
			var objectInfos []store.S3ObjectInfo
			for _, objectInfo := range deleteMarkerResp {
				objectInfos = append(objectInfos, *objectInfo)
				sizeByPackage[nonFolderNodeIdToId[objectInfo.NodeId]] = h.parseSize(objectInfo)
			}

			if deleteResponse, err := h.Store.Object.DeleteObjectsVersion(ctx, objectInfos...); err != nil {
				return fmt.Errorf("error restoring S3 objects: %w", err)
			} else if len(deleteResponse.AWSErrors) > 0 {
				sqlStore.LogError("AWS errors while restoring S3 objects", deleteResponse.AWSErrors)
				return fmt.Errorf("AWS error restoring S3 objects: %v. More errors may appear in server logs", deleteResponse.AWSErrors[0])
			} else {
				deletedPackages := deleteResponse.Deleted
				for _, deletedPackage := range deletedPackages {
					s3RestoredPackageIds = append(s3RestoredPackageIds, nonFolderNodeIdToId[deletedPackage.NodeId])
					s3RestoredInfos = append(s3RestoredInfos, nonFolderNodeIdToInfos[deletedPackage.NodeId])
				}
				if err = h.Store.NoSQL.RemoveDeleteRecords(ctx, s3RestoredInfos...); err != nil {
					sqlStore.LogError("error removing delete records from DynamoDB", err)
				}
			}
		}

		// restore dataset_storage
		restoredSize := int64(0)
		for _, size := range sizeByPackage {
			restoredSize += size
		}
		sqlStore.LogInfo("restored size ", restoredSize)
		if err = sqlStore.IncrementDatasetStorage(ctx, datasetId, restoredSize); err != nil {

		}

		// restore descendant state
		for _, p := range folderDescRestoreInfos {
			sqlStore.LogInfoWithFields(log.Fields{"nodeId": p.NodeId, "type": p.Type}, "restoring descendant folder state")
			if err = h.restoreState(ctx, datasetId, *p, sqlStore); err != nil {
				return err
			}
		}
		for _, p := range s3RestoredInfos {
			sqlStore.LogInfoWithFields(log.Fields{"nodeId": p.NodeId, "type": p.Type}, "restoring descendant non-folder state")
			if err = h.restoreState(ctx, datasetId, *p, sqlStore); err != nil {
				return err
			}
		}
		// restore own state
		if err = h.restoreState(ctx, datasetId, restoreInfo, sqlStore); err != nil {
			return err
		}
		return nil
	})
	return err
}
