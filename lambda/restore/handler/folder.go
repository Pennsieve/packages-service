package handler

import (
	"context"
	"fmt"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
)

func (h *MessageHandler) handleFolderPackage(ctx context.Context, orgId int, datasetId int64, restoreInfo models.RestorePackageInfo) error {
	err := h.Store.SQLFactory.ExecStoreTx(ctx, orgId, func(sqlStore store.SQLStore) error {
		// gather descendants and set to RESTORING
		restoring, err := sqlStore.TransitionDescendantPackageState(ctx, datasetId, restoreInfo.Id, packageState.Deleted, packageState.Deleted)
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
		// restore descendant names
		for _, p := range restoring {
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
			}
		}

		var s3RestoredPackageIds []int64

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
			}

			if deleteResponse, err := h.Store.Object.DeleteObjectsVersion(ctx, objectInfos...); err != nil {
				return fmt.Errorf("error restoring S3 objects: %w", err)
			} else if len(deleteResponse.AWSErrors) > 0 {

			}
			//TODO undelete in S3 and remove DeleteRecords from DynamoDB and populate s3RestoredPackageIds from the S3 Undelete response
		}

		// restore dataset_storage
		restoredSize := restoreInfo.Size
		sizeByPackage, err := sqlStore.GetPackageSizes(ctx, s3RestoredPackageIds...)
		if err != nil {
			return err
		}
		for _, size := range sizeByPackage {
			restoredSize += size
		}
		sqlStore.LogInfo("restored size ", restoredSize)
		if err = sqlStore.IncrementDatasetStorage(ctx, datasetId, restoredSize); err != nil {

		}

		// restore descendant state
		for _, p := range folderDescRestoreInfos {
			if err = h.restoreState(ctx, datasetId, *p, sqlStore); err != nil {
				return err
			}
		}
		for _, p := range nonFolderDescRestoreInfos {
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
