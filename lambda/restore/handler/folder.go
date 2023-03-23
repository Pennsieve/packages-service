package handler

import (
	"context"
	"errors"
	"fmt"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
)

func (h *MessageHandler) handleFolderPackage(ctx context.Context, orgId int, datasetId int64, restoreInfo models.RestorePackageInfo) error {
	err := h.Store.SQLFactory.ExecStoreTx(ctx, orgId, func(store store.SQLStore) error {
		// gather descendants and set to RESTORING
		restoring, err := store.TransitionDescendantPackageState(ctx, datasetId, restoreInfo.Id, packageState.Deleted, packageState.Deleted)
		if err != nil {
			return fmt.Errorf("unable to set descendants of %s (%s) to RESTORING: %w", restoreInfo.Name, restoreInfo.NodeId, err)
		}

		// restore name
		err = h.restoreName(ctx, restoreInfo, store)
		if err != nil {
			return err
		}

		var folderDescRestoreInfos []*models.RestorePackageInfo
		var nonFolderDescRestoreInfos []*models.RestorePackageInfo
		var restoredSize int64
		if restoreInfo.Size != nil {
			restoredSize = *restoreInfo.Size
		}
		// restore descendant names
		for _, p := range restoring {
			descRestoreInfo := models.NewRestorePackageInfo(p)
			err = h.restoreName(ctx, descRestoreInfo, store)
			if err != nil {
				return fmt.Errorf("error restoring descendant %s of %s: %w", p.NodeId, restoreInfo.NodeId, err)
			}
			if p.PackageType == packageType.Collection {
				folderDescRestoreInfos = append(folderDescRestoreInfos, &descRestoreInfo)
			} else {
				nonFolderDescRestoreInfos = append(nonFolderDescRestoreInfos, &descRestoreInfo)
			}
			if p.Size.Valid {
				restoredSize += p.Size.Int64
			}
		}

		// restore S3 objects and clean up DynamoDB
		if len(nonFolderDescRestoreInfos) > 0 {
			deleteMarkerResp, err := h.Store.NoSQL.GetDeleteMarkerVersions(ctx, nonFolderDescRestoreInfos...)
			if err != nil {
				return err
			}
			if len(deleteMarkerResp) < len(nonFolderDescRestoreInfos) {
				h.LogInfo("fewer delete markers found than expected:", len(deleteMarkerResp), len(nonFolderDescRestoreInfos))
			}

			//TODO undelete in S3 and remove DeleteRecords from DynamoDB
		}

		//TODO update storage
		store.LogInfo("restored size ", restoredSize)

		// restore descendant state
		for _, p := range folderDescRestoreInfos {
			err = h.restoreState(ctx, datasetId, *p, store)
			if err != nil {
				return err
			}
		}
		for _, p := range nonFolderDescRestoreInfos {
			err = h.restoreState(ctx, datasetId, *p, store)
			if err != nil {
				return err
			}
		}
		// restore own state
		err = h.restoreState(ctx, datasetId, restoreInfo, store)
		if err != nil {
			return err
		}
		return errors.New("returning error to rollback Tx during development")
	})
	return err
}
