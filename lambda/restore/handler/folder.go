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
		restoring, err := store.TransitionDescendantPackageState(ctx, datasetId, restoreInfo.Id, packageState.Deleted, packageState.Deleted)
		if err != nil {
			return fmt.Errorf("unable to set descendants of %s (%s) to RESTORING: %w", restoreInfo.Name, restoreInfo.NodeId, err)
		}
		var descRestoreInfos []*models.RestorePackageInfo
		// restore descendants
		for _, p := range restoring {
			descRestoreInfo := models.NewRestorePackageInfo(p)
			err = h.restorePackage(ctx, datasetId, descRestoreInfo, store)
			if err != nil {
				return fmt.Errorf("error restoring descendant %s of %s: %w", p.NodeId, restoreInfo.NodeId, err)
			}
			if p.PackageType != packageType.Collection {
				descRestoreInfos = append(descRestoreInfos, &descRestoreInfo)
			}
		}
		// restore self
		err = h.restorePackage(ctx, datasetId, restoreInfo, store)
		if err != nil {
			return err
		}
		if len(descRestoreInfos) > 0 {
			deleteMarkerResp, err := h.Store.NoSQL.GetDeleteMarkerVersions(ctx, descRestoreInfos...)
			if err != nil {
				return err
			}
			if len(deleteMarkerResp) < len(descRestoreInfos) {
				h.logInfo("fewer delete markers found than expected:", len(deleteMarkerResp), len(descRestoreInfos))
			}
		}
		return errors.New("returning error to rollback Tx during development")
	})
	return err
}
