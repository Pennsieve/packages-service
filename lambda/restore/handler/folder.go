package handler

import (
	"context"
	"fmt"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
)

func (h *MessageHandler) handleFolderPackage(ctx context.Context, orgId int, datasetId int64, restoreInfo models.RestorePackageInfo) error {
	err := h.Store.SQLFactory.ExecStoreTx(ctx, orgId, func(store store.SQLStore) error {
		restoring, err := store.TransitionDescendantPackageState(ctx, datasetId, restoreInfo.Id, packageState.Deleted, packageState.Restoring)
		if err != nil {
			return fmt.Errorf("unable to set descendants of %s (%s) to RESTORING: %w", restoreInfo.Name, restoreInfo.NodeId, err)
		}
		descRestoreInfos := make([]models.RestorePackageInfo, len(restoring))
		var rootPkgs []*pgdb.Package
		parentIdToPkg := map[int64][]*pgdb.Package{}
		for i, p := range restoring {
			descRestoreInfos[i] = models.NewRestorePackageInfo(&p)
			if p.ParentId.Valid {
				parentId := p.ParentId.Int64
				parentIdToPkg[parentId] = append(parentIdToPkg[parentId], &p)

			} else {
				rootPkgs = append(rootPkgs, &p)
			}
		}
		return nil
	})
	return err
}
