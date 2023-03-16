package handler

import (
	"context"
	"fmt"
	"github.com/pennsieve/packages-service/api/models"
	"github.com/pennsieve/packages-service/api/store"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"strings"
)

func (h *MessageHandler) handleFilePackage(ctx context.Context, orgId int, datasetId int64, restoreInfo models.RestorePackageInfo) error {
	err := h.Store.SQLFactory.ExecStoreTx(ctx, orgId, func(store store.SQLStore) error {
		var pkgsInFolder []pgdb.Package
		err := restorePackage(ctx, datasetId, restoreInfo, pkgsInFolder, store)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func restorePackage(ctx context.Context, datasetId int64, restoreInfo models.RestorePackageInfo, packagesInFolder []pgdb.Package, store store.SQLStore) error {
	originalName, err := getOriginalName(restoreInfo.Name, restoreInfo.NodeId)
	if err != nil {
		return err
	}
	getNewName(originalName, restoreInfo, packagesInFolder)
	return nil
}

func getOriginalName(deletedName, nodeId string) (string, error) {
	expectedPrefix := fmt.Sprintf("__%s__%s_", packageState.Deleted, nodeId)
	if !strings.HasPrefix(deletedName, expectedPrefix) {
		return "", fmt.Errorf("name: %s does not start with expected prefix: %s", deletedName, expectedPrefix)
	}
	return deletedName[len(expectedPrefix):], nil
}

func getNewName(desiredName string, restoreInfo models.RestorePackageInfo, packagesInFolder []pgdb.Package) string {
	newName := desiredName
	suffix := 0
	for _, p := range packagesInFolder {
		if p.Name == newName {
			suffix++
			newName = fmt.Sprintf("%s %d", desiredName, suffix)
		}
	}
	return newName
}
