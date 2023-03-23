package handler

import (
	"context"
	"errors"
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
	err := h.Store.SQLFactory.ExecStoreTx(ctx, orgId, h, func(store store.SQLStore) error {
		err := h.restorePackage(ctx, datasetId, restoreInfo, store)
		if err != nil {
			return err
		}
		deleteMarkerResp, err := h.Store.NoSQL.GetDeleteMarkerVersions(ctx, &restoreInfo)
		if err != nil {
			return err
		}
		deleteMarker, ok := deleteMarkerResp[restoreInfo.NodeId]
		if !ok {
			h.LogInfoWithFields(log.Fields{"nodeId": restoreInfo.NodeId}, "no delete marker found")
		}
		h.LogInfoWithFields(log.Fields{"nodeId": restoreInfo.NodeId, "deleteMarker": *deleteMarker}, "delete marker found")

		return errors.New("returning error to rollback Tx during development")
	})
	return err
}

func (h *MessageHandler) restorePackage(ctx context.Context, datasetId int64, restoreInfo models.RestorePackageInfo, store store.SQLStore) error {
	err := h.restoreName(ctx, restoreInfo, store)
	if err != nil {
		return err
	}
	finalState := packageState.Uploaded
	if restoreInfo.Type == packageType.Collection {
		finalState = packageState.Ready
	}
	_, err = store.TransitionPackageState(ctx, datasetId, restoreInfo.NodeId, packageState.Restoring, finalState)
	if err != nil {
		return err
	}
	return nil
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
	rowCount, err := store.UpdatePackageName(ctx, restoreInfo.Id, originalName)
	for retryCtx = NewRetryContext(originalName, rowCount, err); retryCtx.TryAgain; retryCtx.Update(rowCount, err) {
		newName := retryCtx.Parts.Next()
		h.LogInfoWithFields(log.Fields{"previousError": retryCtx.Err, "newName": newName}, "retrying name update")
		if spErr := store.RollbackToSavepoint(ctx, savepoint); spErr != nil {
			return spErr
		}
		rowCount, err = store.UpdatePackageName(ctx, restoreInfo.Id, newName)
		h.LogInfoWithFields(log.Fields{"updatedRowCount": rowCount, "error": err, "newName": newName}, "retried name update")
	}
	if err = store.ReleaseSavepoint(ctx, savepoint); err != nil {
		return err
	}
	return retryCtx.Err
}

type RetryContex struct {
	Parts    *NameParts
	Err      error
	TryAgain bool
}

func NewRetryContext(name string, rowCount int64, err error) *RetryContex {
	retryCtx := &RetryContex{}
	if retryCtx = retryCtx.Update(rowCount, err); retryCtx.TryAgain {
		retryCtx.Parts = NewNameParts(name)
	}
	return retryCtx
}

func (c *RetryContex) Update(rowCount int64, err error) *RetryContex {
	if err != nil {
		if checkedError, ok := err.(models.PackageNameUniquenessError); ok {
			c.TryAgain = c.Parts == nil || c.Parts.More()
			c.Err = checkedError
		} else {
			c.TryAgain = false
			c.Err = err
		}
	} else if rowCount == 0 {
		c.TryAgain = false
		c.Err = errors.New("package row not found during name update")
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
