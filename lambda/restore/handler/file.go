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
	err := h.Store.SQLFactory.ExecStoreTx(ctx, orgId, func(store store.SQLStore) error {
		// restore name
		err := h.restoreName(ctx, restoreInfo, store)
		if err != nil {
			return err
		}
		// restore S3 and clean up DynamoDB
		deleteMarkerResp, err := h.Store.NoSQL.GetDeleteMarkerVersions(ctx, &restoreInfo)
		if err != nil {
			return err
		}
		//TODO undelete S3 and remove delete record from DynamoDB
		deleteMarker, ok := deleteMarkerResp[restoreInfo.NodeId]
		if !ok {
			h.LogWarnWithFields(log.Fields{"nodeId": restoreInfo.NodeId}, "no delete marker found")
		} else {
			h.LogInfoWithFields(log.Fields{"nodeId": restoreInfo.NodeId, "deleteMarker": *deleteMarker}, "delete marker found")
		}
		// restore dataset storage
		restoredSize := restoreInfo.Size
		store.LogInfo("restored size: ", restoredSize)
		// TODO restore dataset storage
		// restore state
		if err = h.restoreState(ctx, datasetId, restoreInfo, store); err != nil {
			return err
		}
		return errors.New("returning error to rollback Tx during development")
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
		h.LogInfoWithFields(log.Fields{"previousError": retryCtx.Err, "newName": newName}, "retrying name update")
		if spErr := store.RollbackToSavepoint(ctx, savepoint); spErr != nil {
			return spErr
		}
		err = store.UpdatePackageName(ctx, restoreInfo.Id, newName)
		h.LogInfoWithFields(log.Fields{"error": err, "newName": newName}, "retried name update")
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
