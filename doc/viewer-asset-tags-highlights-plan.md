# Plan: Package Tags & Dataset Highlights

Backend plan for the "Return of the Gallery v0" feature in packages-service.
Dataset editors pick files to highlight; the gallery shows those files' thumbnails,
in the editor's order, in the Pennsieve App and on Discover.

This replaces an earlier plan that tagged viewer assets directly.

## Schema

Migration `V20261116102914__create_package_tags.sql` (pennsieve-db-migrations#22, merged).
It lives in the per-org schema, next to `packages`.

```sql
CREATE TABLE package_tags (
    package_id INTEGER NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
    tag VARCHAR(255) NOT NULL,
    position INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (package_id, tag),
    CONSTRAINT package_tags_tag_normalized CHECK (tag <> '' AND tag = lower(btrim(tag)))
);
CREATE INDEX package_tags_tag_idx ON package_tags(tag);
```

## Why tag packages, not assets

- A package can have several viewer assets (e.g. a `thumb` and a `zarr`). One tag on
  the package covers all of them, so they can't drift apart, and the frontend doesn't
  have to group assets back into files.
- The table is generic. A new kind of tag is a new value, not a new table.
  The app uses the tag `highlight`.

## `position`

- Orders packages within one tag. The app picks the scope: for `highlight` it is
  per dataset (via `packages.dataset_id`).
- NULL means no order.
- On every reorder the service rewrites the dataset's positions to 1…N in one
  transaction.
- The DB does not enforce unique positions. The service owns that.

## Endpoints (planned)

All paths are under `/packages`. Same permissions as the existing asset endpoints
(writes need `EditFiles`).

| Method | Path | Does |
|---|---|---|
| `POST` | `/assets/tags` | Add a tag to packages. |
| `DELETE` | `/assets/tags` | Remove a tag from packages. |
| `PUT` | `/assets/highlights` | Set the dataset's highlights as one full ordered list. |
| `GET` | `/assets/highlights` | The dataset's highlights, in order, with their assets. |
| `GET` | `/discover/assets/highlights` | Public version. Adds `file_path` and `version` per highlight. |

Changes to the existing list endpoints (`GET /assets`, `GET /discover/assets`):

- New filters: `tag`, `asset_type`, `status`. They combine.
- Each list item gets `highlighted`.
- `scope=all` gets pagination.

Decided:

- Thumbnails are `asset_type = 'thumb'`.
- The authenticated side returns the package file name only, no folder path.
- Discover's `file_path` and `version` come from Discover `public_file_versions.s3_key`.
- Public highlights are live: read from the workspace tables, not frozen at publication.

## Notes for implementers (from the migration review)

**Normalize tags first.** Run `lower(btrim(tag))` in the service before insert.
Empty after that → 400. Don't let the CHECK constraint be the validator; it would
surface as a DB error, not a clean 400.

**Filter on `packages.state`.** Skip deleted and replaced packages. The cascade only
fires on hard delete, so tag rows stay on `DELETED` packages until then.

**Use a stable order.** A package can have several thumbnails, so `position` alone ties:

```sql
ORDER BY position NULLS LAST, va.created_at, va.id
```

**Discover highlights are live, not versioned.** Un-highlighting a file hides it on
Discover right away. Publication still decides which files may show at all (see open
questions).

Rough shape of the highlights read:

```sql
SELECT p.node_id, p.name, t.position, va.*
FROM "{org}".package_tags t
JOIN "{org}".packages p                ON p.id = t.package_id
JOIN "{org}".viewer_asset_packages vap ON vap.package_id = p.id
JOIN "{org}".viewer_assets va          ON va.id = vap.viewer_asset_id
WHERE t.tag = 'highlight'
  AND p.dataset_id = $1
  AND p.state NOT IN (/* deleted, replaced */)
  AND va.asset_type = 'thumb'
ORDER BY t.position NULLS LAST, va.created_at, va.id;
```

## Open questions

1. **Replaced files.** Does a highlight follow a file when it is replaced?
2. **Several thumbnails.** Show one or all thumbnails per package?
3. **Folders.** Can a folder be highlighted? If not, the service must block it;
   a CHECK can't see `packages.type`.
4. ~~**Discover publication rule.** Show a highlight if its file is in the latest
   version, or in any `PUBLISH_SUCCEEDED` version?~~ **Settled:** latest version
   only. A package counts as published only if it has a file in the dataset's
   newest `PUBLISH_SUCCEEDED` version. `GET /discover/assets` follows the same rule.
