# Pennsieve Published Dataset (PPD), v1

Identifier: `https://schemas.pennsieve.io/structures/pennsieve-published-dataset/v1`
Registry id (app.yml `conformsTo`): `pennsieve-published-dataset`

A dataset as Pennsieve Discover publishes it (discover-publish). A workspace
dataset doesn't have this layout; only a published version does.

## Layout

```
manifest.json     metadata, and a list of every file (required)
readme.md         the dataset's description, when it has one
changelog.md      the version's changes, when it has one
banner.jpg        the dataset's banner image, when it has one
files/            the data: the dataset's folders and files, as published
```

- `files/` is the **content root**. The data under it may follow another
  structure, such as BIDS. An app.yml says so with a list, outermost first:
  `conformsTo: [pennsieve-published-dataset, bids]`.
- A folder format (OME-Zarr, MEF3) under `files/` is one entry in the
  manifest, stored as one compressed file.

## manifest.json

JSON-LD with a schema.org `Dataset` (`@context` `http://schema.org/`, `@type`
`Dataset`, `@id` the dataset's DOI link). Its fields include `name`,
`description`, `creator`, `contributors`, `sourceOrganization`, `keywords`,
`datePublished`, `license`, `version`, `pennsieveDatasetId`, `collections`,
`relatedPublications`, and `files`: every file with its path, name, size and
file type.

`pennsieveSchemaVersion` is the layout's version as published: `1`, `2`,
`3.0`, `4` and `5.0` exist; new publications are `5.0`. In app.yml,
`pennsieve-published-dataset@5.0` names one version.

A JSON Schema for manifest.json is planned.
