# Package formats registry

`formats.json` lists the file formats Pennsieve knows: each format's id, name,
description, media types, go-core file types, extensions and, where EDAM
defines one, its EDAM format id. The service serves it at
`GET /packages/formats`, and it's published at
https://schemas.pennsieve.io/formats/v1.json (`terraform/schemas.tf`).

## Media types

- **Use the official IANA media type where one exists:** `application/pdf`,
  `image/tiff`, `text/csv`.
- **Otherwise use `application/vnd.pennsieve.<id>`**, named after the format's
  `id`: `application/vnd.pennsieve.edf`.
- **Each format gets its own media type,** because apps and workflows match
  files by media type.
- Don't add new `x-` types. The existing ones are de facto types kept as they
  are, because changing a format's media type changes what stored packages say
  they are; they're allowlisted in `conventions_test.go`.

**Known exceptions**, allowlisted in `conventions_test.go`:

| Media type | Formats |
|---|---|
| `application/x-hdf5` | `hdf5`, `nwb` |
| `image/tiff` | `tiff`, `ome-tiff` |
| `application/octet-stream` | `generic-data`, `data` |
| `application/postscript` | `adobe-illustrator`, `eps` |

NWB should get its own type, `application/x-nwb`, in a follow-up. That needs
care for packages already stored as `application/x-hdf5`.

## EDAM

`edam` is optional: the format's EDAM id, short form (`format_3590`). Only set
it for an id verified against the EDAM ontology
(https://github.com/edamontology/edamontology, `releases/EDAM.tsv`), and not
marked obsolete. Many formats Pennsieve relies on have no EDAM id (EDF, BDF,
MEF, NWB, Persyst, TDMS, Open Ephys); leave those without one. App manifests
(app.yml) get their EDAM `formats` from here, through the media type.

## Folder formats and structures

- **`kind: directory`** marks a format that is a folder (OME-Zarr, TS-Zarr,
  MEF3). A package is still one file, its compressed form, listed in
  `extensions`. `folderExtensions` lists the folder's own endings (`.ome.zarr`,
  `.mefd`): how a folder of that format is recognised, e.g. among an app's
  outputs. An app says in app.yml whether it wants the folder or the stored
  file (`container`).
- **`structures`** lists folder layouts a whole input or output can follow,
  such as BIDS. They aren't file formats and have no media type. App
  manifests name them with `conformsTo: <id>` or `<id>@<version>` (the term
  RO-Crate and DCAT use). Each one has `id`, `name`, `spec` (a link to the
  specification) and optionally `description` and `validator`. Don't give a
  structure the id of a format.

## Adding a format

1. Add an entry to `formats.json`:
   - required: `id` (lowercase, dashes), `name`, `mediaTypes` (following the
     convention above);
   - optional: `description`, `fileTypes` (go-core `fileType` names),
     `extensions` (with the dot), `edam`, and for a folder format `kind:
     directory` with `folderExtensions`.
2. Run `go test ./internal/formats_registry/`. The tests check:
   - ids are unique and the required fields are set;
   - EDAM ids are well formed;
   - the media type convention;
   - every go-core file type has a format, and every listed file type exists
     in go-core, apart from the allowlist in `conventions_test.go`:
     - **legacy types** (BCI2000, BNI1, Blackrock, DirectoryViewerData,
       FSFast, StaticViewerData, WFDB): present on older packages (pennsieve-api's
       Scala `FileType`), never assigned on upload, since no extension maps to
       them. The registry keeps describing them.
     - **MAT:** added to go-core in pennsieve-go-core#63. Remove it from the
       allowlist once packages-service depends on that version.
     - **OMEZarr, TSZarr:** added in pennsieve-go-core#64. Remove them once
       packages-service depends on that version.
3. Open a PR. Reviewed by the packages-service owners.
