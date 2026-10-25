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

## Adding a format

1. Add an entry to `formats.json`:
   - required: `id` (lowercase, dashes), `name`, `mediaTypes` (following the
     convention above);
   - optional: `description`, `fileTypes` (go-core `fileType` names),
     `extensions` (with the dot), `edam`.
2. Run `go test ./internal/formats_registry/`. The tests check:
   - ids are unique and the required fields are set;
   - EDAM ids are well formed;
   - the media type convention;
   - every go-core file type has a format, and every listed file type exists
     in go-core.
3. Open a PR. Reviewed by the packages-service owners.
