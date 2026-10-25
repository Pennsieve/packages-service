# The package formats registry, published where its $id says:
# schemas.<domain>/formats/v1.json, the platform's JSON schemas host
# (platform-infrastructure's schema bucket). It's the same formats.json the
# service embeds and serves at GET /packages/formats.
locals {
  formats_registry = jsondecode(file("${path.module}/../lambda/service/internal/formats_registry/formats.json"))
  formats_export = jsonencode(merge(
    { "$id" = "https://schemas.pennsieve.io/formats/v1.json" },
    local.formats_registry,
  ))
}

resource "aws_s3_object" "formats_registry" {
  count = var.publish_schemas ? 1 : 0

  bucket        = data.terraform_remote_state.platform_infrastructure.outputs.schemas_bucket_name
  key           = "formats/v1.json"
  content       = local.formats_export
  content_type  = "application/json"
  cache_control = "public, max-age=300"
}

# Specifications of the folder structures Pennsieve defines (formats.json
# "structures"): every structures/<id>/v<version>.md in the registry,
# published at its spec link, schemas.<domain>/structures/<id>/v<version>.
# A new version is a new file. Plain text so a browser shows them.
locals {
  structures_dir  = "${path.module}/../lambda/service/internal/formats_registry/structures"
  structure_specs = fileset(local.structures_dir, "*/v*.md")
}

resource "aws_s3_object" "structure_specs" {
  for_each = var.publish_schemas ? local.structure_specs : toset([])

  bucket        = data.terraform_remote_state.platform_infrastructure.outputs.schemas_bucket_name
  key           = "structures/${trimsuffix(each.value, ".md")}"
  source        = "${local.structures_dir}/${each.value}"
  etag          = filemd5("${local.structures_dir}/${each.value}")
  content_type  = "text/plain; charset=utf-8"
  cache_control = "public, max-age=300"
}
