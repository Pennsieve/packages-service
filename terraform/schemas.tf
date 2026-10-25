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
# "structures"), published at their spec link, one per version (a Pennsieve
# Published Dataset's version is manifest.json's pennsieveSchemaVersion).
# Plain text so a browser shows them.
resource "aws_s3_object" "structure_pennsieve_published_dataset" {
  count = var.publish_schemas ? 1 : 0

  bucket        = data.terraform_remote_state.platform_infrastructure.outputs.schemas_bucket_name
  key           = "structures/pennsieve-published-dataset/v5.0"
  source        = "${path.module}/../lambda/service/internal/formats_registry/structures/pennsieve-published-dataset.md"
  etag          = filemd5("${path.module}/../lambda/service/internal/formats_registry/structures/pennsieve-published-dataset.md")
  content_type  = "text/plain; charset=utf-8"
  cache_control = "public, max-age=300"
}
