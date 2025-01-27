#
# Packages Service Lambda Role
#

resource "aws_iam_role" "packages_service_lambda_role" {
  name = "${var.environment_name}-${var.service_name}-lambda-role-${data.terraform_remote_state.region.outputs.aws_region_shortname}"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "packages_service_lambda_iam_policy_attachment" {
  role       = aws_iam_role.packages_service_lambda_role.name
  policy_arn = aws_iam_policy.packages_service_lambda_iam_policy.arn
}

resource "aws_iam_policy" "packages_service_lambda_iam_policy" {
  name   = "${var.environment_name}-${var.service_name}-lambda-iam-policy-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  path   = "/"
  policy = data.aws_iam_policy_document.packages_service_iam_policy_document.json
}

data "aws_iam_policy_document" "packages_service_iam_policy_document" {

  statement {
    sid     = "PackagesServiceLambdaLogsPermissions"
    effect  = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutDestination",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams"
    ]
    resources = ["*"]
  }

  statement {
    sid     = "PackagesServiceLambdaEC2Permissions"
    effect  = "Allow"
    actions = [
      "ec2:CreateNetworkInterface",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DeleteNetworkInterface",
      "ec2:AssignPrivateIpAddresses",
      "ec2:UnassignPrivateIpAddresses"
    ]
    resources = ["*"]
  }

  statement {
    sid     = "PackagesServiceLambdaRDSPermissions"
    effect  = "Allow"
    actions = [
      "rds-db:connect"
    ]
    resources = ["*"]
  }

  statement {
    sid    = "PackageServiceLambdaWriteToEventsPermission"
    effect = "Allow"

    actions = [
      "sqs:SendMessage",
      "sqs:GetQueueUrl"
    ]

    resources = [
      aws_sqs_queue.restore_package_queue.arn,
    ]
  }

}

#
# Restore Package Lambda Role
#

resource "aws_iam_role" "restore_package_lambda_role" {
  name = "${var.environment_name}-restore-package-lambda-role-${data.terraform_remote_state.region.outputs.aws_region_shortname}"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "restore_package_lambda_iam_policy_attachment" {
  role       = aws_iam_role.restore_package_lambda_role.name
  policy_arn = aws_iam_policy.restore_package_lambda_iam_policy.arn
}

resource "aws_iam_policy" "restore_package_lambda_iam_policy" {
  name   = "${var.environment_name}-restore-package-lambda-iam-policy-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  path   = "/"
  policy = data.aws_iam_policy_document.restore_package_iam_policy_document.json
}

data "aws_iam_policy_document" "restore_package_iam_policy_document" {

  statement {
    sid     = "RestorePackageLambdaLogsPermissions"
    effect  = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutDestination",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams"
    ]
    resources = ["*"]
  }

  statement {
    sid     = "RestorePackageLambdaEC2Permissions"
    effect  = "Allow"
    actions = [
      "ec2:CreateNetworkInterface",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DeleteNetworkInterface",
      "ec2:AssignPrivateIpAddresses",
      "ec2:UnassignPrivateIpAddresses"
    ]
    resources = ["*"]
  }

  statement {
    sid     = "RestorePackageLambdaRDSPermissions"
    effect  = "Allow"
    actions = [
      "rds-db:connect"
    ]
    resources = ["*"]
  }

  statement {
    sid    = "RestorePackageLambdaReadFromEventsPermission"
    effect = "Allow"

    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl"
    ]

    resources = [
      aws_sqs_queue.restore_package_queue.arn,
      "${aws_sqs_queue.restore_package_queue.arn}/*",
    ]
  }

  statement {
    sid    = "RestorePackageLambdaAccessToDynamoDB"
    effect = "Allow"

    actions = [
      "dynamodb:BatchGetItem",
      "dynamodb:BatchWriteItem",
    ]

    resources = [
      data.terraform_remote_state.process_jobs_service.outputs.process_jobs_table_arn,
      "${data.terraform_remote_state.process_jobs_service.outputs.process_jobs_table_arn}/*",
    ]

  }

  statement {
    sid    = "RestorePackageLambdaS3Permissions"
    effect = "Allow"

    actions = [
      "s3:DeleteObjectVersion",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.storage_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.storage_bucket_arn}/*",
      data.terraform_remote_state.platform_infrastructure.outputs.sparc_storage_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.sparc_storage_bucket_arn}/*",
      data.terraform_remote_state.platform_infrastructure.outputs.rejoin_storage_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.rejoin_storage_bucket_arn}/*",
      data.terraform_remote_state.platform_infrastructure.outputs.precision_storage_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.precision_storage_bucket_arn}/*",
      data.terraform_remote_state.upload_service.outputs.uploads_bucket_arn,
      "${data.terraform_remote_state.upload_service.outputs.uploads_bucket_arn}/*",
      data.terraform_remote_state.africa_south_region.outputs.af_south_s3_bucket_arn,
      "${data.terraform_remote_state.africa_south_region.outputs.af_south_s3_bucket_arn}/*",
    ]
  }

  statement {
    sid    = "PostChangeLogMessages"
    effect = "Allow"

    actions = [
      "sqs:SendMessage",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.jobs_queue_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.jobs_queue_arn}/*",
    ]
  }

  // Access Jobs Queue KMS key
  statement {
    sid    = "KMSDecryptMessages"
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKey",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.jobs_kms_key_arn,
    ]
  }

}

