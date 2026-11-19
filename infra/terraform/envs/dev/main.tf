locals {
  cluster_name = var.name
  tags = {
    Project     = "mlops-pipeline"
    Environment = "dev"
    ManagedBy   = "terraform"
  }
}

module "network" {
  source       = "../../modules/network"
  name         = var.name
  cluster_name = local.cluster_name
  tags         = local.tags
}

module "eks" {
  source               = "../../modules/eks"
  cluster_name         = local.cluster_name
  kubernetes_version   = var.kubernetes_version
  private_subnet_ids   = module.network.private_subnet_ids
  node_instance_types  = var.node_instance_types
  node_capacity_type   = var.node_capacity_type
  public_access_cidrs  = var.public_access_cidrs
  admin_principal_arns = var.admin_principal_arns
  tags                 = local.tags
}

module "ecr" {
  source = "../../modules/ecr"
  name   = "mlops-inference"
  tags   = local.tags
}

# The MLflow server image (official image plus boto3 for S3 artifacts; see infra/mlflow)
module "ecr_mlflow" {
  source = "../../modules/ecr"
  name   = "mlops-mlflow"
  tags   = local.tags
}

module "artifacts" {
  source      = "../../modules/artifacts"
  bucket_name = var.artifacts_bucket_name
  tags        = local.tags
}

# --- Least-privilege roles, one per workload ---------------------------------
# The inference pods get no AWS role at all: they fetch models through the MLflow
# server, which is the only thing allowed to touch mlflow/ in S3.

data "aws_iam_policy_document" "mlflow" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${module.artifacts.bucket_arn}/mlflow/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [module.artifacts.bucket_arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["mlflow/*"]
    }
  }
  statement {
    actions   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"]
    resources = [module.artifacts.kms_key_arn]
  }
}

module "mlflow_role" {
  source            = "../../modules/irsa_role"
  name              = "${var.name}-mlflow"
  oidc_provider_arn = module.eks.oidc_provider_arn
  oidc_issuer       = module.eks.oidc_issuer
  namespace         = var.app_namespace
  service_account   = "mlflow"
  policy_json       = data.aws_iam_policy_document.mlflow.json
  tags              = local.tags
}

# Fluent Bit may only append prediction logs; it can't read anything back
data "aws_iam_policy_document" "log_shipper" {
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${module.artifacts.bucket_arn}/prediction-logs/*"]
  }
  statement {
    actions   = ["kms:GenerateDataKey", "kms:Encrypt"]
    resources = [module.artifacts.kms_key_arn]
  }
}

module "log_shipper_role" {
  source            = "../../modules/irsa_role"
  name              = "${var.name}-fluent-bit"
  oidc_provider_arn = module.eks.oidc_provider_arn
  oidc_issuer       = module.eks.oidc_issuer
  namespace         = "logging"
  service_account   = "fluent-bit"
  policy_json       = data.aws_iam_policy_document.log_shipper.json
  tags              = local.tags
}

# The drift job reads prediction logs and labels; it writes nothing to S3
data "aws_iam_policy_document" "drift_checker" {
  statement {
    actions = ["s3:GetObject"]
    resources = [
      "${module.artifacts.bucket_arn}/prediction-logs/*",
      "${module.artifacts.bucket_arn}/labels/*",
    ]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [module.artifacts.bucket_arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["prediction-logs/*", "labels/*"]
    }
  }
  statement {
    actions   = ["kms:Decrypt"]
    resources = [module.artifacts.kms_key_arn]
  }
}

module "drift_checker_role" {
  source            = "../../modules/irsa_role"
  name              = "${var.name}-drift-checker"
  oidc_provider_arn = module.eks.oidc_provider_arn
  oidc_issuer       = module.eks.oidc_issuer
  namespace         = var.app_namespace
  service_account   = "drift-checker"
  policy_json       = data.aws_iam_policy_document.drift_checker.json
  tags              = local.tags
}

# --- CI/CD -------------------------------------------------------------------

data "aws_iam_policy_document" "deploy" {
  statement {
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }
  statement {
    actions = [
      "ecr:BatchCheckLayerAvailability", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart",
      "ecr:CompleteLayerUpload", "ecr:PutImage", "ecr:BatchGetImage", "ecr:DescribeImages",
    ]
    resources = [module.ecr.repository_arn, module.ecr_mlflow.repository_arn]
  }
  statement {
    actions   = ["eks:DescribeCluster"]
    resources = ["*"]
  }
  # CI reads the frozen holdout to run the evaluation gate
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${module.artifacts.bucket_arn}/holdouts/*"]
  }
  statement {
    actions   = ["kms:Decrypt"]
    resources = [module.artifacts.kms_key_arn]
  }
}

module "github" {
  source             = "../../modules/github_oidc"
  name_prefix        = var.name
  github_repository  = var.github_repository
  state_bucket_name  = var.state_bucket_name
  lock_table_name    = var.lock_table_name
  deploy_policy_json = data.aws_iam_policy_document.deploy.json
  tags               = local.tags
}

# The deploy role can change workloads in the app namespace, not the whole cluster
resource "aws_eks_access_entry" "github_deploy" {
  cluster_name  = module.eks.cluster_name
  principal_arn = module.github.deploy_role_arn
}

resource "aws_eks_access_policy_association" "github_deploy" {
  cluster_name  = module.eks.cluster_name
  principal_arn = module.github.deploy_role_arn
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSEditPolicy"

  access_scope {
    type       = "namespace"
    namespaces = [var.app_namespace]
  }

  depends_on = [aws_eks_access_entry.github_deploy]
}
