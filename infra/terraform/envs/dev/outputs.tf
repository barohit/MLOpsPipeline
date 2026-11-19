output "cluster_name" {
  value = module.eks.cluster_name
}

output "kubeconfig_command" {
  value = "aws eks update-kubeconfig --name ${module.eks.cluster_name} --region ${var.region}"
}

output "ecr_repository_url" {
  value = module.ecr.repository_url
}

output "ecr_mlflow_repository_url" {
  value = module.ecr_mlflow.repository_url
}

output "artifacts_bucket" {
  value = module.artifacts.bucket_name
}

# Paste these into the ServiceAccount annotations in k8s/base
output "mlflow_role_arn" {
  value = module.mlflow_role.role_arn
}

output "drift_checker_role_arn" {
  value = module.drift_checker_role.role_arn
}

# Store these as GitHub repository variables (not secrets; they aren't credentials)
output "github_plan_role_arn" {
  value = module.github.plan_role_arn
}

output "github_deploy_role_arn" {
  value = module.github.deploy_role_arn
}
