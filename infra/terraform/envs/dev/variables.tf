variable "region" {
  type    = string
  default = "us-east-1"
}

variable "name" {
  type    = string
  default = "mlops-dev"
}

variable "artifacts_bucket_name" {
  type        = string
  description = "Globally unique bucket for model artifacts, holdouts, prediction logs and labels"
}

variable "state_bucket_name" {
  type        = string
  description = "The bucket created by ../../bootstrap (CI's plan role needs access to it)"
}

variable "lock_table_name" {
  type    = string
  default = "terraform-locks"
}

variable "github_repository" {
  type        = string
  description = "owner/repo, for the GitHub Actions OIDC roles"
}

variable "admin_principal_arns" {
  type        = list(string)
  description = "IAM users/roles that get cluster-admin"
  default     = []
}

variable "kubernetes_version" {
  type    = string
  default = "1.33"
}

variable "node_instance_types" {
  type    = list(string)
  default = ["t3.large"]
}

variable "node_capacity_type" {
  type    = string
  default = "SPOT"
}

variable "public_access_cidrs" {
  type        = list(string)
  description = "Who can reach the cluster API. Set this to your own IP range."
  default     = ["0.0.0.0/0"]
}

variable "app_namespace" {
  type    = string
  default = "mlops"
}

# Leave a chart version null to get the latest, but pin them once things work:
# an unpinned chart can change under you on the next apply.
variable "chart_versions" {
  type = map(string)
  default = {
    kube_prometheus_stack  = null
    prometheus_pushgateway = null
    argo_rollouts          = null
    fluent_bit             = null
  }
}
