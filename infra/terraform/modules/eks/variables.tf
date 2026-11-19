variable "cluster_name" {
  type = string
}

variable "kubernetes_version" {
  type        = string
  description = "Check the versions EKS currently supports before applying"
  default     = "1.33"
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "endpoint_public_access" {
  type        = bool
  description = "Public API endpoint for kubectl from outside the VPC. Lock it to public_access_cidrs."
  default     = true
}

variable "public_access_cidrs" {
  type    = list(string)
  default = ["0.0.0.0/0"]
}

variable "node_instance_types" {
  type    = list(string)
  default = ["t3.large"]
}

variable "node_capacity_type" {
  type        = string
  description = "ON_DEMAND or SPOT. SPOT is fine for a dev cluster."
  default     = "ON_DEMAND"
}

variable "node_desired_size" {
  type    = number
  default = 2
}

variable "node_min_size" {
  type    = number
  default = 1
}

variable "node_max_size" {
  type    = number
  default = 4
}

variable "admin_principal_arns" {
  type        = list(string)
  description = "IAM users/roles that get cluster-admin through EKS access entries"
  default     = []
}

variable "tags" {
  type    = map(string)
  default = {}
}
