variable "name" {
  type = string
}

variable "cluster_name" {
  type        = string
  description = "Used to tag subnets so the AWS load balancer controller can find them"
}

variable "cidr_block" {
  type    = string
  default = "10.0.0.0/16"
}

variable "az_count" {
  type    = number
  default = 2
}

variable "tags" {
  type    = map(string)
  default = {}
}
