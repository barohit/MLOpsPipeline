terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.60, < 7.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    # Pinned to 2.x: helm provider 3.x changed the kubernetes block to an attribute
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.17"
    }
  }

  # Created by ../../bootstrap. Bucket and table names are filled in at init time:
  #   terraform init -backend-config=backend.hcl
  backend "s3" {
    key     = "mlops/dev/terraform.tfstate"
    encrypt = true
  }
}
