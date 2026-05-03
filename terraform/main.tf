terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# ----------------------------------------------------------------
# AWS Academy Learner Lab profile
# This config is tuned to the sandbox restrictions:
#   - cannot create IAM roles -> use the pre-existing LabRole
#   - cannot edit EMR Block Public Access -> stick to port 22 only
#   - limited instance types and quota -> m5.large + 1 core node
# If you ever move to a real AWS account, set service_role,
# instance_profile and instance sizes back to bigger ones.
# ----------------------------------------------------------------

# ---------- Variables ----------
variable "region" {
  default = "us-east-1"
}

variable "cluster_name" {
  default = "emr-osm2ohm"
}

variable "release_label" {
  default = "emr-7.2.0"
}

variable "bucket_name" {
  default = "osm2ohm-rub21"
}

variable "service_role" {
  description = "EMR service role. Both Academy and real accounts have 'EMR_DefaultRole' pre-created."
  default     = "EMR_DefaultRole"
}

variable "instance_profile" {
  description = "EC2 instance profile used by EMR nodes."
  default     = "EMR_EC2_DefaultRole"
}

variable "master_instance_type" {
  default = "m5.xlarge"
}

variable "core_instance_type" {
  default = "m5.xlarge"
}

variable "core_instance_count" {
  default = 2
}

variable "auto_terminate_on_completion" {
  type    = bool
  default = true
}

variable "idle_timeout_seconds" {
  type    = number
  default = 600
}

# ---------- SSH Key Pair ----------
resource "tls_private_key" "emr" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "emr" {
  key_name   = "${var.cluster_name}-key"
  public_key = tls_private_key.emr.public_key_openssh
}

resource "local_file" "private_key" {
  content         = tls_private_key.emr.private_key_pem
  filename        = "${path.module}/emr-key.pem"
  file_permission = "0400"
}

# ---------- EMR Cluster ----------
resource "aws_emr_cluster" "cluster" {
  name          = var.cluster_name
  release_label = var.release_label
  applications  = ["Spark"]

  service_role = var.service_role

  ec2_attributes {
    instance_profile = var.instance_profile
    key_name         = aws_key_pair.emr.key_name
  }

  master_instance_group {
    instance_type = var.master_instance_type
  }

  core_instance_group {
    instance_type  = var.core_instance_type
    instance_count = var.core_instance_count
  }

  auto_termination_policy {
    idle_timeout = var.idle_timeout_seconds
  }

  bootstrap_action {
    name = "noop"
    path = "s3://${var.bucket_name}/bootstrap/libs.sh"
  }

  configurations_json = jsonencode([
    {
      Classification = "spark-defaults",
      Properties = {
        # Allow reading the public osm-pds bucket without credentials
        "spark.hadoop.fs.s3a.bucket.osm-pds.aws.credentials.provider" = "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
      }
    }
  ])

  log_uri = "s3://${var.bucket_name}/logs/"

  step {
    name              = "extract-ohm-candidates"
    action_on_failure = "CONTINUE"

    hadoop_jar_step {
      jar = "command-runner.jar"
      args = [
        "spark-submit",
        "--deploy-mode", "client",
        "s3://${var.bucket_name}/scripts/extract_ohm_candidates.py",
        "--history_uri", "s3a://osm-pds/planet-history/history-latest.orc",
        "--rules_uri", "s3://${var.bucket_name}/scripts/rules.json",
        "--output_uri", "s3://${var.bucket_name}/output/ohm_candidates/",
      ]
    }
  }

  keep_job_flow_alive_when_no_steps = !var.auto_terminate_on_completion

  lifecycle {
    ignore_changes = [step]
  }
}

# SSH (port 22) ingress rule is NOT managed by Terraform on purpose.
# EMR creates and reuses two persistent SGs across clusters in the same
# region (`ElasticMapReduce-master` and `ElasticMapReduce-slave`). The
# port 22 rule, once added (manually or by an earlier apply), survives
# cluster termination, so trying to add it again fails with
# "InvalidPermission.Duplicate". If you ever wipe those SGs, run:
#   aws ec2 authorize-security-group-ingress \
#     --group-id <master-sg-id> --protocol tcp --port 22 --cidr 0.0.0.0/0

# ---------- Outputs ----------
output "cluster_id" {
  value = aws_emr_cluster.cluster.id
}

output "master_dns" {
  value = aws_emr_cluster.cluster.master_public_dns
}

output "ssh_command" {
  value = "ssh -i ${path.module}/emr-key.pem hadoop@${aws_emr_cluster.cluster.master_public_dns}"
}

output "key_path" {
  value = "${path.module}/emr-key.pem"
}

output "bucket_name" {
  value = var.bucket_name
}
