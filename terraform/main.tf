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

variable "master_instance_type" {
  default = "m5.xlarge"
}

variable "core_instance_type" {
  default = "m5.2xlarge"
}

variable "core_instance_count" {
  default = 3
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
  applications  = ["Spark", "JupyterHub"]

  service_role = "EMR_DefaultRole"

  ec2_attributes {
    instance_profile = "EMR_EC2_DefaultRole"
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
    idle_timeout = 3600
  }

  bootstrap_action {
    name = "Install Python libs"
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

  # Auto-launch the Spark job as soon as the cluster is ready.
  # client mode keeps the driver on the master so its stdout/stderr land in
  # the step log (s3://.../logs/<cluster>/steps/<step>/stderr.gz) and are
  # also tailable live via SSH at /mnt/var/log/hadoop/steps/<step>/stderr.
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
        "--countries_uri", "s3://${var.bucket_name}/countries",
        "--output_uri", "s3://${var.bucket_name}/output/ohm_candidates/",
      ]
    }
  }

  # The cluster keeps running after the step finishes (idle timeout handles
  # shutdown). We also tell terraform to ignore step drift so re-applies
  # don't try to recreate the cluster just because a step ran.
  keep_job_flow_alive_when_no_steps = true

  lifecycle {
    ignore_changes = [step]
  }
}

# ---------- Open port 9443 (JupyterHub) ----------
resource "aws_security_group_rule" "jupyterhub" {
  type              = "ingress"
  from_port         = 9443
  to_port           = 9443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_emr_cluster.cluster.ec2_attributes[0].emr_managed_master_security_group
  description       = "JupyterHub access"
}

# ---------- Open port 22 (SSH) ----------
resource "aws_security_group_rule" "ssh" {
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_emr_cluster.cluster.ec2_attributes[0].emr_managed_master_security_group
  description       = "SSH access"
}

# ---------- Open port 8088 (YARN ResourceManager UI) ----------
resource "aws_security_group_rule" "yarn_ui" {
  type              = "ingress"
  from_port         = 8088
  to_port           = 8088
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_emr_cluster.cluster.ec2_attributes[0].emr_managed_master_security_group
  description       = "YARN ResourceManager UI"
}

# ---------- Open port 18080 (Spark History Server) ----------
resource "aws_security_group_rule" "spark_history" {
  type              = "ingress"
  from_port         = 18080
  to_port           = 18080
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_emr_cluster.cluster.ec2_attributes[0].emr_managed_master_security_group
  description       = "Spark History Server UI"
}

# ---------- Open port 20888 (Spark Application UI proxy via YARN) ----------
resource "aws_security_group_rule" "spark_app_proxy" {
  type              = "ingress"
  from_port         = 20888
  to_port           = 20888
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_emr_cluster.cluster.ec2_attributes[0].emr_managed_master_security_group
  description       = "Spark application UI proxied through YARN"
}

# ---------- Outputs ----------
output "cluster_id" {
  value = aws_emr_cluster.cluster.id
}

output "master_dns" {
  value = aws_emr_cluster.cluster.master_public_dns
}

output "jupyter_url" {
  value = "https://${aws_emr_cluster.cluster.master_public_dns}:9443"
}

output "ssh_command" {
  value = "ssh -i ${path.module}/emr-key.pem hadoop@${aws_emr_cluster.cluster.master_public_dns}"
}

output "yarn_ui" {
  value = "http://${aws_emr_cluster.cluster.master_public_dns}:8088"
}

output "spark_history_ui" {
  value = "http://${aws_emr_cluster.cluster.master_public_dns}:18080"
}

output "key_path" {
  value = "${path.module}/emr-key.pem"
}

output "bucket_name" {
  value = var.bucket_name
}
