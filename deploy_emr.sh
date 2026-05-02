#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Deploys the osm2ohm EMR cluster via Terraform.
# Usage:
#   ./deploy_emr.sh                  -> apply (create cluster + auto-run step)
#   ./deploy_emr.sh apply            -> same as above
#   ./deploy_emr.sh plan             -> preview terraform changes
#   ./deploy_emr.sh destroy          -> tear down the cluster
#   ./deploy_emr.sh run              -> re-upload scripts and add a fresh step
#                                       (use after editing rules.json or .py)
#   ./deploy_emr.sh logs             -> tail the latest step's stderr live
#   ./deploy_emr.sh status           -> show the latest step state
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="$SCRIPT_DIR/terraform"

source "$SCRIPT_DIR/.env.aws"

ACTION="${1:-apply}"
BUCKET="osm2ohm-rub21"

prepare_aws() {
  echo ">> Creating EMR default roles (if missing)..."
  aws emr create-default-roles >/dev/null

  echo ">> Ensuring bucket s3://$BUCKET ..."
  if ! aws s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
    aws s3 mb "s3://$BUCKET" --region "$AWS_DEFAULT_REGION"
  fi

  echo ">> Uploading bootstrap script to s3://$BUCKET/bootstrap/ ..."
  aws s3 cp "$SCRIPT_DIR/libs.sh" "s3://$BUCKET/bootstrap/libs.sh"

  echo ">> Uploading scripts and rules to s3://$BUCKET/scripts/ ..."
  aws s3 cp "$SCRIPT_DIR/extract_ohm_candidates.py" "s3://$BUCKET/scripts/extract_ohm_candidates.py"
  aws s3 cp "$SCRIPT_DIR/rules.json" "s3://$BUCKET/scripts/rules.json"

  echo ">> Uploading countries/ to s3://$BUCKET/countries/ ..."
  aws s3 sync "$SCRIPT_DIR/countries/" "s3://$BUCKET/countries/" --delete
}

cluster_id() {
  cd "$TF_DIR"
  terraform output -raw cluster_id
}

master_dns() {
  cd "$TF_DIR"
  terraform output -raw master_dns
}

key_path() {
  cd "$TF_DIR"
  terraform output -raw key_path
}

latest_step_id() {
  local cid="$1"
  aws emr list-steps --cluster-id "$cid" \
    --query 'Steps[0].Id' --output text
}

echo ">> Verifying credentials..."
aws sts get-caller-identity | jq -r '"  Account: \(.Account)\n  User:    \(.Arn)"'

case "$ACTION" in
  apply)
    cd "$TF_DIR"
    echo ">> terraform init ..."
    terraform init -input=false
    echo ">> terraform apply ..."
    prepare_aws
    terraform apply -auto-approve

    JUP_URL=$(terraform output -raw jupyter_url)
    YARN_URL=$(terraform output -raw yarn_ui)
    SPARK_URL=$(terraform output -raw spark_history_ui)
    CID=$(terraform output -raw cluster_id)
    SSH_CMD=$(terraform output -raw ssh_command)
    KEY_PATH=$(terraform output -raw key_path)

    echo ""
    echo "==================================================================="
    echo "Cluster:        $CID"
    echo "Bucket:         s3://$BUCKET"
    echo "JupyterHub:     $JUP_URL  (login: jovyan / jupyter)"
    echo "YARN UI:        $YARN_URL"
    echo "Spark History:  $SPARK_URL"
    echo "Key (.pem):     $KEY_PATH"
    echo "SSH:            $SSH_CMD"
    echo "==================================================================="
    echo "Spark step lanzado automaticamente."
    echo "Ver progreso:   ./deploy_emr.sh logs"
    echo "Estado:         ./deploy_emr.sh status"
    echo "Destruir:       ./deploy_emr.sh destroy"
    ;;

  destroy)
    cd "$TF_DIR"
    echo ">> terraform destroy ..."
    terraform destroy -auto-approve
    ;;

  plan)
    cd "$TF_DIR"
    terraform init -input=false >/dev/null
    terraform plan
    ;;

  run)
    CID=$(cluster_id)
    prepare_aws
    echo ">> Adding step to cluster $CID ..."
    STEP_ID=$(aws emr add-steps --cluster-id "$CID" --steps "$(cat <<EOF
[{
  "Name": "extract-ohm-candidates",
  "ActionOnFailure": "CONTINUE",
  "Jar": "command-runner.jar",
  "Args": [
    "spark-submit",
    "--deploy-mode", "client",
    "s3://$BUCKET/scripts/extract_ohm_candidates.py",
    "--history_uri",   "s3a://osm-pds/planet-history/history-latest.orc",
    "--rules_uri",     "s3://$BUCKET/scripts/rules.json",
    "--countries_uri", "s3://$BUCKET/countries",
    "--output_uri",    "s3://$BUCKET/output/ohm_candidates/"
  ]
}]
EOF
)" --query 'StepIds[0]' --output text)
    echo "   step: $STEP_ID"
    echo ">> Tail logs with: ./deploy_emr.sh logs"
    ;;

  status)
    CID=$(cluster_id)
    SID=$(latest_step_id "$CID")
    aws emr describe-step --cluster-id "$CID" --step-id "$SID" \
      --query 'Step.{Id:Id,Name:Name,State:Status.State,Reason:Status.StateChangeReason.Message,Created:Status.Timeline.CreationDateTime,Started:Status.Timeline.StartDateTime,Ended:Status.Timeline.EndDateTime}' \
      --output table
    ;;

  logs)
    CID=$(cluster_id)
    DNS=$(master_dns)
    KEY=$(key_path)
    SID=$(latest_step_id "$CID")
    echo ">> Cluster: $CID  step: $SID"
    echo ">> Tailing /mnt/var/log/hadoop/steps/$SID/stderr on $DNS ..."
    echo "   (Ctrl-C to stop. Si el step recien arranca, espera ~30s.)"
    ssh -o StrictHostKeyChecking=accept-new -i "$KEY" "hadoop@$DNS" \
      "tail -F /mnt/var/log/hadoop/steps/$SID/stderr /mnt/var/log/hadoop/steps/$SID/stdout 2>/dev/null"
    ;;

  *)
    echo "Unknown action: $ACTION"
    echo "Use: apply | plan | destroy | run | logs | status"
    exit 1
    ;;
esac
