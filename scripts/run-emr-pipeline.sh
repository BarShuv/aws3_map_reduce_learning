#!/usr/bin/env bash
# Submit the DIRT pipeline as EMR steps to an existing cluster.
# Usage: ./run-emr-pipeline.sh <cluster-id>
#   e.g. ./run-emr-pipeline.sh j-XXXXXXXXXXXXX
# Or set CLUSTER_ID: CLUSTER_ID=j-XXX ./run-emr-pipeline.sh
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_ID="${1:-$CLUSTER_ID}"
if [[ -z "$CLUSTER_ID" ]]; then
  echo "Usage: $0 <cluster-id>"
  echo "  or:  CLUSTER_ID=j-XXXXXXXXXXXXX $0"
  echo "Get cluster-id from: aws emr list-clusters --active"
  exit 1
fi
echo "Adding pipeline steps to cluster $CLUSTER_ID ..."
aws emr add-steps --cluster-id "$CLUSTER_ID" --steps "file://$SCRIPT_DIR/emr-steps.json"
echo "Done. Check cluster progress in the EMR console or: aws emr list-steps --cluster-id $CLUSTER_ID"
