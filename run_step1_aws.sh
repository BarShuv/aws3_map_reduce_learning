#!/usr/bin/env bash
# Build, upload JAR, and submit Step 1 (Counting) to EMR.
# Usage: ./run_step1_aws.sh <cluster-id>
#   e.g. ./run_step1_aws.sh j-XXXXXXXXXXXXX
set -e

CLUSTER_ID="$1"
if [[ -z "$CLUSTER_ID" ]]; then
  echo "Usage: $0 <cluster-id>"
  echo "  e.g. $0 j-XXXXXXXXXXXXX"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
JAR_NAME="dirt-algorithm.jar"
S3_JAR="s3://meni-biarcs-data/jars/$JAR_NAME"

echo "1. Building JAR..."
mvn -q clean package -DskipTests
cp "target/aws3-map-reduce-learning-1.0-SNAPSHOT.jar" "target/$JAR_NAME"

echo "2. Uploading to $S3_JAR ..."
aws s3 cp "target/$JAR_NAME" "$S3_JAR"

echo "3. Submitting Step 1 to cluster $CLUSTER_ID ..."
aws emr add-steps --cluster-id "$CLUSTER_ID" --steps '[
  {
    "Name": "Step1_Counting",
    "ActionOnFailure": "CONTINUE",
    "Type": "CUSTOM_JAR",
    "Jar": "command-runner.jar",
    "Args": [
      "hadoop", "jar", "'"$S3_JAR"'",
      "Step1_Counting",
      "s3://meni-biarcs-data/",
      "s3://meni-biarcs-data/output/step1"
    ]
  }
]'

echo "Done. Step 1 submitted. Check: aws emr list-steps --cluster-id $CLUSTER_ID"
