#!/usr/bin/env bash
# Build the project, rename JAR to dirt-algorithm.jar, upload to S3.
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUCKET="meni-biarcs-data"
JAR_NAME="dirt-algorithm.jar"
S3_JARS="s3://${BUCKET}/jars/"

echo "Building project in $PROJECT_DIR ..."
cd "$PROJECT_DIR"
mvn -q clean package -DskipTests

JAR_PATH="$PROJECT_DIR/target/aws3-map-reduce-learning-1.0-SNAPSHOT.jar"
if [[ ! -f "$JAR_PATH" ]]; then
  echo "ERROR: JAR not found at $JAR_PATH"
  exit 1
fi

cp "$JAR_PATH" "$PROJECT_DIR/target/$JAR_NAME"
echo "Uploading $JAR_NAME to $S3_JARS ..."
aws s3 cp "$PROJECT_DIR/target/$JAR_NAME" "$S3_JARS$JAR_NAME"
echo "Done. JAR at $S3_JARS$JAR_NAME"
