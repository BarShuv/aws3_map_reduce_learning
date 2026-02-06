# Deploy DIRT Pipeline to AWS EMR

## 1. Package (single JAR)

From the project root:

```bash
mvn clean package -DskipTests
```

- **Output JAR:** `target/aws3-map-reduce-learning-1.0-SNAPSHOT.jar`  
- This JAR contains your classes only; Hadoop is provided by EMR (`provided` scope).

---

## 2. Upload JAR to S3

**Option A – use the script (recommended):**

```bash
chmod +x scripts/upload-jar.sh
./scripts/upload-jar.sh
```

This will:
- Run `mvn clean package -DskipTests`
- Copy the JAR to `target/dirt-algorithm.jar`
- Upload to `s3://meni-biarcs-data/jars/dirt-algorithm.jar`

**Option B – manual:**

```bash
mvn clean package -DskipTests
cp target/aws3-map-reduce-learning-1.0-SNAPSHOT.jar target/dirt-algorithm.jar
aws s3 cp target/dirt-algorithm.jar s3://meni-biarcs-data/jars/dirt-algorithm.jar
```

---

## 3. EMR execution

### Prerequisites

- An EMR cluster (Hadoop 3.x) with the same S3 bucket accessible.
- JAR already at `s3://meni-biarcs-data/jars/dirt-algorithm.jar`.

### Option A – add steps to an existing cluster

```bash
# Get your cluster ID (e.g. j-XXXXXXXXXXXXX)
aws emr list-clusters --active

# Run the pipeline steps
./scripts/run-emr-pipeline.sh j-XXXXXXXXXXXXX
```

Or with `CLUSTER_ID` set:

```bash
CLUSTER_ID=j-XXXXXXXXXXXXX ./scripts/run-emr-pipeline.sh
```

Steps are defined in `scripts/emr-steps.json`.

### Option B – AWS Console (add steps manually)

1. EMR → Clusters → your cluster → **Steps** → **Add step**.
2. For each step below, use:
   - **Step type:** Custom JAR  
   - **JAR location:** `command-runner.jar`  
   - **Arguments:** as in the table (space-separated).

| Step name         | Arguments |
|-------------------|-----------|
| Step1_Counting    | `hadoop` `jar` `s3://meni-biarcs-data/jars/dirt-algorithm.jar` `Step1_Counting` `s3://meni-biarcs-data/` `s3://meni-biarcs-data/output/step1` |
| Step2_CalcMI      | `hadoop` `jar` `s3://meni-biarcs-data/jars/dirt-algorithm.jar` `Step2_CalcMI` `s3://meni-biarcs-data/output/step1` `s3://meni-biarcs-data/output/marginals` `s3://meni-biarcs-data/output/step2_mi` |
| Step2_PathTotals  | `hadoop` `jar` `s3://meni-biarcs-data/jars/dirt-algorithm.jar` `Step2_PathTotals` `s3://meni-biarcs-data/output/step2_mi` `s3://meni-biarcs-data/output/path_totals` |
| Step3_Similarity  | `hadoop` `jar` `s3://meni-biarcs-data/jars/dirt-algorithm.jar` `Step3_Similarity` `s3://meni-biarcs-data/output/step2_mi` `s3://meni-biarcs-data/output/step3_pairs` |
| Step4_FinalSum    | `hadoop` `jar` `s3://meni-biarcs-data/jars/dirt-algorithm.jar` `Step4_FinalSum` `s3://meni-biarcs-data/output/step3_pairs` `s3://meni-biarcs-data/output/final_result` `s3://meni-biarcs-data/output/path_totals/part-r-00000` |

Add steps in this order so each step runs after its inputs are written.

---

## Pipeline I/O summary

| Step              | Input | Output |
|-------------------|--------|--------|
| Step 1            | `s3://meni-biarcs-data/` | `s3://meni-biarcs-data/output/step1` |
| Step 2 (Marginals + MI) | `.../step1` | `.../marginals`, `.../step2_mi` |
| Step 2b Path Totals | `.../step2_mi` | `.../path_totals` |
| Step 3           | `.../step2_mi` | `.../step3_pairs` |
| Step 4           | `.../step3_pairs` + path_totals file | `.../final_result` |

**Final result:** `s3://meni-biarcs-data/output/final_result/` (e.g. `part-r-00000` with lines `PathA \t PathB \t SimilarityScore`).
