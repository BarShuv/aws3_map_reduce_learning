# System Design

## Step 1: Parsing and Counting

**Component Description:**
This step processes the raw corpus to extract dependency paths and count their occurrences with specific slot fillers.

**Input:**
* **Source:** Google Biarcs dataset (S3).
* **Format:** Raw syntactic n-gram lines. Each line is a sentence; top-level fields are TAB-separated. The parse field contains space-separated tokens of the form `word/POS/dependency/head` (e.g., `death/NN/nsubj/2`).

**Map Logic:**
* **Parsing:** The `PathExtractor` parses each line. It splits by TAB and then by whitespace.
* **Filtering:**
    * Verbs are identified by POS tags starting with `VB`.
    * Auxiliary verbs (like "be") are kept if they are at the root or `ccomp` position.
    * Slot fillers must be Nouns (NN), Determiners (DT), or Pronouns (PRP).
* **Path Extraction:** Constructs a dependency graph and extracts paths:
    * `Verb -> Relation -> Noun`
    * `Verb -> Preposition -> Noun`
* **Emission:** Converts extracted paths to a Triple format.
    * **Key:** `TripleKey(path, slot, word)` where `path` is the dependency path string, `slot` is "X" or "Y", and `word` is the filler.
    * **Value:** `1` (IntWritable).

**Reduce Logic:**
* **Aggregation:** Groups by the unique `TripleKey`.
* **Summation:** Sums all counts for that specific (Path, Slot, Word) combination.
* **Output:** Emits the total count for each triple.

**Data Structures & Types:**
* **Map Input:** `LongWritable` (offset), `Text` (line).
* **Map Output:** `TripleKey` (Custom WritableComparable), `IntWritable` (1).
* **Output Format:** `TextOutputFormat` (Tab-separated: `Path \t Slot \t Word \t Count`).

---

## Implementation Details & Optimizations

### 1. Two-Phase MapReduce for MI (Step 2)
Calculating Mutual Information requires global counts (Total, Count(p,s), Count(s,w)) which are not available when processing a single triple.
* **Solution:** We split Step 2 into two jobs. The first job calculates marginals and writes them to a single file. The second job loads this small file into the **Distributed Cache**, allowing every Mapper to access global statistics instantly without a Join operation.

### 2. Inverted Index for Similarity (Step 3)
A naive comparison of all paths would require $O(N^2)$ complexity, which is infeasible.
* **Solution:** We implemented an Inverted Index approach where the Mapper emits `(Slot, Word)` as the key. The Reducer receives all paths sharing a specific feature and only performs pairwise comparisons within that small subset.

### 3. Distributed Cache for Normalization (Step 4)
Lin's similarity formula requires dividing by the sum of total MI scores for both paths.
* **Solution:** Instead of a complex Reduce-side join, we pre-calculate path totals in Step 2b and load them into memory using **Distributed Cache** in Step 4. This allows the Reducer to perform the normalization `(MI_A + MI_B) / (Total_A + Total_B)` in a single pass.
