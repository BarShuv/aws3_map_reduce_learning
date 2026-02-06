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

## Complexity & Memory Estimation

Final design specifications for data volume and memory [[cite: 42]].

### 1. Step 1: Counting — Data Volume and TripleKey Size

* **Data volume:** The pipeline processes **~25 GB** of raw text (10 files) [[cite: 42]].
* **Step 1 KV-pairs:** The Map phase of Step 1 produces on the order of **~150–200 million** (key, value) pairs (TripleKeys) before Shuffle and Combiner. Each key is a `TripleKey` composed of three Strings (`path`, `slot`, `word`). Each **`TripleKey` object** is **~120 bytes** in memory (three strings plus Java and Writable overhead).
* **Reduce output:** After aggregation, distinct TripleKeys are emitted once per unique (Path, Slot, Word) with their count.

### 2. Step 3: Similarity — Inverted Index and 100-Path Limit

* **Data structure:** The Reducer builds a **`List<PathMI>`** in memory for each feature (Slot+Word). Without a cap, a single feature with many paths would grow unbounded.
* **100-path limit (finalized optimization):** The Reducer enforces a **100-path limit**: when `pathMIList.size() > 100`, it returns immediately and does not build the full list or run the pairwise loop. This keeps **`List<PathMI>` memory usage per feature** bounded and **prevents OutOfMemory (OOM) errors** when data is skewed (e.g. very common words with thousands of paths). Per-feature memory stays on the order of ~17–20 KB instead of hundreds of MB.

### 3. Step 4: pathTotals and Distributed Cache

* **Loading pathTotals:** Step 4 loads the path-totals file from Step 2b into a **`HashMap<String, Double>`** (`pathTotals`) in each reducer’s `setup()`, via the Distributed Cache.
* **Normalization memory:** The **pathTotals** HashMap contains **~50k–100k** unique paths and consumes **~8 MB** of RAM in the Distributed Cache, which is negligible for the JVM.

---

## Implementation Details & Optimizations

### 1. Two-Phase MapReduce for MI (Step 2) — Finalized
Calculating Mutual Information requires global counts (Total, Count(p,s), Count(s,w)) which are not available when processing a single triple.
* **Finalized solution:** Step 2 is split into two jobs. The first job calculates marginals and writes them to a single file. The second job loads this file into the **Distributed Cache**, allowing every Mapper to access global statistics without a Join. This two-phase MI calculation is the production design.

### 2. Inverted Index and 100-Path Limit (Step 3) — Finalized
A naive comparison of all paths would require $O(N^2)$ complexity, which is infeasible.
* **Finalized solution:** An Inverted Index where the Mapper emits `(Slot, Word)` as the key; the Reducer receives all paths sharing a feature and performs pairwise comparisons only within that subset. The **100-path limit** (return when a feature has more than 100 paths) is a finalized optimization that prevents OOM under data skew.

### 3. Distributed Cache for Normalization (Step 4)
Lin's similarity formula requires dividing by the sum of total MI scores for both paths.
* **Solution:** Instead of a complex Reduce-side join, we pre-calculate path totals in Step 2b and load them into memory using **Distributed Cache** in Step 4. This allows the Reducer to perform the normalization `(MI_A + MI_B) / (Total_A + Total_B)` in a single pass.

