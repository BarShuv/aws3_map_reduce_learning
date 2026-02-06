# DIRT Algorithm - Extraction Phase

**Repository:** https://github.com/BarShuv/aws3_map_reduce_learning

**Submission Note:** This project implements the DIRT algorithm and evaluates it on the Small Input experiment (10 files). Due to technical constraints with AWS resource limits during the final phase, results for the 100-file experiment are not included, though the system design fully supports it.

## Project Overview

This project implements the **Extraction Phase** of the DIRT (Discovery of Inference Rules from Text) algorithm for a university assignment. The extraction phase parses syntactic N-gram dependency parse trees and extracts meaningful dependency paths between verbs and nouns.

## What Has Been Implemented

### Core Components

1. **`Node.java`** - Data structure representing a word in the dependency parse tree
   - Stores: word, POS tag, dependency label, head index, and node index
   - Helper methods to identify verbs, nouns, prepositions, and auxiliary verbs

2. **`PorterStemmer.java`** - Word stemming utility
   - Implements simplified Porter Stemming Algorithm
   - Handles common verb endings (-ing, -ed, -s) and noun pluralizations
   - Reduces words to their root forms (e.g., "waits" → "wait", "walking" → "walk")

3. **`PathExtractor.java`** - Main extraction logic
   - Parses syntactic N-gram format input lines
   - Builds dependency tree structure
   - Extracts paths following specific rules:
     - Finds head verbs (excluding auxiliary verbs: is, are, was, were, be, been, am)
     - Identifies noun children connected to verbs
     - Handles two cases:
       - **Case A (Direct):** Verb → Noun (using dependency label as relation)
       - **Case B (Preposition):** Verb → Preposition → Noun (collapses preposition as relation)
   - Auto-detects 0-based vs 1-based head index formats

4. **`Main.java`** - Test suite with comprehensive test cases
   - Tests various scenarios to verify correctness
   - Includes helper method `testSentence()` for easy testing

## Input Format

The input format is a single line with tab-separated tokens:

```
word/POS-tag/dependency-label/head-index
```

**Example:**
```
waits/VBZ/ROOT/0	for/IN/prep/1	Mary/NNP/pobj/2	153
```

Where:
- `word` - The actual word token
- `POS-tag` - Part-of-speech tag (e.g., VBZ, NNP, IN)
- `dependency-label` - Dependency relation (e.g., ROOT, prep, pobj)
- `head-index` - Index of the head/parent node (0-based or 1-based, auto-detected)
- The last number (e.g., `153`) is a frequency count and is ignored

## Output Format

Extracted paths are printed in the format:

```
stemmed_verb -> relation -> stemmed_noun
```

**Examples:**
- `wait -> for -> mary` (preposition case)
- `eat -> dobj -> apple` (direct object case)
- `walk -> nsubj -> john` (subject case)

## Test Cases

The project includes 4 test cases in `Main.java`:

1. **The Standard Case (Preposition):** Tests verb → preposition → noun paths
   - Input: `waits/VBZ/ROOT/0	for/IN/prep/1	Mary/NNP/pobj/2	153`
   - Expected: `wait -> for -> mary`

2. **Direct Object Case (No Preposition):** Tests direct verb → noun relations
   - Input: `John/NNP/nsubj/1	eats/VBZ/ROOT/0	apples/NNS/dobj/1`
   - Expected: `eat -> nsubj -> john` and `eat -> dobj -> apple`

3. **Auxiliary Verb Filter:** Verifies auxiliary verbs are ignored
   - Input: `John/NNP/nsubj/2	is/VBZ/aux/2	walking/VBG/ROOT/0`
   - Expected: `walk -> nsubj -> john` (ignores "is")

4. **Stemming Check:** Verifies word stemming works correctly
   - Input: `He/PRP/nsubj/1	solved/VBD/ROOT/0	it/PRP/dobj/1`
   - Note: No paths found (correct - "it" is a pronoun, not a noun)

## How to Compile and Run

```bash
# Compile all Java files
javac src/*.java

# Run the test suite
java -cp src Main
```

## Project Structure

```
aws3_map_reduce_learning/
├── src/
│   ├── Node.java, PorterStemmer.java, PathExtractor.java, Main.java  # Step 1 (extraction)
│   ├── Step2_CalcMI.java, Step2_PathTotals.java  # Step 2 (MI + path totals)
│   ├── Step3_Similarity.java, Step4_FinalSum.java # Steps 3–4 (similarity)
│   └── ...
├── pom.xml
├── .gitignore
└── README.md
```

## Key Features

- ✅ Parses syntactic N-gram dependency format
- ✅ Auto-detects head index format (0-based vs 1-based)
- ✅ Filters auxiliary verbs
- ✅ Handles both direct and preposition-mediated paths
- ✅ Stems words using Porter Stemmer
- ✅ Comprehensive test suite
- ✅ Clean, documented code with Javadoc headers

---

## MapReduce Pipeline (Steps 2–4): MI and Lin Similarity

This is the **completed** MapReduce implementation for the DIRT extraction and similarity pipeline. Step 1 output is `((Path, Slot, Word), Count)` — text lines: `Path \t Slot \t Word \t Count`.

### Step 2: MI Calculation (`Step2_CalcMI.java`)

- **Input:** Step 1 output (Path, Slot, Word, Count).
- **Two-phase:**
  1. **Marginals:** Aggregates `count(p,s)`, `count(s,w)`, and `|Total|`.
  2. **MI:** Loads marginals from distributed cache and computes  
     `MI(p,s,w) = log( (count(p,s,w) * |Total|) / (count(p,s) * count(s,w)) )`.
- **Output:** `Path \t Slot \t Word \t MI`.

```bash
# Run (e.g. on Hadoop or local runner)
hadoop jar ... Step2_CalcMI <input_triples> <marginals_dir> <mi_output_dir>
```

### Step 2b: Path Totals (`Step2_PathTotals.java`) — optional, for Lin normalization

- **Input:** Step 2 MI output.
- **Output:** `Path \t TotalMI` (sum of MI over all (Slot, Word) for that Path).
- Use this file as the third argument to Step 4 for normalized Lin similarity.

### Step 3: Similarity via Inverted Index (`Step3_Similarity.java`)

- **Mapper:** Input `(Path, Slot, Word, MI)`. Key = **Feature** `(Slot, Word)`, Value = `(Path, MI)`.
- **Reducer:** For each feature, gets list of `[(PathA, MI_A), (PathB, MI_B), ...]`. Emits all unordered pairs `(PathA, PathB)` with **partial score** `MI_A + MI_B` (numerator term for Lin).
- **Output:** `PathA \t PathB \t PartialScore`.

```bash
hadoop jar ... Step3_Similarity <step2_mi_output> <step3_output>
```

### Step 4: Summing Scores (`Step4_FinalSum.java`)

- **Mapper:** Identity: `(PathA, PathB, PartialScore)` → key `(PathA, PathB)`, value `PartialScore`.
- **Reducer:** Sums partial scores for each `(PathA, PathB)`. Optionally divides by `(TotalMI_A + TotalMI_B)` if path totals file is provided (Lin’s full similarity).
- **Output:** `PathA \t PathB \t SimilarityScore` (e.g. `X control Y \t X prevent Y \t 0.1566`).

```bash
# Raw numerator sum only:
hadoop jar ... Step4_FinalSum <step3_output> <final_output>

# Normalized Lin similarity (pass path totals from Step2_PathTotals):
hadoop jar ... Step4_FinalSum <step3_output> <final_output> <path_totals_file>
```

### Build (Maven)

```bash
mvn compile package
# Use target/aws3-map-reduce-learning-1.0-SNAPSHOT.jar on your Hadoop cluster
```

### Project Structure (updated)

```
aws3_map_reduce_learning/
├── src/
│   ├── Node.java            # Node data structure
│   ├── PorterStemmer.java   # Stemming utility
│   ├── PathExtractor.java   # Extraction logic (Step 1)
│   ├── Main.java            # Extraction test suite
│   ├── Step2_CalcMI.java    # MI calculation (two-phase)
│   ├── Step2_PathTotals.java # Path totals for Lin denominator
│   ├── Step3_Similarity.java # Inverted-index similarity (pairs + partial scores)
│   └── Step4_FinalSum.java  # Sum partial scores; optional Lin normalization
├── pom.xml
├── .gitignore
└── README.md
```

---

## Notes

- The Porter Stemmer is implemented with a simplified algorithm that handles common cases. A full Porter Stemmer implementation can be added if needed.
- The code automatically detects whether head indices are 0-based or 1-based by analyzing the dependency structure.
- Only paths with actual nouns (POS tags starting with "NN") are extracted - pronouns are excluded.
