/**
 * Standalone local test for Step 1 (Parsing & Counting) without a Hadoop cluster.
 * Simulates Map phase (using Step1_Counting.PathCountMapper logic), grouping (combiner effect),
 * and Reduce phase (using Step1_Counting.PathCountReducer); verifies counts.
 */
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;

public class Step1_LocalTest {

    public static void main(String[] args) {
        System.out.println("=== Step1 Local Test (Map -> Group/Combine -> Reduce) ===\n");

        // Input: 2-3 raw lines simulating Biarcs (syntactic n-gram format)
        List<String> inputLines = new ArrayList<>();
        inputLines.add("John/NNP/nsubj/1\teats/VBZ/ROOT/0\tapples/NNS/dobj/1");
        inputLines.add("John/NNP/nsubj/1\teats/VBZ/ROOT/0\tapples/NNS/dobj/1");  // duplicate: "eat -> dobj -> apple" twice
        inputLines.add("waits/VBZ/ROOT/0\tfor/IN/prep/1\tMary/NNP/pobj/2\t153");

        // ---- Map phase: same logic as PathCountMapper ----
        List<Step1_Counting.MapEntry> mapOutput = new ArrayList<>();
        for (String line : inputLines) {
            mapOutput.addAll(Step1_Counting.runMapLogic(line));
        }

        System.out.println("Map output (key -> value) count: " + mapOutput.size());
        for (Step1_Counting.MapEntry e : mapOutput) {
            System.out.println("  " + e.key.toString() + " -> " + e.value);
        }

        // ---- Group by key (simulates Shuffle + Combiner: same key -> same reducer) ----
        Map<TripleKey, List<IntWritable>> grouped = new LinkedHashMap<>();
        for (Step1_Counting.MapEntry e : mapOutput) {
            TripleKey k = e.key;
            grouped.computeIfAbsent(k, x -> new ArrayList<>()).add(new IntWritable(e.value));
        }
        System.out.println("\nGrouped keys: " + grouped.size());

        // ---- Reduce phase: same logic as PathCountReducer (and Combiner) ----
        Map<TripleKey, Integer> finalOutput = new LinkedHashMap<>();
        for (Map.Entry<TripleKey, List<IntWritable>> e : grouped.entrySet()) {
            int sum = Step1_Counting.runReduceLogic(e.getKey(), e.getValue());
            finalOutput.put(e.getKey(), sum);
        }

        System.out.println("\nReducer output:");
        for (Map.Entry<TripleKey, Integer> e : finalOutput.entrySet()) {
            System.out.println("  " + e.getKey().toString() + " -> " + e.getValue());
        }

        // ---- Verification (Path = "verb -> relation", Slot = X/Y, Word = filler) ----
        Map<String, Integer> tripleToCount = new LinkedHashMap<>();
        for (Map.Entry<TripleKey, Integer> e : finalOutput.entrySet()) {
            tripleToCount.put(e.getKey().toString(), e.getValue());
        }

        boolean pass = true;
        // (eat -> dobj, Y, apple) should appear twice
        int eatApple = tripleToCount.getOrDefault("eat -> dobj\tY\tapple", 0);
        if (eatApple != 2) {
            System.err.println("FAIL: expected count 2 for (eat -> dobj, Y, apple), got " + eatApple);
            pass = false;
        }
        // (eat -> nsubj, X, john) should appear twice
        int eatJohn = tripleToCount.getOrDefault("eat -> nsubj\tX\tjohn", 0);
        if (eatJohn != 2) {
            System.err.println("FAIL: expected count 2 for (eat -> nsubj, X, john), got " + eatJohn);
            pass = false;
        }
        // (wait -> for, Y, mary) should appear once
        int waitMary = tripleToCount.getOrDefault("wait -> for\tY\tmary", 0);
        if (waitMary != 1) {
            System.err.println("FAIL: expected count 1 for (wait -> for, Y, mary), got " + waitMary);
            pass = false;
        }
        // Combiner verification: we had 2+2+1 = 5 map emits (2 paths per "eats" line * 2 lines = 4, plus 1 path for "waits") -> 5 map outputs; after group+reduce we must have 3 distinct triples with correct totals
        int totalDistinct = finalOutput.size();
        if (totalDistinct < 3) {
            System.err.println("FAIL: expected at least 3 distinct triples, got " + totalDistinct);
            pass = false;
        }

        if (pass) {
            System.out.println("\n*** PASS *** Counts and combiner (group+sum) logic verified.");
        } else {
            System.out.println("\n*** FAIL ***");
            System.exit(1);
        }
    }
}
