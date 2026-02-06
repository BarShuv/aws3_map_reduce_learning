/**
 * Full pipeline local test: Step 1 -> Step 2 (Marginals + MI) -> Step 3 (Inverted Index + Pairs) -> Step 4 (Sum).
 * Prints inferred rules (PathA <-> PathB : similarity).
 */
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;

public class FullPipeline_LocalTest {

    public static void main(String[] args) {
        System.out.println("========== DIRT Full Pipeline Local Test ==========\n");

        // ---- 1. Input: sample sentences (include a synonym path so (Slot, Word) is shared) ----
        List<String> inputLines = new ArrayList<>();
        inputLines.add("John/NNP/nsubj/1\teats/VBZ/ROOT/0\tapples/NNS/dobj/1");
        inputLines.add("John/NNP/nsubj/1\teats/VBZ/ROOT/0\tapples/NNS/dobj/1");
        inputLines.add("John/NNP/nsubj/1\tconsumes/VBZ/ROOT/0\tapples/NNS/dobj/1");  // same (Y, apple) -> pair with eat
        inputLines.add("waits/VBZ/ROOT/0\tfor/IN/prep/1\tMary/NNP/pobj/2\t153");

        System.out.println("Input sentences: " + inputLines.size());

        // ---- 2. Step 1: Triple counts ----
        List<Step1_Counting.MapEntry> mapOut = new ArrayList<>();
        for (String line : inputLines) {
            mapOut.addAll(Step1_Counting.runMapLogic(line));
        }
        Map<TripleKey, List<IntWritable>> grouped1 = new LinkedHashMap<>();
        for (Step1_Counting.MapEntry e : mapOut) {
            grouped1.computeIfAbsent(e.key, k -> new ArrayList<>()).add(new IntWritable(e.value));
        }
        List<Step2_CalcMI.TripleCount> tripleCounts = new ArrayList<>();
        for (Map.Entry<TripleKey, List<IntWritable>> e : grouped1.entrySet()) {
            int count = Step1_Counting.runReduceLogic(e.getKey(), e.getValue());
            TripleKey k = e.getKey();
            tripleCounts.add(new Step2_CalcMI.TripleCount(k.getPath(), k.getSlot(), k.getWord(), count));
        }
        System.out.println("Step 1 output (triple counts): " + tripleCounts.size());
        for (Step2_CalcMI.TripleCount t : tripleCounts) {
            System.out.println("  " + t.path + "\t" + t.slot + "\t" + t.word + "\t" + t.count);
        }

        // ---- 3. Step 2: Marginals + MI ----
        Step2_CalcMI.Marginals marginals = Step2_CalcMI.computeMarginalsFromTriples(tripleCounts);
        List<Step3_Similarity.PathSlotWordMI> miRows = new ArrayList<>();
        for (Step2_CalcMI.TripleCount t : tripleCounts) {
            double mi = Step2_CalcMI.calculateMIForTriple(t, marginals);
            if (!Double.isNaN(mi)) {
                miRows.add(new Step3_Similarity.PathSlotWordMI(t.path, t.slot, t.word, mi));
            }
        }
        System.out.println("\nStep 2 output (Path, Slot, Word, MI): " + miRows.size());
        for (Step3_Similarity.PathSlotWordMI r : miRows) {
            System.out.println("  " + r.path + "\t" + r.slot + "\t" + r.word + "\t" + String.format("%.4f", r.mi));
        }

        // Path totals for Lin normalization (sum MI per path)
        Map<String, Double> pathTotals = new LinkedHashMap<>();
        for (Step3_Similarity.PathSlotWordMI r : miRows) {
            pathTotals.merge(r.path, r.mi, Double::sum);
        }

        // ---- 4. Step 3: Inverted index -> pairs + partial scores ----
        Map<String, List<Step3_Similarity.PathMI>> featureToPathMI = Step3_Similarity.runInvertedIndexMap(miRows);
        List<Step3_Similarity.Pair<String, Double>> partialScores = new ArrayList<>();
        for (Map.Entry<String, List<Step3_Similarity.PathMI>> e : featureToPathMI.entrySet()) {
            partialScores.addAll(Step3_Similarity.runPairEmitReduce(e.getKey(), e.getValue()));
        }
        System.out.println("\nStep 3 output (path pairs, partial score): " + partialScores.size());
        for (Step3_Similarity.Pair<String, Double> p : partialScores) {
            System.out.println("  " + p.first + "\t" + String.format("%.4f", p.second));
        }

        // ---- 5. Step 4: Sum partial scores (with optional Lin normalization) ----
        Map<String, List<Double>> pairToPartials = new LinkedHashMap<>();
        for (Step3_Similarity.Pair<String, Double> p : partialScores) {
            pairToPartials.computeIfAbsent(p.first, k -> new ArrayList<>()).add(p.second);
        }
        List<Step3_Similarity.Pair<String, Double>> finalScores = new ArrayList<>();
        for (Map.Entry<String, List<Double>> e : pairToPartials.entrySet()) {
            double score = Step4_FinalSum.runSumReduce(e.getKey(), e.getValue(), pathTotals);
            finalScores.add(new Step3_Similarity.Pair<>(e.getKey(), score));
        }

        // ---- 6. Print Inference Rules ----
        System.out.println("\n========== Inferred Rules (PathA <-> PathB : Similarity) ==========");
        for (Step3_Similarity.Pair<String, Double> p : finalScores) {
            int tab = p.first.indexOf('\t');
            String pathA = tab > 0 ? p.first.substring(0, tab) : p.first;
            String pathB = tab > 0 ? p.first.substring(tab + 1) : "";
            System.out.println("  " + pathA + "  <->  " + pathB + "  :  " + String.format("%.4f", p.second));
            System.out.println("    ( " + formatRule(pathA) + "  <->  " + formatRule(pathB) + " )");
        }
        System.out.println("\n========== End ==========");
    }

    /** Format path "verb -> relation" as "X verb Y" for display. */
    private static String formatRule(String path) {
        if (path == null || path.isEmpty()) return path;
        int i = path.indexOf(" -> ");
        if (i <= 0) return "X " + path + " Y";
        String verb = path.substring(0, i);
        return "X " + verb + " Y";
    }
}
