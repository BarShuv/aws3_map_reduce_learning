/**
 * DIRT Algorithm - Discovery of Inference Rules from Text
 * 
 * Repository: https://github.com/BarShuv/aws3_map_reduce_learning
 * 
 * Main test class for the Extraction phase of the DIRT algorithm.
 * This class demonstrates parsing and path extraction from syntactic N-gram format.
 */
import java.util.List;

public class Main {
    
    /**
     * Tests a sentence with the given description and input line.
     * 
     * @param description a description of what this test case validates
     * @param line the input line in syntactic N-gram format
     */
    private static void testSentence(String description, String line) {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("Test: " + description);
        System.out.println("=".repeat(60));
        System.out.println("Input: " + line);
        
        PathExtractor extractor = new PathExtractor();
        List<String> paths = extractor.processLine(line);
        
        System.out.println("\nExtracted paths:");
        if (paths.isEmpty()) {
            System.out.println("  [No paths found]");
        } else {
            for (String path : paths) {
                System.out.println("  " + path);
            }
        }
    }
    
    public static void main(String[] args) {
        System.out.println("DIRT Algorithm - Extraction Phase Test Suite");
        System.out.println("============================================\n");
        
        // Test 1: The Standard Case (Preposition)
        testSentence("The Standard Case (Preposition)",
            "waits/VBZ/ROOT/0\tfor/IN/prep/1\tMary/NNP/pobj/2\t153");
        
        // Test 2: Direct Object Case (No Preposition)
        testSentence("Direct Object Case (No Preposition)",
            "John/NNP/nsubj/1\teats/VBZ/ROOT/0\tapples/NNS/dobj/1");
        
        // Test 3: Auxiliary Verb Filter
        testSentence("Auxiliary Verb Filter",
            "John/NNP/nsubj/2\tis/VBZ/aux/2\twalking/VBG/ROOT/0");
        
        // Test 4: Stemming Check
        testSentence("Stemming Check",
            "He/PRP/nsubj/1\tsolved/VBD/ROOT/0\tit/PRP/dobj/1");
        
        System.out.println("\n" + "=".repeat(60));
        System.out.println("Test Suite Complete");
        System.out.println("=".repeat(60));
    }
}

