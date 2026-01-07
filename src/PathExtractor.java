/**
 * DIRT Algorithm - Discovery of Inference Rules from Text
 * 
 * Repository: https://github.com/BarShuv/aws3_map_reduce_learning
 * 
 * This class handles parsing of syntactic N-gram format and extraction
 * of dependency paths from dependency parse trees.
 */
import java.util.*;

public class PathExtractor {
    private PorterStemmer stemmer;

    /**
     * Constructs a new PathExtractor with a PorterStemmer instance.
     */
    public PathExtractor() {
        this.stemmer = new PorterStemmer();
    }

    /**
     * Parses a single line of syntactic N-gram format into a list of Nodes.
     * 
     * Format: word/POS-tag/dependency-label/head-index (tab-separated)
     * 
     * @param line the input line to parse
     * @return a list of Node objects representing the dependency parse
     */
    public List<Node> parseLine(String line) {
        List<Node> nodes = new ArrayList<>();
        
        if (line == null || line.trim().isEmpty()) {
            return nodes;
        }
        
        // Split by tabs to get individual tokens
        String[] tokens = line.trim().split("\t+");
        
        // The last token might be a frequency count, so we'll ignore it if it's numeric
        int endIndex = tokens.length;
        if (tokens.length > 0 && tokens[tokens.length - 1].matches("\\d+")) {
            endIndex = tokens.length - 1;
        }
        
        for (int i = 0; i < endIndex; i++) {
            String token = tokens[i].trim();
            if (token.isEmpty()) {
                continue;
            }
            
            // Split by '/' to get word, POS tag, dependency label, and head index
            String[] parts = token.split("/");
            if (parts.length >= 4) {
                try {
                    String word = parts[0];
                    String posTag = parts[1];
                    String dependencyLabel = parts[2];
                    int headIndex = Integer.parseInt(parts[3]);
                    
                    Node node = new Node(word, posTag, dependencyLabel, headIndex, i);
                    nodes.add(node);
                } catch (NumberFormatException e) {
                    // Skip malformed tokens
                    System.err.println("Warning: Could not parse token: " + token);
                }
            }
        }
        
        return nodes;
    }

    /**
     * Builds a map from head index to list of child nodes for efficient lookup.
     * Auto-detects whether headIndex is 0-based or 1-based by trying both and seeing which works.
     * 
     * @param nodes the list of nodes
     * @return a map where keys are 0-based node indices and values are lists of child nodes
     */
    private Map<Integer, List<Node>> buildChildMap(List<Node> nodes) {
        // Try both formats and see which produces more valid parent-child relationships
        int zeroBasedCount = 0;
        int oneBasedCount = 0;
        
        for (Node node : nodes) {
            int headIndex = node.getHeadIndex();
            if (headIndex == 0 && "ROOT".equals(node.getDependencyLabel())) {
                continue;
            }
            
            // Check 0-based: headIndex directly as node index
            if (headIndex >= 0 && headIndex < nodes.size() && headIndex != node.getIndex()) {
                zeroBasedCount++;
            }
            
            // Check 1-based: headIndex as position, convert to index
            int oneBasedParent = (headIndex > 0) ? headIndex - 1 : -1;
            if (oneBasedParent >= 0 && oneBasedParent < nodes.size() && oneBasedParent != node.getIndex()) {
                oneBasedCount++;
            }
        }
        
        // Use the format that produces more valid relationships
        // If tied, prefer 0-based (more common in dependency parsing)
        boolean isOneBased = oneBasedCount > zeroBasedCount;
        
        Map<Integer, List<Node>> childMap = new HashMap<>();
        
        for (Node node : nodes) {
            int headIndex = node.getHeadIndex();
            int parentNodeIndex = -1;
            
            if (isOneBased) {
                // Convert 1-based headIndex to 0-based node index
                // headIndex=0 means ROOT (no parent), headIndex=1 means first word (index 0), etc.
                if (headIndex == 0 && "ROOT".equals(node.getDependencyLabel())) {
                    continue; // ROOT has no parent
                }
                parentNodeIndex = (headIndex > 0) ? headIndex - 1 : -1;
            } else {
                // 0-based: headIndex directly refers to the parent node's index
                if (headIndex == 0 && "ROOT".equals(node.getDependencyLabel())) {
                    continue; // ROOT has no parent
                }
                if (headIndex >= 0 && headIndex < nodes.size() && headIndex != node.getIndex()) {
                    parentNodeIndex = headIndex;
                }
            }
            
            if (parentNodeIndex >= 0 && parentNodeIndex < nodes.size()) {
                childMap.putIfAbsent(parentNodeIndex, new ArrayList<>());
                childMap.get(parentNodeIndex).add(node);
            }
        }
        
        return childMap;
    }

    /**
     * Extracts dependency paths from a list of nodes.
     * Finds head verbs and their noun children, handling both direct and preposition cases.
     * 
     * @param nodes the list of nodes representing the dependency parse
     * @return a list of extracted paths as strings
     */
    public List<String> extractPaths(List<Node> nodes) {
        List<String> paths = new ArrayList<>();
        
        if (nodes == null || nodes.isEmpty()) {
            return paths;
        }
        
        // Build a map of head index to children for efficient lookup
        Map<Integer, List<Node>> childMap = buildChildMap(nodes);
        
        // Find all head verbs (verbs that are not auxiliary)
        for (Node node : nodes) {
            if (node.isVerb() && !node.isAuxiliaryVerb()) {
                // Check if this verb has children
                List<Node> children = childMap.get(node.getIndex());
                if (children != null) {
                    // Look for noun children
                    for (Node child : children) {
                        // Case A: Direct verb -> noun
                        if (child.isNoun()) {
                            String path = formatPath(node, child, null);
                            paths.add(path);
                        }
                        // Case B: Verb -> preposition -> noun
                        else if (child.isPreposition()) {
                            // Check if the preposition has noun children
                            List<Node> prepChildren = childMap.get(child.getIndex());
                            if (prepChildren != null) {
                                for (Node nounChild : prepChildren) {
                                    if (nounChild.isNoun()) {
                                        String path = formatPath(node, nounChild, child);
                                        paths.add(path);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return paths;
    }

    /**
     * Formats a dependency path as a readable string.
     * Uses stemmed words and a simple format: verb -> relation -> noun
     * 
     * @param verb the head verb node
     * @param noun the noun node
     * @param preposition the preposition node (null for direct paths)
     * @return a formatted string representation of the path
     */
    private String formatPath(Node verb, Node noun, Node preposition) {
        StringBuilder sb = new StringBuilder();
        
        // Add stemmed verb
        String verbStem = stemmer.stem(verb);
        sb.append(verbStem);
        
        if (preposition != null) {
            // Case B: Verb -> Preposition -> Noun
            sb.append(" -> ").append(preposition.getWord());
        } else {
            // Case A: Verb -> Noun (direct) - use the noun's dependency label
            sb.append(" -> ").append(noun.getDependencyLabel());
        }
        
        // Add stemmed noun
        String nounStem = stemmer.stem(noun);
        sb.append(" -> ").append(nounStem);
        
        return sb.toString();
    }

    /**
     * Processes a single line: parses it and extracts paths.
     * 
     * @param line the input line to process
     * @return a list of extracted paths
     */
    public List<String> processLine(String line) {
        List<Node> nodes = parseLine(line);
        return extractPaths(nodes);
    }

    /**
     * Stems a word using the Porter Stemmer.
     * This method is available for future use when stemming is needed.
     * 
     * @param word the word to stem
     * @return the stemmed form of the word
     */
    public String stemWord(String word) {
        return stemmer.stem(word);
    }
}

