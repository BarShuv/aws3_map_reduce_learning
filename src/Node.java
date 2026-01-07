/**
 * DIRT Algorithm - Discovery of Inference Rules from Text
 * 
 * Repository: https://github.com/BarShuv/aws3_map_reduce_learning
 * 
 * This class represents a node in the dependency parse tree.
 * Each node contains a word, its part-of-speech tag, dependency label,
 * and the index of its head node.
 */
public class Node {
    private String word;
    private String posTag;
    private String dependencyLabel;
    private int headIndex;
    private int index;

    /**
     * Constructs a new Node with the specified attributes.
     * 
     * @param word the word token
     * @param posTag the part-of-speech tag
     * @param dependencyLabel the dependency relation label
     * @param headIndex the index of the head node (parent)
     * @param index the index of this node in the sentence
     */
    public Node(String word, String posTag, String dependencyLabel, int headIndex, int index) {
        this.word = word;
        this.posTag = posTag;
        this.dependencyLabel = dependencyLabel;
        this.headIndex = headIndex;
        this.index = index;
    }

    public String getWord() {
        return word;
    }

    public String getPosTag() {
        return posTag;
    }

    public String getDependencyLabel() {
        return dependencyLabel;
    }

    public int getHeadIndex() {
        return headIndex;
    }

    public int getIndex() {
        return index;
    }

    /**
     * Checks if this node is a verb (POS tag starts with "VB").
     * 
     * @return true if the node is a verb
     */
    public boolean isVerb() {
        return posTag != null && posTag.startsWith("VB");
    }

    /**
     * Checks if this node is a noun (POS tag starts with "NN").
     * 
     * @return true if the node is a noun
     */
    public boolean isNoun() {
        return posTag != null && posTag.startsWith("NN");
    }

    /**
     * Checks if this node is a preposition (POS tag is "IN" or "TO").
     * 
     * @return true if the node is a preposition
     */
    public boolean isPreposition() {
        return posTag != null && (posTag.equals("IN") || posTag.equals("TO"));
    }

    /**
     * Checks if this verb is an auxiliary verb that should be filtered out.
     * 
     * @return true if the verb is an auxiliary (is, are, was, were, be, been, am)
     */
    public boolean isAuxiliaryVerb() {
        if (!isVerb()) {
            return false;
        }
        String lowerWord = word.toLowerCase();
        return lowerWord.equals("is") || lowerWord.equals("are") || 
               lowerWord.equals("was") || lowerWord.equals("were") || 
               lowerWord.equals("be") || lowerWord.equals("been") || 
               lowerWord.equals("am");
    }

    @Override
    public String toString() {
        return String.format("%s/%s/%s/%d", word, posTag, dependencyLabel, headIndex);
    }
}

