/**
 * DIRT Algorithm - Discovery of Inference Rules from Text
 * 
 * Repository: https://github.com/BarShuv/aws3_map_reduce_learning
 * 
 * Porter Stemmer implementation for word stemming.
 * This class provides functionality to reduce words to their root forms.
 * Implements a simplified version of the Porter Stemming Algorithm.
 */
public class PorterStemmer {
    
    /**
     * Stems a word using the Porter Stemming Algorithm.
     * This is a simplified implementation that handles common cases.
     * 
     * @param word the word to stem
     * @return the stemmed form of the word
     */
    public String stem(String word) {
        if (word == null || word.isEmpty()) {
            return word;
        }
        
        String w = word.toLowerCase();
        
        // Handle common verb endings
        // -ing endings (walking -> walk, waiting -> wait)
        if (w.length() > 4 && w.endsWith("ing")) {
            // Check for double consonant before -ing (e.g., running -> run)
            if (w.length() > 5 && isConsonant(w, w.length() - 4) && 
                w.charAt(w.length() - 4) == w.charAt(w.length() - 5)) {
                w = w.substring(0, w.length() - 4);
            } else if (w.endsWith("ying")) {
                w = w.substring(0, w.length() - 3) + "y";
            } else {
                w = w.substring(0, w.length() - 3);
            }
        }
        // -ed endings (waited -> wait, solved -> solve)
        else if (w.length() > 3 && w.endsWith("ed")) {
            if (w.endsWith("ied")) {
                w = w.substring(0, w.length() - 3) + "y";
            } else if (w.length() > 4 && isConsonant(w, w.length() - 3) && 
                       w.charAt(w.length() - 3) == w.charAt(w.length() - 4)) {
                // Double consonant (e.g., stopped -> stop)
                w = w.substring(0, w.length() - 3);
            } else {
                w = w.substring(0, w.length() - 2);
            }
        }
        // -s endings (waits -> wait, eats -> eat)
        else if (w.length() > 2 && w.endsWith("s") && !w.endsWith("ss") && 
                 !w.endsWith("us") && !w.endsWith("is")) {
            // Don't stem if word ends with -ss, -us, -is
            if (w.endsWith("ies")) {
                w = w.substring(0, w.length() - 3) + "y";
            } else if (w.endsWith("es") && w.length() > 3) {
                char beforeE = w.charAt(w.length() - 3);
                if (beforeE == 's' || beforeE == 'x' || beforeE == 'z' || 
                    beforeE == 'h' || beforeE == 'o') {
                    w = w.substring(0, w.length() - 2);
                } else {
                    w = w.substring(0, w.length() - 1);
                }
            } else if (!w.endsWith("ss")) {
                w = w.substring(0, w.length() - 1);
            }
        }
        
        return w;
    }
    
    /**
     * Checks if a character at a given position is a consonant.
     * 
     * @param word the word
     * @param pos the position to check
     * @return true if the character is a consonant
     */
    private boolean isConsonant(String word, int pos) {
        if (pos < 0 || pos >= word.length()) {
            return false;
        }
        char c = word.charAt(pos);
        return c != 'a' && c != 'e' && c != 'i' && c != 'o' && c != 'u';
    }
    
    /**
     * Stems a word from a Node object.
     * 
     * @param node the node containing the word to stem
     * @return the stemmed form of the word
     */
    public String stem(Node node) {
        if (node == null) {
            return "";
        }
        return stem(node.getWord());
    }
}

