/**
 * Composite key for (Path, Slot, Word) used in Step 1 counting.
 * Implements WritableComparable for Hadoop serialization and grouping.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TripleKey implements WritableComparable<TripleKey> {

    private String path;
    private String slot;
    private String word;

    public TripleKey() {
        this("", "", "");
    }

    public TripleKey(String path, String slot, String word) {
        this.path = path != null ? path : "";
        this.slot = slot != null ? slot : "";
        this.word = word != null ? word : "";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(path);
        out.writeUTF(slot);
        out.writeUTF(word);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        path = in.readUTF();
        slot = in.readUTF();
        word = in.readUTF();
    }

    @Override
    public int compareTo(TripleKey o) {
        int c = path.compareTo(o.path);
        if (c != 0) return c;
        c = slot.compareTo(o.slot);
        if (c != 0) return c;
        return word.compareTo(o.word);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TripleKey)) return false;
        TripleKey o = (TripleKey) obj;
        return path.equals(o.path) && slot.equals(o.slot) && word.equals(o.word);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * path.hashCode() + slot.hashCode()) + word.hashCode();
    }

    public String getPath() { return path; }
    public String getSlot() { return slot; }
    public String getWord() { return word; }

    public void setPath(String path) { this.path = path != null ? path : ""; }
    public void setSlot(String slot) { this.slot = slot != null ? slot : ""; }
    public void setWord(String word) { this.word = word != null ? word : ""; }

    @Override
    public String toString() {
        return path + "\t" + slot + "\t" + word;
    }
}
