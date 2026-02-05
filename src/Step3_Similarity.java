/**
 * DIRT Algorithm - Step 3: Similarity via Inverted Index
 *
 * Uses an inverted index: map features (Slot, Word) to lists of (Path, MI).
 * Reducer receives a feature and all paths that share it, then emits
 * (PathA, PathB) -> (MI_A + MI_B) for each pair (numerator part of Lin's similarity).
 *
 * Input format (from Step 2): Path \t Slot \t Word \t MI  (one line per triple)
 * Output: PathA \t PathB \t PartialScore  (PartialScore = MI_A + MI_B for this feature)
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step3_Similarity {

    public static final String SEP = "\t";

    /**
     * Mapper: Input (Path, Slot, Word, MI).
     * Key = Feature = (Slot, Word); Value = (Path, MI).
     */
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
        private final Text featureKey = new Text();
        private final Text pathAndMI = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(SEP, -1);
            if (parts.length < 4) return;

            String path = parts[0];
            String slot = parts[1];
            String word = parts[2];
            String miStr = parts[3];

            featureKey.set(slot + SEP + word);
            pathAndMI.set(path + SEP + miStr);
            context.write(featureKey, pathAndMI);
        }
    }

    /**
     * Reducer: For each feature (Slot, Word), receives list of (Path, MI).
     * Emits for every pair (PathA, PathB) the partial score MI_A + MI_B.
     * Uses canonical ordering (PathA < PathB) so each pair is emitted once.
     */
    public static class PairEmitReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private final Text pathPairKey = new Text();
        private final DoubleWritable partialScore = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> pathMIList = new ArrayList<>();
            for (Text t : values) {
                String s = t.toString();
                if (s != null && !s.isEmpty()) pathMIList.add(s);
            }

            if (pathMIList.size() < 2) return;

            // Parse (Path, MI) and compute Cartesian product for pairs
            List<PathMI> list = new ArrayList<>();
            for (String s : pathMIList) {
                int lastTab = s.lastIndexOf(SEP);
                if (lastTab <= 0) continue;
                String path = s.substring(0, lastTab);
                String miStr = s.substring(lastTab + 1);
                try {
                    double mi = Double.parseDouble(miStr.trim());
                    list.add(new PathMI(path, mi));
                } catch (NumberFormatException e) {
                    // skip malformed
                }
            }

            for (int i = 0; i < list.size(); i++) {
                for (int j = i + 1; j < list.size(); j++) {
                    PathMI a = list.get(i);
                    PathMI b = list.get(j);
                    String pathA = a.path;
                    String pathB = b.path;
                    if (pathA.equals(pathB)) continue;
                    // Canonical key: smaller lexicographically first
                    String first = pathA.compareTo(pathB) <= 0 ? pathA : pathB;
                    String second = pathA.compareTo(pathB) <= 0 ? pathB : pathA;
                    pathPairKey.set(first + SEP + second);
                    partialScore.set(a.mi + b.mi);
                    context.write(pathPairKey, partialScore);
                }
            }
        }

        private static class PathMI {
            final String path;
            final double mi;
            PathMI(String path, double mi) { this.path = path; this.mi = mi; }
        }
    }

    public static Job createJob(Configuration conf, Path inputPath, Path outputPath) throws IOException {
        Job job = Job.getInstance(conf, "Step3_Similarity_InvertedIndex");
        job.setJarByClass(Step3_Similarity.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(PairEmitReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, inputPath);
        TextOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Step3_Similarity <input> <output>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = createJob(conf, new Path(args[0]), new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
