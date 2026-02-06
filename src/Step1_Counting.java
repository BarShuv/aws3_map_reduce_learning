/**
 * DIRT Algorithm - Step 1: Parsing & Counting
 *
 * MapReduce job that parses raw Biarcs-style lines, extracts (Path, Slot, Word) triples
 * via PathExtractor, and counts occurrences. Output: Path \t Slot \t Word \t Count.
 *
 * Triple convention: Path = "verb -> relation", Slot = "X" (subject) or "Y" (object), Word = filler.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step1_Counting {

    private static final String PATH_SEP = " -> ";
    private static final IntWritable ONE = new IntWritable(1);

    /**
     * Returns "X" for subject-like relations (nsubj, nsubjpass, csubj), "Y" for object-like (dobj, pobj, etc.).
     */
    private static boolean isSubjectSlot(String relation) {
        if (relation == null) return false;
        String r = relation.toLowerCase();
        return r.startsWith("nsubj") || r.equals("csubj") || r.equals("csubjpass");
    }

    /**
     * Parses a path string "verb -> relation -> noun" into TripleKey:
     * Path = "verb -> relation", Slot = "X" or "Y", Word = noun.
     * Example: "eat -> dobj -> apple" -> Path="eat -> dobj", Slot="Y", Word="apple".
     */
    public static void parsePath(String pathStr, TripleKey out) {
        if (pathStr == null || pathStr.isEmpty()) {
            out.setPath("");
            out.setSlot("");
            out.setWord("");
            return;
        }
        String[] parts = pathStr.split(PATH_SEP, -1);
        if (parts.length >= 3) {
            String verb = parts[0].trim();
            String relation = parts[1].trim();
            String word = parts[2].trim();
            out.setPath(verb + PATH_SEP + relation);
            out.setSlot(isSubjectSlot(relation) ? "X" : "Y");
            out.setWord(word);
        } else {
            out.setPath(pathStr.trim());
            out.setSlot("");
            out.setWord("");
        }
    }

    /**
     * Core map logic: same as PathCountMapper. Given a line, returns (TripleKey, 1) for each path.
     * Used by the Mapper and by standalone tests without Hadoop Context.
     */
    public static List<MapEntry> runMapLogic(String line) {
        List<MapEntry> out = new ArrayList<>();
        PathExtractor extractor = new PathExtractor();
        TripleKey key = new TripleKey();
        for (String pathStr : extractor.processLine(line)) {
            parsePath(pathStr, key);
            out.add(new MapEntry(new TripleKey(key.getPath(), key.getSlot(), key.getWord()), 1));
        }
        return out;
    }

    /**
     * Core reduce/combiner logic: sums counts for a key. Used by Reducer and standalone tests.
     */
    public static int runReduceLogic(TripleKey key, Iterable<IntWritable> values) {
        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }
        return sum;
    }

    /** Simple (key, value) pair for local test / map output. */
    public static final class MapEntry {
        public final TripleKey key;
        public final int value;
        public MapEntry(TripleKey key, int value) { this.key = key; this.value = value; }
    }

    /**
     * Mapper: reads a line (raw Biarcs format), uses PathExtractor to get paths,
     * emits (TripleKey(path, slot, word), 1) for each path.
     */
    public static class PathCountMapper extends Mapper<LongWritable, Text, TripleKey, IntWritable> {
        private boolean firstFailureLogged = false;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            List<MapEntry> entries = runMapLogic(line);
            if (entries.isEmpty()) {
                if (!firstFailureLogged) {
                    System.err.println("[Step1_Counting] First line with no emitted paths (sample): " + (line.length() > 200 ? line.substring(0, 200) + "..." : line));
                    firstFailureLogged = true;
                }
                return;
            }
            for (MapEntry e : entries) {
                context.write(e.key, ONE);
            }
        }
    }

    /**
     * Combiner: sums counts locally (same logic as reducer). REQUIRED for efficiency.
     */
    public static class PathCountCombiner extends Reducer<TripleKey, IntWritable, TripleKey, IntWritable> {
        private final IntWritable outVal = new IntWritable();

        @Override
        protected void reduce(TripleKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            outVal.set(runReduceLogic(key, values));
            context.write(key, outVal);
        }
    }

    /**
     * Reducer: sums counts and emits Path \t Slot \t Word \t Count.
     */
    public static class PathCountReducer extends Reducer<TripleKey, IntWritable, TripleKey, IntWritable> {
        private final IntWritable outVal = new IntWritable();

        @Override
        protected void reduce(TripleKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            outVal.set(runReduceLogic(key, values));
            context.write(key, outVal);
        }
    }

    /**
     * Builds and returns the Step 1 MapReduce job with Mapper, Combiner, and Reducer set.
     */
    public static Job createJob(Configuration conf, Path inputPath, Path outputPath) throws IOException {
        Job job = Job.getInstance(conf, "Step1_Counting");
        job.setJarByClass(Step1_Counting.class);
        job.setMapperClass(PathCountMapper.class);
        job.setCombinerClass(PathCountCombiner.class);
        job.setReducerClass(PathCountReducer.class);
        job.setMapOutputKeyClass(TripleKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(TripleKey.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, inputPath);
        TextOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Step1_Counting <input> <output>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = createJob(conf, new Path(args[0]), new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
