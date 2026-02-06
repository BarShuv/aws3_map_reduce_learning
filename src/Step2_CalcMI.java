/**
 * DIRT Algorithm - Step 2: MI Calculation
 *
 * Computes Mutual Information for each (Path, Slot, Word) triple.
 * MI(p, s, w) = log( (count(p,s,w) * |Total|) / (count(p,s) * count(s,w)) )
 *
 * Two-phase approach:
 * - Phase 1 (Marginals): Aggregate count(p,s), count(s,w), and |Total| from triple counts.
 * - Phase 2 (MI): Load marginals from cache, compute MI per triple, emit (Path, Slot, Word, MI).
 *
 * Input format (from Step 1): Path \t Slot \t Word \t Count  (one line per triple)
 * Output: Path \t Slot \t Word \t MI
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step2_CalcMI {

    public static final String SEP = "\t";
    public static final String PREFIX_PS = "PS:";
    public static final String PREFIX_SW = "SW:";
    public static final String PREFIX_TOTAL = "TOTAL";

    /** One triple with count: (Path, Slot, Word) -> Count. */
    public static final class TripleCount {
        public final String path, slot, word;
        public final long count;
        public TripleCount(String path, String slot, String word, long count) {
            this.path = path; this.slot = slot; this.word = word; this.count = count;
        }
    }

    /** Marginals: count(p,s), count(s,w), and |Total|. */
    public static final class Marginals {
        public final Map<String, Long> countPS;
        public final Map<String, Long> countSW;
        public final long total;
        public Marginals(Map<String, Long> countPS, Map<String, Long> countSW, long total) {
            this.countPS = countPS; this.countSW = countSW; this.total = total;
        }
    }

    /**
     * Compute marginals from a list of (path, slot, word, count) triples.
     * Same logic as MarginalsMapper + MarginalsReducer.
     */
    public static Marginals computeMarginalsFromTriples(List<TripleCount> triples) {
        Map<String, Long> countPS = new HashMap<>();
        Map<String, Long> countSW = new HashMap<>();
        long total = 0;
        for (TripleCount t : triples) {
            long c = t.count;
            countPS.merge(t.path + SEP + t.slot, c, Long::sum);
            countSW.merge(t.slot + SEP + t.word, c, Long::sum);
            total += c;
        }
        return new Marginals(countPS, countSW, total);
    }

    /**
     * Calculate MI(p,s,w) = log( (count(p,s,w) * total) / (count(p,s) * count(s,w)) ).
     * Returns Double.NaN if any denominator is missing or zero.
     */
    public static double calculateMI(long countPSW, long countPS, long countSW, long total) {
        if (total <= 0 || countPS <= 0 || countSW <= 0) return Double.NaN;
        double ratio = (double) (countPSW * total) / (double) (countPS * countSW);
        if (ratio <= 0) return Double.NaN;
        return Math.log(ratio);
    }

    /**
     * Compute MI for one triple using precomputed marginals. Returns Double.NaN if not computable.
     */
    public static double calculateMIForTriple(TripleCount t, Marginals m) {
        Long ps = m.countPS.get(t.path + SEP + t.slot);
        Long sw = m.countSW.get(t.slot + SEP + t.word);
        if (ps == null || sw == null) return Double.NaN;
        return calculateMI(t.count, ps, sw, m.total);
    }

    // ---------- Phase 1: Compute marginals ----------

    public static class MarginalsMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final Text outKey = new Text();
        private final LongWritable outVal = new LongWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split(SEP, -1);
            if (parts.length < 4) return;

            String p = parts[0];
            String s = parts[1];
            String w = parts[2];
            long c;
            try {
                c = Long.parseLong(parts[3].trim());
            } catch (NumberFormatException e) {
                return;
            }

            outVal.set(c);
            outKey.set(PREFIX_PS + p + SEP + s);
            context.write(outKey, outVal);
            outKey.set(PREFIX_SW + s + SEP + w);
            context.write(outKey, outVal);
            outKey.set(PREFIX_TOTAL);
            context.write(outKey, outVal);
        }
    }

    public static class MarginalsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable outVal = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v : values) sum += v.get();
            outVal.set(sum);
            context.write(key, outVal);
        }
    }

    // ---------- Phase 2: Compute MI using marginals ----------

    public static class MIMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private final Text outKey = new Text();
        private final DoubleWritable outVal = new DoubleWritable();
        private Map<String, Long> countPS = new HashMap<>();
        private Map<String, Long> countSW = new HashMap<>();
        private long total = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] cacheFiles = context.getLocalCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) return;
            try (BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    int i = line.lastIndexOf(SEP);
                    if (i < 0) continue;
                    String keyPart = line.substring(0, i);
                    long val;
                    try {
                        val = Long.parseLong(line.substring(i + 1).trim());
                    } catch (NumberFormatException e) {
                        continue;
                    }
                    if (keyPart.startsWith(PREFIX_TOTAL)) {
                        total = val;
                    } else if (keyPart.startsWith(PREFIX_PS)) {
                        countPS.put(keyPart.substring(PREFIX_PS.length()), val);
                    } else if (keyPart.startsWith(PREFIX_SW)) {
                        countSW.put(keyPart.substring(PREFIX_SW.length()), val);
                    }
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split(SEP, -1);
            if (parts.length < 4) return;

            String p = parts[0];
            String s = parts[1];
            String w = parts[2];
            long c;
            try {
                c = Long.parseLong(parts[3].trim());
            } catch (NumberFormatException e) {
                return;
            }

            Long ps = countPS.get(p + SEP + s);
            Long sw = countSW.get(s + SEP + w);
            if (ps == null || sw == null || total <= 0 || ps <= 0 || sw <= 0) return;

            double ratio = (double) (c * total) / (double) (ps * sw);
            if (ratio <= 0) return;
            double mi = Math.log(ratio);
            outKey.set(p + SEP + s + SEP + w);
            outVal.set(mi);
            context.write(outKey, outVal);
        }
    }

    /**
     * Identity reducer: one (p,s,w) per key, so we just pass through.
     * Output format: Path \t Slot \t Word \t MI (single line per key).
     */
    public static class MIReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final Text outKey = new Text();
        private final DoubleWritable outVal = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable v : values) {
                outKey.set(key.toString());
                outVal.set(v.get());
                context.write(outKey, outVal);
                return;
            }
        }
    }

    public static Job createMarginalsJob(Configuration conf, Path inputPath, Path marginalsPath) throws IOException {
        Job job = Job.getInstance(conf, "Step2_Marginals");
        job.setJarByClass(Step2_CalcMI.class);
        job.setMapperClass(MarginalsMapper.class);
        job.setReducerClass(MarginalsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, inputPath);
        TextOutputFormat.setOutputPath(job, marginalsPath);
        job.setNumReduceTasks(1); // single file for distributed cache
        return job;
    }

    public static Job createMIJob(Configuration conf, Path inputPath, Path marginalsPath, Path outputPath) throws IOException {
        Job job = Job.getInstance(conf, "Step2_CalcMI");
        job.setJarByClass(Step2_CalcMI.class);
        job.setMapperClass(MIMapper.class);
        job.setReducerClass(MIReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, inputPath);
        TextOutputFormat.setOutputPath(job, outputPath);
        // Marginals file: use single file from marginals path (part-r-00000)
        Path marginalsFile = new Path(marginalsPath, "part-r-00000");
        job.addCacheFile(marginalsFile.toUri());
        return job;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: Step2_CalcMI <input_triples> <marginals_output_dir> <mi_output_dir>");
            System.exit(1);
        }
        Path inputPath = new Path(args[0]);
        Path marginalsPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        Configuration conf = new Configuration();

        Job job1 = createMarginalsJob(conf, inputPath, marginalsPath);
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }
        Job job2 = createMIJob(conf, inputPath, marginalsPath, outputPath);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
