/**
 * DIRT Algorithm - Step 4: Summing Scores
 *
 * Sums all partial scores for each (PathA, PathB) to get the numerator of Lin's similarity.
 * Optionally normalizes by path totals (Lin's denominator) if a path-totals file is provided.
 *
 * Input format (from Step 3): PathA \t PathB \t PartialScore
 * Output: PathA \t PathB \t SimilarityScore
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step4_FinalSum {

    public static final String SEP = "\t";
    /** Optional: set to path of file "path\ttotalMI" per line for Lin normalization */
    public static final String PATH_TOTALS_FILE = "dirt.path.totals.file";

    /**
     * Sum partial scores for one (PathA, PathB). If pathTotals is non-null, normalizes by Lin denominator.
     * Returns numerator sum, or similarity = sum / (totalA + totalB) when pathTotals provided.
     */
    public static double runSumReduce(String pathPairKey, Iterable<Double> partialScores, Map<String, Double> pathTotals) {
        double sum = 0;
        for (Double d : partialScores) sum += d;
        if (pathTotals != null && !pathTotals.isEmpty()) {
            int idx = pathPairKey.indexOf(SEP);
            if (idx > 0) {
                String pathA = pathPairKey.substring(0, idx);
                String pathB = pathPairKey.substring(idx + 1);
                Double tA = pathTotals.get(pathA);
                Double tB = pathTotals.get(pathB);
                if (tA != null && tB != null) {
                    double denom = tA + tB;
                    if (denom > 0) sum = sum / denom;
                }
            }
        }
        return sum;
    }

    /**
     * Pass-through: (PathA, PathB, PartialScore) -> same key (PathA, PathB), value = PartialScore.
     */
    public static class SumMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private final Text pathPair = new Text();
        private final DoubleWritable score = new DoubleWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(SEP, -1);
            if (parts.length < 3) return;

            String pathA = parts[0];
            String pathB = parts[1];
            try {
                double partial = Double.parseDouble(parts[2].trim());
                pathPair.set(pathA + SEP + pathB);
                score.set(partial);
                context.write(pathPair, score);
            } catch (NumberFormatException e) {
                // skip
            }
        }
    }

    /**
     * Sums partial scores per (PathA, PathB). If path totals are loaded, outputs
     * Lin similarity = numerator / (total_A + total_B); otherwise outputs numerator.
     */
    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final DoubleWritable result = new DoubleWritable();
        private Map<String, Double> pathTotals = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Path totals: from distributed cache (local file) or from config path
            pathTotals = new HashMap<>();
            try {
                Path[] cacheFiles = context.getLocalCacheFiles();
                if (cacheFiles != null && cacheFiles.length > 0) {
                    try (BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()))) {
                        loadPathTotals(br);
                    }
                    return;
                }
                String totalsPath = context.getConfiguration().get(PATH_TOTALS_FILE);
                if (totalsPath != null && !totalsPath.isEmpty()) {
                    try (BufferedReader br = new BufferedReader(new FileReader(totalsPath))) {
                        loadPathTotals(br);
                    }
                }
            } catch (Exception e) {
                // optional: ignore if no path totals
            }
        }

        private void loadPathTotals(BufferedReader br) throws IOException {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                int i = line.lastIndexOf(SEP);
                if (i <= 0) continue;
                String path = line.substring(0, i);
                try {
                    double total = Double.parseDouble(line.substring(i + 1).trim());
                    pathTotals.put(path, total);
                } catch (NumberFormatException e) { /* skip */ }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable d : values) sum += d.get();

            if (!pathTotals.isEmpty()) {
                String k = key.toString();
                int idx = k.indexOf(SEP);
                if (idx > 0) {
                    String pathA = k.substring(0, idx);
                    String pathB = k.substring(idx + 1);
                    Double tA = pathTotals.get(pathA);
                    Double tB = pathTotals.get(pathB);
                    if (tA != null && tB != null) {
                        double denom = tA + tB;
                        if (denom > 0) sum = sum / denom;
                    }
                }
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static Job createJob(Configuration conf, Path inputPath, Path outputPath) throws IOException {
        Job job = Job.getInstance(conf, "Step4_FinalSum");
        job.setJarByClass(Step4_FinalSum.class);
        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
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
            System.err.println("Usage: Step4_FinalSum <input> <output> [path_totals_file]");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = createJob(conf, new Path(args[0]), new Path(args[1]));
        if (args.length >= 3) {
            conf.set(PATH_TOTALS_FILE, args[2]);
            job.addCacheFile(new Path(args[2]).toUri());
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
