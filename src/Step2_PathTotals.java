/**
 * DIRT Algorithm - Path Totals for Lin's Similarity
 *
 * Reads Step 2 output (Path \t Slot \t Word \t MI) and sums MI per Path.
 * Output: Path \t TotalMI  (used as denominator in Lin: total_MI(p1) + total_MI(p2))
 *
 * Usage: run after Step2_CalcMI; pass output to Step4_FinalSum as optional third argument
 * for normalized similarity scores.
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step2_PathTotals {

    public static final String SEP = "\t";

    public static class PathTotalsMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private final Text pathKey = new Text();
        private final DoubleWritable miVal = new DoubleWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split(SEP, -1);
            if (parts.length < 4) return;
            try {
                double mi = Double.parseDouble(parts[3].trim());
                pathKey.set(parts[0]);
                miVal.set(mi);
                context.write(pathKey, miVal);
            } catch (NumberFormatException e) { /* skip */ }
        }
    }

    public static class PathTotalsReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final DoubleWritable total = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable v : values) sum += v.get();
            total.set(sum);
            context.write(key, total);
        }
    }

    public static Job createJob(Configuration conf, Path inputPath, Path outputPath) throws IOException {
        Job job = Job.getInstance(conf, "Step2_PathTotals");
        job.setJarByClass(Step2_PathTotals.class);
        job.setMapperClass(PathTotalsMapper.class);
        job.setReducerClass(PathTotalsReducer.class);
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
            System.err.println("Usage: Step2_PathTotals <step2_mi_output> <path_totals_output>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = createJob(conf, new Path(args[0]), new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
