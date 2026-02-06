/**
 * Dispatcher for DIRT pipeline steps. First argument is the step name;
 * remaining arguments are passed to the corresponding step's main().
 *
 * Usage: hadoop jar dirt-algorithm.jar step1 [args...]
 *   step1   -> Step1_Counting    (input, output)
 *   step2   -> Step2_CalcMI      (input_triples, marginals_output, mi_output)
 *   step2b  -> Step2_PathTotals (step2_mi_output, path_totals_output)
 *   step3   -> Step3_Similarity  (input, output)
 *   step4   -> Step4_FinalSum    (input, output [, path_totals_file])
 */
import java.util.Arrays;

public class ProgramDriver {

    private static void usage() {
        System.err.println("Usage: <step> [arguments...]");
        System.err.println("  step1   Step1_Counting     <input> <output>");
        System.err.println("  step2   Step2_CalcMI       <input_triples> <marginals_output> <mi_output>");
        System.err.println("  step2b  Step2_PathTotals   <step2_mi_output> <path_totals_output>");
        System.err.println("  step3   Step3_Similarity   <input> <output>");
        System.err.println("  step4   Step4_FinalSum     <input> <output> [path_totals_file]");
        System.err.println("Example: step1 s3://bucket/input/ s3://bucket/output/step1");
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            usage();
            System.exit(1);
        }
        String step = args[0].trim().toLowerCase();
        String[] rest = args.length > 1 ? Arrays.copyOfRange(args, 1, args.length) : new String[0];

        switch (step) {
            case "step1":
                Step1_Counting.main(rest);
                break;
            case "step2":
                Step2_CalcMI.main(rest);
                break;
            case "step2b":
                Step2_PathTotals.main(rest);
                break;
            case "step3":
                Step3_Similarity.main(rest);
                break;
            case "step4":
                Step4_FinalSum.main(rest);
                break;
            default:
                System.err.println("Unknown step: " + args[0]);
                usage();
                System.exit(1);
        }
    }
}
