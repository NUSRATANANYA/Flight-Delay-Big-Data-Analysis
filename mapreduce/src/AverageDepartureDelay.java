import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Big Data Lab Project
 * Flight Delay Dataset 2024
 *
 * Analysis 2:
 * Calculate the average departure delay for each airline.
 */
public class AverageDepartureDelay {

    /**
     * CSV-aware parser.
     *
     * Required because fields such as "New York, NY"
     * contain commas inside quotation marks.
     */
    public static List<String> parseCSVLine(String line) {

        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean insideQuotes = false;

        for (int i = 0; i < line.length(); i++) {

            char ch = line.charAt(i);

            if (ch == '"') {

                if (insideQuotes
                        && i + 1 < line.length()
                        && line.charAt(i + 1) == '"') {

                    currentField.append('"');
                    i++;

                } else {

                    insideQuotes = !insideQuotes;
                }

            } else if (ch == ',' && !insideQuotes) {

                fields.add(currentField.toString());
                currentField.setLength(0);

            } else {

                currentField.append(ch);
            }
        }

        fields.add(currentField.toString());

        return fields;
    }


    /**
     * Mapper
     *
     * Key   = Airline code
     * Value = Departure delay
     *
     * Example:
     *
     * AA -> -5
     * AA -> 12
     * AA -> 30
     */
    public static class DelayMapper
            extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private final Text airline = new Text();
        private final DoubleWritable delayValue = new DoubleWritable();

        @Override
        public void map(
                LongWritable key,
                Text value,
                Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.trim().isEmpty()) {
                return;
            }

            List<String> fields = parseCSVLine(line);

            if (fields.size() < 35) {
                return;
            }

            String carrier = fields.get(5).trim();
            String departureDelay = fields.get(15).trim();

            // Ignore header
            if (carrier.equalsIgnoreCase("op_unique_carrier")) {
                return;
            }

            // Ignore missing carrier or missing departure delay
            if (carrier.isEmpty() || departureDelay.isEmpty()) {
                return;
            }

            try {

                double delay = Double.parseDouble(departureDelay);

                airline.set(carrier);
                delayValue.set(delay);

                context.write(airline, delayValue);

            } catch (NumberFormatException e) {

                // Ignore invalid numerical values
            }
        }
    }


    /**
     * Reducer
     *
     * Calculates:
     *
     * average delay = sum of delays / number of valid records
     */
    public static class DelayReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private final DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(
                Text key,
                Iterable<DoubleWritable> values,
                Context context)
                throws IOException, InterruptedException {

            double totalDelay = 0.0;
            long count = 0;

            for (DoubleWritable value : values) {

                totalDelay += value.get();
                count++;
            }

            if (count > 0) {

                double averageDelay = totalDelay / count;

                result.set(averageDelay);

                context.write(key, result);
            }
        }
    }


    /**
     * Driver
     */
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {

            System.err.println(
                "Usage: AverageDepartureDelay <input path> <output path>"
            );

            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
            conf,
            "Flight Delay 2024 - Average Departure Delay by Airline"
        );

        job.setJarByClass(AverageDepartureDelay.class);

        job.setMapperClass(DelayMapper.class);
        job.setReducerClass(DelayReducer.class);

        /*
         * Do NOT use the reducer as a combiner here.
         *
         * Taking an average of partial averages could produce
         * an incorrect overall average.
         */

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(
            job,
            new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
            job,
            new Path(args[1])
        );

        System.exit(
            job.waitForCompletion(true) ? 0 : 1
        );
    }
}