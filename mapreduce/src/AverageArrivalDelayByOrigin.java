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
 * Analysis 3:
 * Calculate average arrival delay by origin airport.
 */
public class AverageArrivalDelayByOrigin {

    /**
     * CSV-aware parser.
     * Handles commas contained inside quoted fields.
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
     * Key   = origin airport
     * Value = arrival delay
     *
     * Example:
     *
     * JFK -> -19
     * JFK -> 15
     * ATL -> 8
     */
    public static class ArrivalDelayMapper
            extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private final Text airport = new Text();
        private final DoubleWritable delay = new DoubleWritable();

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

            // Index 7 = origin
            String origin = fields.get(7).trim();

            // Index 22 = arr_delay
            String arrivalDelay = fields.get(22).trim();

            // Ignore header
            if (origin.equalsIgnoreCase("origin")) {
                return;
            }

            // Ignore missing values
            if (origin.isEmpty() || arrivalDelay.isEmpty()) {
                return;
            }

            try {

                double delayValue =
                        Double.parseDouble(arrivalDelay);

                airport.set(origin);
                delay.set(delayValue);

                context.write(airport, delay);

            } catch (NumberFormatException e) {

                // Ignore invalid numerical records
            }
        }
    }


    /**
     * Reducer
     *
     * Calculates average arrival delay
     * for each origin airport.
     */
    public static class ArrivalDelayReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private final DoubleWritable result =
                new DoubleWritable();

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

                double average =
                        totalDelay / count;

                result.set(average);

                context.write(key, result);
            }
        }
    }


    /**
     * Driver
     */
    public static void main(String[] args)
            throws Exception {

        if (args.length != 2) {

            System.err.println(
                "Usage: AverageArrivalDelayByOrigin <input path> <output path>"
            );

            System.exit(2);
        }

        Configuration conf =
                new Configuration();

        Job job = Job.getInstance(
                conf,
                "Flight Delay 2024 - Average Arrival Delay by Origin Airport"
        );

        job.setJarByClass(
                AverageArrivalDelayByOrigin.class
        );

        job.setMapperClass(
                ArrivalDelayMapper.class
        );

        job.setReducerClass(
                ArrivalDelayReducer.class
        );

        /*
         * No ordinary average combiner is used because
         * averaging partial averages can give an
         * incorrect global average.
         */

        job.setMapOutputKeyClass(
                Text.class
        );

        job.setMapOutputValueClass(
                DoubleWritable.class
        );

        job.setOutputKeyClass(
                Text.class
        );

        job.setOutputValueClass(
                DoubleWritable.class
        );

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