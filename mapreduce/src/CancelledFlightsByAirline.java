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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CancelledFlightsByAirline {

    // CSV parser for fields containing commas inside quotes
    public static List<String> parseCSVLine(String line) {

        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean insideQuotes = false;

        for (int i = 0; i < line.length(); i++) {

            char ch = line.charAt(i);

            if (ch == '"') {

                if (insideQuotes &&
                    i + 1 < line.length() &&
                    line.charAt(i + 1) == '"') {

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


    public static class CancelledMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text airline = new Text();
        private static final IntWritable ONE = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.trim().isEmpty()) {
                return;
            }

            List<String> fields = parseCSVLine(line);

            if (fields.size() < 35) {
                return;
            }

            // Column 5 = airline
            String carrier = fields.get(5).trim();

            // Column 23 = cancelled
            String cancelled = fields.get(23).trim();

            // Skip header
            if (carrier.equalsIgnoreCase("op_unique_carrier")) {
                return;
            }

            if (carrier.isEmpty() || cancelled.isEmpty()) {
                return;
            }

            try {

                int cancelledValue =
                        Integer.parseInt(cancelled);

                // Only count cancelled flights
                if (cancelledValue == 1) {

                    airline.set(carrier);

                    context.write(airline, ONE);
                }

            } catch (NumberFormatException e) {
                // Ignore invalid records
            }
        }
    }


    public static class CancelledReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key,
                           Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int total = 0;

            for (IntWritable value : values) {
                total += value.get();
            }

            result.set(total);

            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {

        if (args.length != 2) {

            System.err.println(
                "Usage: CancelledFlightsByAirline <input> <output>"
            );

            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "Flight Delay 2024 - Cancelled Flights by Airline"
        );

        job.setJarByClass(CancelledFlightsByAirline.class);

        job.setMapperClass(CancelledMapper.class);

        // Safe because this is a summation operation
        job.setCombinerClass(CancelledReducer.class);

        job.setReducerClass(CancelledReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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