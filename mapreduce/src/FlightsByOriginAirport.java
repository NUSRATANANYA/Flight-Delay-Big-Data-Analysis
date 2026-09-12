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

/**
 * Big Data Lab Project
 * Flight Delay Dataset 2024
 *
 * Analysis 4:
 * Calculate total number of flights
 * originating from each airport.
 */
public class FlightsByOriginAirport {

    /**
     * CSV-aware parser.
     * Handles commas inside quoted fields such as
     * "New York, NY".
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
     * Input:
     * One flight CSV record.
     *
     * Output:
     * origin airport -> 1
     *
     * Example:
     * JFK -> 1
     * ATL -> 1
     */
    public static class AirportMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text airport = new Text();

        private static final IntWritable ONE =
                new IntWritable(1);

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

            List<String> fields =
                    parseCSVLine(line);

            if (fields.size() < 35) {
                return;
            }

            // Index 7 = origin airport
            String origin =
                    fields.get(7).trim();

            // Ignore header
            if (origin.equalsIgnoreCase("origin")) {
                return;
            }

            // Ignore missing airport values
            if (origin.isEmpty()) {
                return;
            }

            airport.set(origin);

            context.write(
                    airport,
                    ONE
            );
        }
    }


    /**
     * Reducer
     *
     * Adds all flight records belonging
     * to the same origin airport.
     */
    public static class AirportReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result =
                new IntWritable();

        @Override
        public void reduce(
                Text key,
                Iterable<IntWritable> values,
                Context context)
                throws IOException, InterruptedException {

            int totalFlights = 0;

            for (IntWritable value : values) {

                totalFlights += value.get();
            }

            result.set(totalFlights);

            context.write(
                    key,
                    result
            );
        }
    }


    /**
     * Driver
     */
    public static void main(String[] args)
            throws Exception {

        if (args.length != 2) {

            System.err.println(
                "Usage: FlightsByOriginAirport <input path> <output path>"
            );

            System.exit(2);
        }

        Configuration conf =
                new Configuration();

        Job job = Job.getInstance(
                conf,
                "Flight Delay 2024 - Total Flights by Origin Airport"
        );

        job.setJarByClass(
                FlightsByOriginAirport.class
        );

        job.setMapperClass(
                AirportMapper.class
        );

        /*
         * This is a summation operation,
         * so the Reducer can safely be used
         * as a Combiner.
         */
        job.setCombinerClass(
                AirportReducer.class
        );

        job.setReducerClass(
                AirportReducer.class
        );

        job.setMapOutputKeyClass(
                Text.class
        );

        job.setMapOutputValueClass(
                IntWritable.class
        );

        job.setOutputKeyClass(
                Text.class
        );

        job.setOutputValueClass(
                IntWritable.class
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