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
 * Analysis 1:
 * Calculate the total number of flights operated by each airline.
 */
public class FlightsByAirline {

    /**
     * Simple CSV parser.
     *
     * We cannot use line.split(",") because fields such as
     * "New York, NY" contain commas inside quotation marks.
     */
    public static List<String> parseCSVLine(String line) {

        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();

        boolean insideQuotes = false;

        for (int i = 0; i < line.length(); i++) {

            char ch = line.charAt(i);

            if (ch == '"') {

                // Handle escaped double quotes ("")
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

        // Add the final field
        fields.add(currentField.toString());

        return fields;
    }


    /**
     * Mapper
     *
     * Input:
     * One CSV record.
     *
     * Output:
     * airline -> 1
     *
     * Example:
     * 9E -> 1
     */
    public static class FlightMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text airline = new Text();
        private static final IntWritable ONE = new IntWritable(1);

        @Override
        public void map(
                LongWritable key,
                Text value,
                Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Ignore empty lines
            if (line.trim().isEmpty()) {
                return;
            }

            List<String> fields = parseCSVLine(line);

            /*
             * Dataset contains 35 columns.
             * Index 5 = op_unique_carrier
             */
            if (fields.size() < 35) {
                return;
            }

            String carrier = fields.get(5).trim();

            // Ignore the CSV header
            if (carrier.equalsIgnoreCase("op_unique_carrier")) {
                return;
            }

            // Ignore missing carrier values
            if (carrier.isEmpty()) {
                return;
            }

            airline.set(carrier);

            context.write(airline, ONE);
        }
    }


    /**
     * Reducer
     *
     * Receives:
     *
     * AA -> [1,1,1,1,...]
     *
     * Adds all values and produces:
     *
     * AA -> total number of flights
     */
    public static class FlightReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

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

            context.write(key, result);
        }
    }


    /**
     * Driver
     *
     * Configures and starts the MapReduce job.
     */
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {

            System.err.println(
                "Usage: FlightsByAirline <input path> <output path>"
            );

            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
            conf,
            "Flight Delay 2024 - Total Flights by Airline"
        );

        job.setJarByClass(FlightsByAirline.class);

        // Mapper
        job.setMapperClass(FlightMapper.class);

        /*
         * Combiner optimization:
         *
         * Because this is simple addition, the Reducer can also
         * safely be used as a Combiner to reduce network traffic.
         */
        job.setCombinerClass(FlightReducer.class);

        // Reducer
        job.setReducerClass(FlightReducer.class);

        // Mapper output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Final output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // HDFS input
        FileInputFormat.addInputPath(
            job,
            new Path(args[0])
        );

        // HDFS output
        FileOutputFormat.setOutputPath(
            job,
            new Path(args[1])
        );

        System.exit(
            job.waitForCompletion(true) ? 0 : 1
        );
    }
}