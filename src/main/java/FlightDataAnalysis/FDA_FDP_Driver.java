package FlightDataAnalysis;
/*
 * Created by vishal kulkarni on 4/28/17
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class FDA_FDP_Driver {

    public static class onScheduleMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            String[] eachRow = value.toString().split(",");

            String year = eachRow[0];
            String flightCarrier = eachRow[8];
            String arrivalDelay = eachRow[14];
            String departureDelay = eachRow[15];

            if (!year.equals("Year") && !arrivalDelay.equals("NA") && !departureDelay.equals("NA")) {
                if ((Integer.parseInt(arrivalDelay) + Integer.parseInt(departureDelay)) <= 5) {
                    context.write(new Text(flightCarrier), new IntWritable(1));
                } else {
                    context.write(new Text(flightCarrier), new IntWritable(0));
                }
            }

        }
    }

    public static class Output {
        String flightCarrier;
        Double onTimeProbability;

        public Output(String flightCarrier, Double onTimeProbability) {
            this.flightCarrier = flightCarrier;
            this.onTimeProbability = onTimeProbability;
        }
    }

    public static class onScheduleReducer extends Reducer<Text, IntWritable, Text, Text> {

        ArrayList<Output> output = new ArrayList<Output>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Long totalDelay = 0L;
            Long delayForEachCarrier = 0L;
            for (IntWritable delay : values) {
                totalDelay += 1;
                if (delay.get() == 1) {
                    delayForEachCarrier += 1;
                }
            }
            Double onTimeProbability = (double) delayForEachCarrier / (double) totalDelay;
            output.add(new Output(key.toString(), onTimeProbability));
        }

        public class compare implements Comparator<Output> {
            @Override
            public int compare(Output T1, Output T2) {
                if (T1.onTimeProbability > T2.onTimeProbability)
                    return -1;
                else if (T1.onTimeProbability < T2.onTimeProbability)
                    return 1;
                else
                    return 0;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!output.isEmpty() && output.size() >= 3) {
                context.write(new Text("=================================="), new Text(""));
                context.write(new Text("Flight Delay probability: "), new Text("Top 3"));
                context.write(new Text("=================================="), new Text(""));
                Collections.sort(output, new compare());
                for (int i = 0; i < 3; i++) {
                    context.write(new Text(output.get(i).flightCarrier), new Text(output.get(i).onTimeProbability.toString()));

                }
                context.write(new Text("==================================="), new Text(""));
                context.write(new Text("Flight Delay probability: "), new Text("Last 3"));
                context.write(new Text("==================================="), new Text(""));

                for (int j = output.size() - 1; j > output.size() - 4; j--) {
                    context.write(new Text(output.get(j).flightCarrier), new Text(output.get(j).onTimeProbability.toString()));
                }

            } else if (output.size() > 0) {
                context.write(new Text("========================"), new Text(""));
                context.write(new Text("Flight Delay probability"), new Text(""));
                context.write(new Text("========================"), new Text(""));

                Collections.sort(output, new compare());

                for (Output instance : output) {
                    context.write(new Text(instance.flightCarrier), new Text(instance.onTimeProbability.toString()));
                }
            } else {
                context.write(new Text("============="), new Text(""));
                context.write(new Text("No Data Found"), new Text(""));
                context.write(new Text("============="), new Text(""));
            }
            System.out.println(System.currentTimeMillis());
        }

    }


}
