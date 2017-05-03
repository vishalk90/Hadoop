package FlightDataAnalysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Created by vishal kulkarni on 4/29/17
 */
public class FDA_ATT_Driver {

    public static class avgTimeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            String[] eachRow = value.toString().split(",");

            String originFrom = eachRow[16];
            String taxiInTime = eachRow[19];

            String year = eachRow[0];

            String destinationTo = eachRow[17];
            String taxiOutTime = eachRow[20];

            if (!year.equals("Year") && !originFrom.equals("NA") && !taxiInTime.equals("NA") && !destinationTo.equals("NA") && !taxiOutTime.equals("NA")) {

                int in = Integer.parseInt(taxiInTime);
                int out = Integer.parseInt(taxiOutTime);

                context.write(new Text(originFrom), new IntWritable(out));
                context.write(new Text(destinationTo), new IntWritable(in));
            }

        }
    }

    public static class output {
        String airport;
        Double avgTaxiTime;

        public output(String airport, Double avgTaxiTime) {
            this.airport = airport;
            this.avgTaxiTime = avgTaxiTime;
        }
    }

    public static class avgTimeReducer extends Reducer<Text, IntWritable, Text, Text> {

        ArrayList<output> output = new ArrayList<output>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Long totalNumberOfInstances = 0L;
            Long totalTimeForTaxi = 0L;

            for (IntWritable instance : values) {
                totalNumberOfInstances += 1;
                totalTimeForTaxi += instance.get();

            }
            Double avgTaxiTime = (double) totalTimeForTaxi / (double) totalNumberOfInstances;
            output.add(new output(key.toString(), avgTaxiTime));
        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!output.isEmpty() && output.size() >= 3) {
                context.write(new Text("=================================="), new Text(""));
                context.write(new Text("Airports with avg Taxi Time: "), new Text("Top 3"));
                context.write(new Text("=================================="), new Text(""));

                Collections.sort(output, new compare());


                for (int i = 0; i < 3; i++) {
                    context.write(new Text(output.get(i).airport), new Text(output.get(i).avgTaxiTime.toString()));
                }

                context.write(new Text("==================================="), new Text(""));
                context.write(new Text("Airports with avg Taxi Time: "), new Text("Last 3"));
                context.write(new Text("==================================="), new Text(""));

                for (int j = output.size() - 1; j > output.size() - 4; j--) {
                    context.write(new Text(output.get(j).airport), new Text(output.get(j).avgTaxiTime.toString()));
                }
            } else if (output.size() > 0) {
                context.write(new Text("==========================="), new Text(""));
                context.write(new Text("Airports with avg Taxi Time"), new Text(""));
                context.write(new Text("==========================="), new Text(""));

                Collections.sort(output, new compare());

                for (output instance : output) {
                    context.write(new Text(instance.airport), new Text(instance.avgTaxiTime.toString()));
                }
            } else {
                context.write(new Text("============="), new Text(""));
                context.write(new Text("No Data Found"), new Text(""));
                context.write(new Text("============="), new Text(""));
            }
            System.out.println(System.currentTimeMillis());

        }

        public class compare implements Comparator<output> {
            @Override
            public int compare(output T1, output T2) {
                if (T1.avgTaxiTime > T2.avgTaxiTime)
                    return -1;
                else if (T1.avgTaxiTime < T2.avgTaxiTime)
                    return 1;
                else
                    return 0;
            }
        }


    }

}
