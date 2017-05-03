package FlightDataAnalysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by vishalkulkarni on 4/29/17.
 */
public class FDA_RFFC_Driver {

    public static class CancellationReasonMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            String[] eachRow = value.toString().split(",");
            String year = eachRow[0];
            int one = 1;

            String cancellationCode = eachRow[22];
            String isCancelled = eachRow[21];

            if (!year.equals("Year") && !cancellationCode.equals("NA") && !isCancelled.equals("NA") && isCancelled.equals("1")) {

                context.write(new Text(cancellationCode), new IntWritable(one));

            }

        }

    }


    public static class CancellationReasonReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;

            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(new Text(key), new IntWritable(count));

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println(System.currentTimeMillis());
        }
    }


}
