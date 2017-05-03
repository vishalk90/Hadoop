package FlightDataAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by vishalkulkarni on 4/28/17.
 */
public class FDA_Avg_Taxi_Time {

    public static void main(String[] args) throws Exception {
        System.out.println(System.currentTimeMillis());
        Configuration conf = new Configuration();
        Job job = new Job(conf, "AvgTaxiTime");
        job.setJarByClass(FDA_Avg_Taxi_Time.class);
        job.setMapperClass(FDA_ATT_Driver.avgTimeMapper.class);
        job.setReducerClass(FDA_ATT_Driver.avgTimeReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path[]{new Path(args[0])});
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
