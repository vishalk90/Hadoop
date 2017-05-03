package FlightDataAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by vishal kulkarni on 4/28/17
 */
public class FDA_Flight_Delay_Prob {
    public static void main(String[] args) throws Exception {
        System.out.println(System.currentTimeMillis());
        Configuration conf = new Configuration();
        Job job = new Job(conf, "DelayProbability");
        job.setJarByClass(FDA_Flight_Delay_Prob.class);
        job.setMapperClass(FDA_FDP_Driver.onScheduleMapper.class);
        job.setReducerClass(FDA_FDP_Driver.onScheduleReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path[]{new Path(args[0])});
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
