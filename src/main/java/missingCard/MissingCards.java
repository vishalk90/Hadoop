package missingCard; /**
 * Created by vishalkulkarni on 4/25/17.
 */

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MissingCards {
    public static void main(String[] args) throws Exception {
//		    Job job = new Job(Conf,"Find Missing Poker Card");
        Job job = new Job();
        job.setJarByClass(MissingCards.class);
        job.setJobName("Find the Missing Poker Cards");
        TextInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(missingCardsMapper.class);
        job.setReducerClass(missingCardsReducer.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }

    public static class missingCardsMapper extends Mapper<LongWritable, Text, Text, Text> {
        // input to mapper is LongWritable, text as key Value and Text , Text as output
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] sp = line.split(" ");    // Spliting with , as seperator
            //   logger.info(line);
            context.write((new Text(sp[0])), (new Text(sp[1])));    // moving suit in Arg1 and Rank In Arg2
        }
    }

    public static class missingCardsReducer extends Reducer<Text, Text, Text, Text> {
        //	 private static final java.util.logging.Logger logger = Logger.getLogger(MisCrdreducer.class);
        @Override
        public void reduce(Text token, Iterable<Text> counts, Context context) throws IOException, InterruptedException {
            String str = "";
            boolean[] exists = new boolean[13];
            //   logger.info(str);
            for (Text counter : counts) {
                for (int i = 1; i <= 13; i++) {
                    if (i == Integer.parseInt(counter.toString())) {
                        exists[(i - 1)] = true;
                    }
                }
            }
            for (int j = 0; j < 13; j++) {
                if (exists[j] == false)
                    str += (j + 1) + " ";
                //   logger.info(str);  //logging which is not available
            }
            Text Str1 = new Text(str);
            context.write(token, Str1);
        }
    }
}