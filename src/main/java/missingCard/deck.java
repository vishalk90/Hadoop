package missingCard; /**
 * Created by vishalkulkarni on 3/2/17.
 */
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class deck {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            String[] s = line.split(" ");
            Text suit = new Text();
            suit.set(s[0]);
            IntWritable number = new IntWritable(Integer.parseInt(s[1]));
            output.collect(suit, number);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {
            System.out.println("reducer entered " + key.toString());
            Set<Integer> s = new HashSet<Integer>();
            System.out.println("set 1-13 creating");
            for (int i = 1; i <= 13; i++) {
                s.add(i);
            }
            while (values.hasNext()) {
                s.remove(values.next().get());
            }
            Iterator<Integer> iter = s.iterator();
            while (iter.hasNext()) {
                output.collect(key, new IntWritable(iter.next()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(deck.class);
        conf.setJobName("cardcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }

}
