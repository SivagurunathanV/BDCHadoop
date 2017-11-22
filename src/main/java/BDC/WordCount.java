package BDC;

/**
 * Created by sivagurunathanvelayutham on Nov, 2017
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount
{
    public static class Map extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable rating = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String itr = new String(value.toString());

            String[] line = itr.split("\\^");
                if(line[3].length()> 0 &&  line[4].length() > 0){
                    IntWritable rating = new IntWritable(Integer.parseInt(line[4]));
                    Text bussinessId  = new Text();
                    bussinessId.set(line[3]);
                    context.write(bussinessId, rating);
                }
        }
    }

    public static class Reduce
            extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,Context context)
                throws IOException, InterruptedException
        {
            int sum = 0;
            int count = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
                count++;
            }
            result.set(sum/count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(Map.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

