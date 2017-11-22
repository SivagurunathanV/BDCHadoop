package BDC;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * Created by sivagurunathanvelayutham on Nov, 2017
 */
public class AverageRating {

   public static <K extends Comparable, V extends Comparable> Map<K,V> sortByValues(Map<K,V> map){
        List<Map.Entry<K,V>> values = new LinkedList<Map.Entry<K, V>>(map.entrySet());
        Collections.sort(values, new Comparator<Map.Entry<K, V>>() {
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        Map<K,V> sortedMap = new LinkedHashMap<K, V>();
       for (Map.Entry<K,V> value: values) {
           sortedMap.put(value.getKey(),value.getValue());
       }
       return sortedMap;
   }

    public static class BusinessRatingMap extends Mapper<Object,Text,Text,DoubleWritable>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String[] line = str.split("\\^");
            if(line[2].length() > 0 && line[3].length() > 0){
                Text businessId = new Text();
                businessId.set(line[2]);
                DoubleWritable rating = new DoubleWritable();
                rating.set(Double.parseDouble(line[3]));
                context.write(businessId,rating);
            }
        }
    }

    public static class AverageRatingReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
        HashMap<Text,DoubleWritable> mapAverage = new HashMap<Text, DoubleWritable>();
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            Double sum = 0.0;
            int count = 0;
            for (DoubleWritable rating :values) {
                sum += rating.get();
                count++;
            }
            // create new object
            // otherwise will override
            mapAverage.put(new Text(key) , new DoubleWritable(sum/count));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Map<Text,DoubleWritable> sortedMap = sortByValues(mapAverage);
            int count = 0;
            for (Text bussinessId : sortedMap.keySet()) {
                if(count == 10)
                    break;
                context.write(bussinessId, sortedMap.get(bussinessId));
                count++;
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "averageRating");
        job.setJarByClass(AverageRating.class);
        job.setMapperClass(BusinessRatingMap.class);
        job.setReducerClass(AverageRatingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
