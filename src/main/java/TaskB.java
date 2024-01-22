import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.TreeMap;

// Find the top 10 popular Facebook pages, namely, those that got the
// most accesses based on your AccessLog dataset compared to all other
// pages. Return their Id, Name and Nationality.
public class TaskB {

    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] owner = value.toString().split(",");
            context.write(new IntWritable(Integer.valueOf(owner[0])), new IntWritable(1));
        }

    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, Text, IntWritable> {

        private TreeMap<Integer, String[]> topOwners= new TreeMap<>();

        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException {
            int sum = 0;
            for(IntWritable x: values) {
                sum += x.get();
            }

            topOwners.put(sum, new String[]{});

            if(topOwners.size() > 10) {
                topOwners.remove(topOwners.firstKey());
            }

            context.write(key, new IntWritable(sum));
        }

    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskB");
        job.setJarByClass(TaskB.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Delete the output directory if it exists
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean ret = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");

        System.exit(ret ? 0 : 1);
    }

    public boolean debug(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskB");
        job.setJarByClass(TaskB.class);
        job.setMapperClass(Map.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Delete the output directory if it exists
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean ret = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");

        return ret;
    }

}
