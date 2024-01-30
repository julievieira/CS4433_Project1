import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;

// Find the top 10 popular Facebook pages, namely, those that got the
// most accesses based on your AccessLog dataset compared to all other
// pages. Return their Id, Name and Nationality.
public class TaskD {
    public static class ReplicatedMapJoin extends Mapper<Object, Text, Text, IntWritable> {
        private Map<String, String> pageMap = new HashMap<>();
        private Text text = new Text();
        // read the record from Pages.csv into the distributed cache
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);
            // open the stream
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);
            // wrap it into a BufferedReader object which is easy to read a record
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis,
                    "UTF-8"));
            // read the record line by line
            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                String[] split = line.split(",");
                pageMap.put(split[0], split[1] + ',' + split[2]);
            }
            // close the stream
            IOUtils.closeStream(reader);
        }
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // read each line of the large data set (Friends.csv)
            String[] fields = value.toString().split(",");
            // use the personID (MyFriend) pulled from the Friends.csv data set
            // to retrieve PersonID and name from the lookup table in memory
            String personInfo = pageMap.get(fields[2]);
            text.set(fields[2] + "," + personInfo);
            // output the mapper key-value pair
            context.write(text, new IntWritable(1));
        }

    }
    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskD");
        job.setJarByClass(TaskD.class);
        job.setMapperClass(ReplicatedMapJoin.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // a file in local file system is being used here as an example
        job.addCacheFile(new URI(args[0]));
        // Delete the output directory if it exists
        Path outputPath = new Path(args[2]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, outputPath);
        boolean ret = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");
        System.exit(ret ? 0 : 1);
    }
    public boolean debug(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskD");
        job.setJarByClass(TaskD.class);
        job.setMapperClass(ReplicatedMapJoin.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // a file in local file system is being used here as an example
        job.addCacheFile(new URI(args[0]));
        // Delete the output directory if it exists
        Path outputPath = new Path(args[2]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, outputPath);
        boolean ret = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");
        return ret;
    }
}
