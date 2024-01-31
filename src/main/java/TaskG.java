import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

// Identify all "disconnected" people (and return their personID and
//  Name) that have not accessed the Facebook site for 14 days or longer
// (i.e., meaning no entries in the AccessLog exist in the last 14
// days).
public class TaskG {

    public static boolean within14Days(String timestamp1, String timestamp2) {
        // Define the date time formatter for parsing the timestamps
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // Parse the timestamps into LocalDateTime objects
        LocalDateTime dateTime1 = LocalDateTime.parse(timestamp1, formatter);
        LocalDateTime dateTime2 = LocalDateTime.parse(timestamp2, formatter);

        // Calculate the difference in days between the two timestamps
        long daysDifference = ChronoUnit.DAYS.between(dateTime1, dateTime2);

        // Check if less than 14
        return daysDifference < 14;
    }

    public static class Map extends Mapper<Object, Text, Text, Text>{

        private java.util.Map<String, String> pagesMap = new HashMap<>();

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
                if(!split[0].equals("PersonID")) {
                    pagesMap.put(split[0], split[1]);
                }
            }
            // close the stream
            IOUtils.closeStream(reader);
        }

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // read each line of the data set (AccessLogs.csv)
            LocalDateTime currentTime = LocalDateTime.now();

            // Define the desired format
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            // Format the current time
            String startTime = currentTime.format(formatter);

            startTime = "2023-09-20 13:44:58";

            String[] fields = value.toString().split(",");
            if(!fields[0].equals("AccessID") && within14Days(fields[4], startTime)) {
                System.out.println("Removed " + fields[1] + " time is " + fields[4]);
                pagesMap.remove(fields[1]);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(java.util.Map.Entry<String, String> set : pagesMap.entrySet()) {
                context.write(new Text(set.getKey()), new Text(set.getValue()));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskG");
        job.setJarByClass(TaskG.class);
        job.setMapperClass(Map.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
        Job job = Job.getInstance(conf, "TaskG");
        job.setJarByClass(TaskG.class);
        job.setMapperClass(Map.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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