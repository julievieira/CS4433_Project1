import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class TaskG {
    /*Identify all "disconnected" people (and return their personID and
Name) that have not accessed the Facebook site for 14 days or longer
10
(i.e., meaning no entries in the AccessLog exist in the last 14
days).
     */


    public static boolean within14Days(String timestamp1, String timestamp2) {
        // Define the date time formatter for parsing the timestamps
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // Parse the timestamps into LocalDateTime objects
        LocalDateTime dateTime1 = LocalDateTime.parse(timestamp1, formatter);
        LocalDateTime dateTime2 = LocalDateTime.parse(timestamp2, formatter);

        // Calculate the difference in days between the two timestamps
        long daysDifference = ChronoUnit.DAYS.between(dateTime1, dateTime2);

        // Check if the absolute difference is less than or equal to 14
        return Math.abs(daysDifference) <= 14;
    }

    public static class Map extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Get the current time
            LocalDateTime currentTime = LocalDateTime.now();

            // Define the desired format
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            // Format the current time
            String startTime = currentTime.format(formatter);


            String[] owner = value.toString().split(",");
            if (!owner[4].toString().contains("AccessTime")&& within14Days(owner[4], startTime)) {
                context.write(new Text(owner[1]), new Text(owner[4]));
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

/*         Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskG");
        job.setJarByClass(TaskG.class);
        job.setMapperClass(TaskG.Map.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // Here we assume that the first input path is for friends.csv and the second for access_log.csv
        FileInputFormat.addInputPath(job, new Path("/home/taya/CS4433_Project1/src/main/data/access_logs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/home/taya/CS4433_Project1/src/output"));

        job.waitForCompletion(true);*/



        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskG");
        job.setJarByClass(TaskG.class);
        job.setMapperClass(TaskG.Map.class);
        job.setNumReduceTasks(0);

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
        job.waitForCompletion(true);

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
        job.setMapperClass(TaskG.Map.class);

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
