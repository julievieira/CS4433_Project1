import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class TaskF {public static class FriendsMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private IntWritable userId = new IntWritable();
    private Text friendId = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length == 5) {
            // For friends.csv
            userId.set(Integer.parseInt(parts[1])); // PersonID
            friendId.set("PersonID" + parts[2]); // Prefix with 'FRIEND_' to identify the source
            context.write(userId, friendId);
        } else if (parts.length == 6) {
            // For access_log.csv
            userId.set(Integer.parseInt(parts[1])); // ByWho
            friendId.set("ByWho" + parts[2]); // Prefix with 'ACCESS_' to identify the source
            context.write(userId, friendId);
        }
    }
}
    public static class FriendsReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean hasAccessed = false;
            String name = "";

            for (Text value : values) {
                String parts = value.toString();
                if (parts.startsWith("ByWho")) {
                    hasAccessed = true;
                } else if (parts.startsWith("PersonID")) {
                    name = parts.substring(7); // Extract the name after 'FRIEND_'
                }
            }

            if (!hasAccessed) {
                context.write(key, new Text(name));
            }
        }
    }
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskF");
        job.setJarByClass(TaskF.class);
        job.setMapperClass(FriendsMapper.class);
        job.setReducerClass(FriendsReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // Here we assume that the first input path is for friends.csv and the second for access_log.csv
        FileInputFormat.addInputPath(job, new Path("/home/taya/CS4433_Project1/src/main/data/friends.csv"));
        FileInputFormat.addInputPath(job, new Path("/home/taya/CS4433_Project1/src/main/data/access_logs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/home/taya/CS4433_Project1/src/output"));

        job.waitForCompletion(true);
    }
}