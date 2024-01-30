import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskH {

    public static class FriendsMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text personId = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields.length == 5) {
                personId.set(fields[2]); //using MyFriend field
                context.write(personId, one);
            }
        }
    }

    public static class AverageFriendCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Map<String, Integer> friendCountMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // Store the total friend count for each person in a map
            friendCountMap.put(key.toString(), sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int totalFriendCount = 0;
            int totalPersonCount = 0;

            // Calculate the overall average friend count
            for (int count : friendCountMap.values()) {
                totalFriendCount += count;
                totalPersonCount++;
            }

            double overallAverage = (double) totalFriendCount / totalPersonCount;

            // Output only those persons with friend counts above the overall average
            for (Map.Entry<String, Integer> entry : friendCountMap.entrySet()) {
                if (entry.getValue() > overallAverage) {
                    result.set(entry.getValue());
                    context.write(new Text(entry.getKey()), result);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskH");

        job.setJarByClass(TaskH.class);
        job.setMapperClass(FriendsMapper.class);
        job.setReducerClass(AverageFriendCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("/home/taya/CS4433_Project1/src/main/data/friends.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/home/taya/CS4433_Project1/src/output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}