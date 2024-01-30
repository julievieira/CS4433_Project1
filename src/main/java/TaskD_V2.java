//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.net.URI;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Map;
//
//import org.apache.commons.lang.StringUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IOUtils;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//
//public class TaskD {
//    public static class OrderJoinMapper extends Mapper<IntWritable, Text, Text, Text> {
//        private Text outkey = new Text();
//        private Text outvalue = new Text();
//        public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
//            // get a line of input from Pages.csv
//            String line = value.toString();
//            String[] split = line.split(",");
//            // The foreign join key is the person ID (PersonID)
//            outkey.set(split[0]);
//            // Flag this record for the reducer and then output
//            outvalue.set("P" + value.toString());
//            context.write(outkey, outvalue);
//        }
//    }
//    public static class ProductJoinMapper extends Mapper<IntWritable, Text, Text, Text> {
//        private Text outkey = new Text();
//        private Text outvalue = new Text();
//        public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
//            // get a line of input from Friends.csv
//            String line = value.toString();
//            String[] split = line.split(",");
//            // The foreign join key is the person ID (MyFriend)
//            outkey.set(split[2]);
//            // Flag this record for the reducer and then output
//            outvalue.set("F" + value.toString());
//            context.write(outkey, outvalue);
//        }
//    }
//    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
//        private static final Text EMPTY_TEXT = new Text("");
//        private Text tag = new Text();
//        private ArrayList<Text> orderList = new ArrayList<Text>();
//        private ArrayList<Text> productList = new ArrayList<Text>();
//        private String joinType = null;
//        public void setup(Context context) {
//            // retrieve the type of join from our configuration
//            joinType = context.getConfiguration().get("join.type");
//        }
//        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            // clear list
//            orderList.clear();
//            productList.clear();
//            // bin each record from both mapper based on the tag letter "P" or "F". Then, remove the tag.
//            for (Text x : values) {
//                tag = x;
//                if (tag.charAt(0) == 'P') {
//                    orderList.add(new Text(tag.toString().substring(1)));
//                } else if (tag.charAt('P') == 'F') {
//                    productList.add(new Text(tag.toString().substring(1)));
//                }
//            }
//            // execute the join logic after both source lists are filled after iterating all values
//            executeJoinLogic(context);
//        }
//        private void executeJoinLogic(Context context) throws IOException, InterruptedException {
//            // you can change the type of join based on the type you configured in driver class
//            // here we use inner join as an example
//            if (joinType.equalsIgnoreCase("inner")) {
//                if (!orderList.isEmpty() && !productList.isEmpty()) {
//                    for (Text P : orderList) {
//                        for (Text F : productList) {
//                            context.write(P, F);
//                        }
//                    }
//                }
//            }
//        }
//    }
//    public boolean debug(String[] args) throws Exception {
//        long startTime = System.currentTimeMillis();
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "TaskD");
//        job.setJarByClass(TaskD.class);
//        job.setReducerClass(TaskD.JoinReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, OrderJoinMapper.class);
//        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ProductJoinMapper.class);
//        job.getConfiguration().set("inner", args[2]);
//
//        // Delete the output directory if it exists
//        Path outputPath = new Path(args[2]);
//        FileSystem fs = outputPath.getFileSystem(conf);
//        if (fs.exists(outputPath)) {
//            fs.delete(outputPath, true); // true will delete recursively
//        }
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//
//        boolean ret = job.waitForCompletion(true);
//
//        long endTime = System.currentTimeMillis();
//        System.out.println((endTime - startTime) / 1000.0 + " seconds");
//
//        return ret;
//    }
//
////    public static void main(String[] args) throws Exception {
////        Configuration conf = new Configuration();
////        Job job = Job.getInstance(conf, "TaskD");
////        job.setJarByClass(TaskD.class);
////        job.setMapperClass(ReduceSideJoin.class);
////        job.setCombinerClass(IntSumReducer.class);
////        job.setReducerClass(IntSumReducer.class);
////        job.setOutputKeyClass(Text.class);
////        job.setOutputValueClass(IntWritable.class);
////        FileInputFormat.addInputPath(job, new Path(args[1]));
////        FileOutputFormat.setOutputPath(job, new Path(args[2]));
////        System.exit(job.waitForCompletion(true) ? 0 : 1);
////    }
//}