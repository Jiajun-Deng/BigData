
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class NumTop10 {

    //In Mapper1, only pair with > 0 mutual friends will be left.
    //0,1  1,2,3...
    public static class FriendListMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private Text friends = new Text(); // output value type
        private Text pair = new Text(); // output key type

        @Override
        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString(); // 0,1    1,2,3,...

            String [] meAndFriends = line.split("\\t");
            String meAndHim = meAndFriends[0].trim();

            if (meAndFriends.length == 2) {
                friends.set(meAndFriends[1]); //the string represent the friends list: "1,2,3..."
                pair.set(meAndHim);
                context.write(pair, friends);
            }
        }
    }

    //Reducer1
    // count the number of mutual friends for each pair.
    public static class SumReducer
            extends Reducer<Text, Text, Text, IntWritable> {

        private IntWritable count = new IntWritable();

        @Override
        public void reduce(Text key,Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int friendNum = 0;
            for (Text temp: values) {
                String[] friends = temp.toString().split(",");
                friendNum = friends.length;
            }
            count.set(friendNum);
            context.write(key, count);
        }
    }

    //Mapper2, swap key and value of output from Reducer1 and then sort by key
    public static class friendNumMapper
            extends Mapper<Text, Text, LongWritable, Text> {

        private LongWritable numOfFriend = new LongWritable();

        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            int val = Integer.parseInt(value.toString());
            numOfFriend.set(val);
            context.write(numOfFriend, key);
        }
    }

    //Reducer2, output the top 10 number of mutual friends
    public static class SumReducer2
            extends Reducer<LongWritable, Text, Text, LongWritable> {
        private int idx = 0;

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value: values) {

                if (idx < 10) {
                    idx++;
                    context.write(value, key);
                }
            }
        }
    }


    //driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String inputPath = otherArgs[0];
        String tempPath = otherArgs[1];
        String outputPath = otherArgs[2];

        //first job
        {   //create first job
            conf = new Configuration();
            Job job = new Job(conf, "FriendCount");
            job.setJarByClass(NumTop10.class);
            job.setMapperClass(NumTop10.FriendListMapper.class);
            job.setReducerClass(NumTop10.SumReducer.class);

            //set job1's mapper output key/value type
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            //set job1's output key/value type
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //set job1's input path for HDFS
            FileInputFormat.addInputPath(job, new Path(inputPath));
            //set job1's output path for HDFS
            FileOutputFormat.setOutputPath(job, new Path(tempPath));

            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }

        //second job
        {
            //create second job
            conf = new Configuration();
            Job job2 = new Job(conf, "TopNum");
            job2.setJarByClass(NumTop10.class);
            job2.setMapperClass(NumTop10.friendNumMapper.class);
            job2.setReducerClass(NumTop10.SumReducer2.class);

            //set job2's mapper output key/value type
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Text.class);

            //set job2's output key/value type
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(LongWritable.class);

            job2.setInputFormatClass(KeyValueTextInputFormat.class);

            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            job2.setNumReduceTasks(1);

            //set job2's input path for HDFS
            FileInputFormat.addInputPath(job2, new Path(tempPath));
            //set job2's output path for HDFS
            FileOutputFormat.setOutputPath(job2, new Path(outputPath));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}

