import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Vector;

/*
List mutual friends' first name and birth date.
 */
public class Q4 {
    private static Log mapLog1 = LogFactory.getLog(Top10Mapper.class);
    private static Log mapLog2 = LogFactory.getLog(ReduceJoin.Mapper1.class);
    private static Log mapLog3 = LogFactory.getLog(ReduceJoin.Mapper2.class);
    private static Log reduceLog = LogFactory.getLog(ReduceJoin.JoinReducer.class);
    private static Log log = LogFactory.getLog(Q4.class);

    //generate key-value pairs as (userId, age of a friend)
    public static class AgeMapper
            extends Mapper<LongWritable, Text, Text, LongWritable> {
        HashMap<String, Integer> map = new HashMap<>();
        private LongWritable FriendAge = new LongWritable(); //type of output value
        private Text User = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] myData = value.toString().split("\\t");
            User.set(myData[0]);
            if (myData.length == 2) {
                String[] friends = myData[1].split(",");
                for (String friend: friends) {
                    if (map.containsKey(friend)) {
                        FriendAge.set(2020 - map.get(friend));
                    }
                    context.write(User, FriendAge);
                }
            }
        }

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            //read data to memory on the mapper
            Configuration conf = context.getConfiguration();

            Path part = new Path(conf.get("ARGUMENT"));//location of file on hdfs

            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fss = fs.listStatus(part);
            for (FileStatus status: fss) {
                Path pt = status.getPath();
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line = br.readLine();
                while (line != null) {
                    String[] arr = line.split(",");
                    //put (id, birthYear) in map.
                    String[] birthDate = arr[9].split("\\/");
                    int year = Integer.parseInt(birthDate[birthDate.length - 1]);
                    map.put(arr[0], year);
                    line = br.readLine();
                }
            }
        }
    }

    // find a user's friends' largest age
    public static class AgeReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long max = 0;
            for (LongWritable age: values) {
                max = age.get() > max ? age.get() : max;
            }
            context.write(key, new LongWritable(max));
        }
    }

    //Job2
    //using job chain to process output from job1, sort the (user   age) by age in descending order.

    //Mapper2, swap key and value of output from Reducer1 and then sort by key
    public static class Top10Mapper
            extends Mapper<Text, Text, LongWritable, Text> {

        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(new LongWritable(Long.parseLong(value.toString())), key);
        }
    }

    //Reducer2, output the top 10 (user age) pairs
    public static class Top10Reducer
            extends Reducer<LongWritable, Text, Text, LongWritable> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value: values) {
                context.write(value, key);
            }
        }
    }


    //Job3
    public static class Mapper1
            extends Mapper<LongWritable, Text, Text, Text> {
        private int idx = 0;
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split("\\t");
            String id = values[0].trim();
            if (values.length == 2) {
                if (idx < 10) {
                    idx++;
                    //write id: oldest friend's age
                    mapLog2.info(id.toString() + "----------" + "b#" + values[1]);
                    context.write(new Text(id), new Text("b#" + values[1]));
                }
            }
        }
    }
    //mapper for userdata.txt
    public static class Mapper2
            extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split(",");
            String id = values[0].trim();
            if (values.length > 1) {
                //write id: address
                StringBuilder sb = new StringBuilder();
                sb.append(values[3] + ", ");
                sb.append(values[4] + ", ");
                sb.append(values[5]);
                mapLog3.info(id.toString() + "-----------------" + "a#" + sb.toString());
                context.write(new Text(id), new Text("a#" + sb.toString()));
            }
        }
    }

    public static class JoinReducer
            extends Reducer<Text, Text, Text, Text> {
        private Text keyString = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Vector<String> vecA = new Vector<>();
            Vector<String> vecB = new Vector<>();

            for (Text item: values) {
                if (item.toString().startsWith("a#")) {
                    vecA.add(item.toString().substring(2));
                } else if (item.toString().startsWith("b#")) {
                    vecB.add(item.toString().substring(2));
                }
            }

            int sizeA = vecA.size();
            int sizeB = vecB.size();

            for (int i = 0; i < sizeA; i++) {
                for (int j = 0; j < sizeB; j++) {
                    reduceLog.info(key.toString() + ", " + vecA.get(i) + ", " + vecB.get(j));
                    keyString.set(key.toString() + ", " + vecA.get(i) + ", " + vecB.get(j));
                    context.write(keyString, new Text());
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //get all args
        if (otherArgs.length != 5) {
            System.err.println(otherArgs.length + " arguments were input instead of 5.");
            System.exit(2);
        }
        String input1Path = otherArgs[0];
        String input2Path = otherArgs[1];
        String temp1Path = otherArgs[2];
        String temp2Path = otherArgs[3];
        String outputPath = otherArgs[4];

        {   //Job1: generate userId: ageofOldestFriend as table A.
            //the userdata.txt input path.
            conf = new Configuration();
            conf.set("ARGUMENT", input2Path);
            log.info("----------------------JOB1");
            //Job1: create a job named as UserAndFriendAge
            Job job = new Job(conf, "UserAndFriendAge");
            job.setJarByClass(Q4.class);
            job.setMapperClass(AgeMapper.class);
            job.setReducerClass(AgeReducer.class);

            //set mapper output key/value type
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            //set output key/value type
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            //set path for input and output
            FileInputFormat.addInputPath(job, new Path(input1Path));
            FileOutputFormat.setOutputPath(job, new Path(temp1Path));

            if (!job.waitForCompletion(true))
                System.exit(1);
        }

        {   //Job2: output of job1 is the input for job2
            conf = new Configuration();
            log.info("----------------------JOB2");
            Job job2 = new Job(conf, "Top10User");
            job2.setJarByClass(Q4.class);
            job2.setMapperClass(Top10Mapper.class);
            job2.setReducerClass(Top10Reducer.class);

            //set mapper output key/value type
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Text.class);

            //set output type of key/value
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(LongWritable.class);

            //set path for input and output
            FileInputFormat.addInputPath(job2, new Path(temp1Path)); //id,age
            FileOutputFormat.setOutputPath(job2, new Path(temp2Path));

            job2.setInputFormatClass(KeyValueTextInputFormat.class);

            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            job2.setNumReduceTasks(1);

            if (!job2.waitForCompletion(true))
                System.exit(1);
        }

        //job3
        //reduce side join of user,age and userdata.txt
        {
            conf = new Configuration();
            log.info("----------------------JOB3");
            Job job3 = new Job(conf, "Final");
            job3.setJarByClass(Q4.class);
//            job3.setMapperClass(JoinMapper.class);
            job3.setReducerClass(JoinReducer.class);
            job3.setNumReduceTasks(1);

            //set output type of key/value
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);

            Path[] paths = new Path[2];
            paths[0] = new Path(temp2Path);
            paths[1] = new Path(input2Path);
            //FileInputFormat.setInputPaths(job3, paths);
            //set input path to different mapper
            MultipleInputs.addInputPath(job3, paths[0], TextInputFormat.class, Mapper1.class);
            MultipleInputs.addInputPath(job3, paths[1], TextInputFormat.class, Mapper2.class);

            //set path for output
            FileOutputFormat.setOutputPath(job3, new Path(outputPath));

            if (!job3.waitForCompletion(true))
                System.exit(1);
        }
    }
}
