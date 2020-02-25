
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

/*
List mutual friends' first name and birth date.
 */
public class FriendsInfo {

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<String, String> map = new HashMap<>();
        private Text UserInfo = new Text(); //type of output value
        private Text Pair = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] myData = value.toString().split("\\t");
            if (myData.length == 2) {
                String[] friends = myData[1].split(",");
                Pair.set(myData[0]);

                StringBuilder sb = new StringBuilder();
                sb.append("[");

                for (String friend: friends) {
                    if (map.containsKey(friend)) {
                        sb.append(map.get(friend));
                        sb.append(", ");
                    }
                }
                //remove extra , and space
                sb.deleteCharAt(sb.length() - 1);
                sb.deleteCharAt(sb.length() - 1);
                sb.append("]");
                UserInfo.set(sb.toString());
                context.write(Pair, UserInfo);
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
                    //put (id, firstname:birthdate) in map.
                    map.put(arr[0], arr[1] + ":" + arr[9]);
                    line = br.readLine();
                }
            }
        }
    }

    public static class Reduce
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String pair = conf.get("PAIR");
            if (key.toString().equals(pair)) {
                for (Text value: values) {
                    context.write(key, value);
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //get all args
        if (otherArgs.length != 4) {
            System.err.println(otherArgs.length + " arguments were input instead of 4: inpu1, input2, output file, target pair of friend(e.g: 1,2).");
            System.exit(2);
        }
        String pair = otherArgs[3];

        //the userdata.txt input path.
        conf.set("ARGUMENT", otherArgs[1]);
        conf.set("PAIR", pair);

        //create a job named as FriensInfo
        Job job = new Job(conf, "FriendsInfo");
        job.setJarByClass(FriendsInfo.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        //set output key/value type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //set path for input and output
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
