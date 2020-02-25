import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.StringTokenizer;


public class Question1 {

    public static class CFMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private Text keyword = new Text(); //type of output key
        private Text friends = new Text(); //type of output value

        @Override
        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString(); // 0    1,2,3,...

            String [] meAndFriends = line.split("\\t");
            String me = meAndFriends[0].trim(); //0  the subject person id

            if (meAndFriends.length == 2) {
                friends.set(meAndFriends[1]); //the string represent the friends list: "1,2,3..."
                //generate (0,1) friends; (0,2) friends... key and value pair.
                StringTokenizer itr = new StringTokenizer(meAndFriends[1], ","); //split "1,2,3.." into "1", "2", "3"... each token is a string
                while (itr.hasMoreTokens()) {
                    String friend = itr.nextToken().trim();
                    if (me.compareTo(friend) < 0) {
                        keyword.set(me + "," + friend);
                    } else {
                        keyword.set(friend + "," + me);
                    }
                    context.write(keyword, friends); //for now, generates (me, friends) pair
                }
            }
        }
    }


    public static class CFReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException {

            ArrayList<String[]> arr = new ArrayList<>();

            // change "1,2,3..." string type friend list to string[]
            for (Text text: values) {
                String temp = text.toString();
                String[] listFriend = temp.split(",");
                arr.add(listFriend); // arr list contains string[] friend lists of a friend pair.
            }

            String commonFriendStr = "";
            //find common friend
            HashSet<String> commonElements = new HashSet<>();

            for (String[] friends: arr) {
                Arrays.sort(friends);
                for (String friend: friends) {
                    if (commonElements.contains(friend)) {
                        commonFriendStr += friend + ",";
                    } else {
                        commonElements.add(friend);
                    }
                }
            }

            //output value type
            Text commonFriendText = new Text();
            if (commonFriendStr != null && commonFriendStr.length() != 0 )
                commonFriendText.set(commonFriendStr.substring(0, commonFriendStr.length() - 1));
            context.write(key, commonFriendText);

        }
    }

    //driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        //get all args
        if (otherArgs.length != 2) {
            System.err.println("ERROR: Wrong number of parameters: " + otherArgs.length + "instead of 2");
            System.exit(2);
        }

        //create a job with the name "CommonFriend"
        Job job = new Job(conf, "CommonFriendOfInputPairs");
        job.setJarByClass(Question1.class);
        job.setMapperClass(CFMapper.class);
        job.setReducerClass(CFReducer.class);

        //set output type (string, string)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //set number of reduce task

        //job.setNumReduceTasks(1);

        //set the hdfs path for input and output data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //wait till job finishes
        System.exit(job.waitForCompletion(true)? 0: 1);
    }
}


