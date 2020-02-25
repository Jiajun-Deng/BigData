import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import java.util.HashSet;

public class CommonFriendsFiltered {

    //Job1 generate the every friend pair's common friend list.
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
                // split "1,2,3.." into "1", "2", "3"... each token is a string
                String[] friendGroup = meAndFriends[1].split(",");
                for (String friend: friendGroup) {
                    if (Integer.parseInt(me) < Integer.parseInt(friend)) {
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

            StringBuilder sb = new StringBuilder();
            //find common friend
            HashSet<String> commonElements = new HashSet<>();


            for (String[] friends: arr) {
                for (String friend: friends) {
                    if (commonElements.contains(friend)) {
                        sb.append(friend+",");
                    } else {
                        commonElements.add(friend);
                    }
                }
            }

            //output value type
            Text commonFriendText = new Text();
            if (sb != null && sb.length() != 0 ) {
                sb.deleteCharAt(sb.length() - 1);
                commonFriendText.set(sb.toString());
            }
            context.write(key, commonFriendText);

        }
    }

    //Job2 filter the list according to the input pairs.
    public static class FilterMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        HashSet<String> set;
        private Text mapKey = new Text();
        private Text mapValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            String[] mydata = value.toString().split("\\t");
            if (mydata.length < 2) return;
            String tmpKey = mydata[0];
            String tmpValue = mydata[1];
            if (set.contains(tmpKey)) {
                mapKey.set(tmpKey);
                mapValue.set(tmpValue);
                context.write(mapKey, mapValue);
            }
        }

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            String[] keys = conf.get("KEYS").split("#");
            set = new HashSet<>();
            for (String key: keys) {
                set.add(key);
            }
        }
    }

    //driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        //get all args
        if (otherArgs.length != 4) {
            System.err.println("ERROR: Wrong number of parameters: "
                    + otherArgs.length + "instead of 4. Input, tempOutput, output, pairs(e.g. 1,2#42,12)");
            System.exit(2);
        }
        conf.set("KEYS", otherArgs[3]);

        {
            //Job1
            Job job = new Job(conf, "CommonFriendOfInputPairs");
            job.setJarByClass(CommonFriendsFiltered.class);
            job.setMapperClass(CFMapper.class);
            job.setReducerClass(CFReducer.class);

            //set output type (string, string)
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //set the hdfs path for input and output data
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            //wait till job finishes
            if (!job.waitForCompletion(true))
                System.exit(1);
        }

        {//Job2

            Job job2 = new Job(conf, "CommonFriendOfInputPairs");
            job2.setJarByClass(CommonFriendsFiltered.class);
            job2.setMapperClass(FilterMapper.class);

            //set output type (string, string)
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            //set the hdfs path for input and output data
            //input is the output of job1
            FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
            //wait till job finishes
            if (!job2.waitForCompletion(true))
                System.exit(1);
        }
    }
}


