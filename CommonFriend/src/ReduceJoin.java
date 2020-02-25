import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Vector;

/*
List mutual friends' first name and birth date.
 */
public class ReduceJoin {
    private static Log mapLog1 = LogFactory.getLog(Mapper1.class);
    private static Log mapLog2 = LogFactory.getLog(Mapper1.class);
    private static Log reduceLog = LogFactory.getLog(JoinReducer.class);


    ///////////////////////////////////////////
    //Job3
    //Mapper to generate id,age
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
                    context.write(new Text(id), new Text("a#" + values[1]));
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
                context.write(new Text(id), new Text("b#" + sb.toString()));
            }
        }
    }

    public static class JoinReducer
            extends Reducer<Text, Text, Text, Text> {

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
                    //reduceLog.info(key.toString() + ", " + vecB.get(j) + ", " + vecA.get(i));
                    context.write(key, new Text(vecB.get(j) + ", " + vecA.get(i)));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //get all args
        if (otherArgs.length != 3) {
            System.err.println(otherArgs.length + " arguments were input instead of 3.");
            System.exit(2);
        }
        String input1Path = otherArgs[0];
        String input2Path = otherArgs[1];
        String outputPath = otherArgs[2];


        //job3
        //reduce side join of user,age and userdata.txt
        {
            conf = new Configuration();
            Job job3 = new Job(conf, "Final");
            job3.setJarByClass(ReduceJoin.class);
            job3.setReducerClass(JoinReducer.class);

            //set output type of key/value
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);

            //set input path to different mapper
            MultipleInputs.addInputPath(job3, new Path(input1Path), TextInputFormat.class, Mapper1.class);
            MultipleInputs.addInputPath(job3, new Path(input2Path), TextInputFormat.class, Mapper2.class);

            //set path for output
            FileOutputFormat.setOutputPath(job3, new Path(outputPath));

            System.exit(job3.waitForCompletion(true)? 0 : 1);
        }

    }
}
