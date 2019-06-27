import com.google.protobuf.TextFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by wangyuhan on 6/24/19.
 */
public class ProduceTransitionMatrixOnHDFS {

    public static class TransitionMatrixMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
        @Override
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            String[] fromTo = value.toString().trim().split("\t");
            if(value.toString().trim().startsWith("#") || fromTo.length<2 || fromTo[1].trim().equals("")) return;
            try {
                context.write(new IntWritable(Integer.parseInt(fromTo[0])), new IntWritable(Integer.parseInt(fromTo[1])));
            }catch (NumberFormatException e){
                return;//Skip this line
            }
        }

    }

    public static class TransitionMatrixReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text>{
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{
            StringBuilder sb = new StringBuilder();
            for(IntWritable v:values){
                String value = v.toString();
                sb.append(v+",");
            }
            context.write(key, new Text(sb.toString().substring(0, sb.toString().length()-1)));
        }
    }

    public static void main(String[] args) throws Exception{
        String inputPath = args[0];
        String transitionMatrixPath = args[2];

        // Transition Matrix
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(ProduceTransitionMatrixOnHDFS.class);

        job.setMapperClass(TransitionMatrixMapper.class);
        job.setReducerClass(TransitionMatrixReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(transitionMatrixPath));
        job.waitForCompletion(true);
    }
}
