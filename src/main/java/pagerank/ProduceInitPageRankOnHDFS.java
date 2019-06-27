import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by wangyuhan on 6/24/19.
 */
public class ProduceInitPageRankOnHDFS {
    public static class PageRankVecMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            String[] fromTo = value.toString().trim().split("\t");
            if(value.toString().trim().startsWith("#") || fromTo.length<2 || fromTo[1].trim().equals("")) return;
            try {
                context.write(new IntWritable(Integer.parseInt(fromTo[0])),new IntWritable(1));
                context.write(new IntWritable(Integer.parseInt(fromTo[1])),new IntWritable(1));
            }catch (NumberFormatException e){
                return;//Skip this line
            }
        }
    }

    public static class PageRankVecReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            context.write(key, new IntWritable(1));
        }
    }
    public static void main(String[] args) throws Exception{
        String inputPath = args[0];
        String pageRankVecPath = args[1];

        // PageRank Vector
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(ProduceInitPageRankOnHDFS.class);

        job.setMapperClass(PageRankVecMapper.class);
        job.setReducerClass(PageRankVecReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(pageRankVecPath));
        job.waitForCompletion(true);
    }
}
