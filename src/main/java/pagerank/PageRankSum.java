import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
/**
 * Created by wangyuhan on 6/17/19.
 */
public class PageRankSum {
    public static class PageRankMapper extends Mapper<Object, Text, IntWritable, FloatWritable>{
        public float beta;

        @Override
        public void setup(Context context){
            Configuration conf = new Configuration();
            beta = conf.getFloat("beta", 0.2f);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] pageRank = value.toString().trim().split("\t");
            String page = pageRank[0];
            float rank = Float.parseFloat(pageRank[1].trim()) * (1-beta);
            context.write(new IntWritable(Integer.parseInt(page)), new FloatWritable(rank));
        }
    }

    public static class CompensatoryMapper extends Mapper<Object, Text, IntWritable, FloatWritable>{
        public float beta;

        @Override
        public void setup(Context context){
            Configuration conf = new Configuration();
            beta = conf.getFloat("beta", 0.2f);
        }


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] pageRank = value.toString().trim().split("\t");
            String page = pageRank[0];
            float rank = Float.parseFloat(pageRank[1].trim()) * beta;
            context.write(new IntWritable(Integer.parseInt(page)), new FloatWritable(rank));
        }
    }

    public static class PageRankReducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>{
        @Override
        public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
            float sum=0f;
            for(FloatWritable value:values){
                sum += value.get();
            }
            DecimalFormat df = new DecimalFormat("#.0000000");
            sum = Float.valueOf(df.format(sum));
            context.write(key, new FloatWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{
        // args0: dir of input pr values from last step
        // args1: dir of input pr values from last mapred task
        // args2: dir of output pr values
        // args3: beta

        Configuration conf = new Configuration();
        conf.setDouble("beta", Float.parseFloat(args[3]));
        Job job = Job.getInstance(conf);
        job.setJarByClass(PageRankSum.class);

        ChainMapper.addMapper(job, PageRankMapper.class, Object.class, Text.class, IntWritable.class, FloatWritable.class, conf);
        ChainMapper.addMapper(job, CompensatoryMapper.class, IntWritable.class, FloatWritable.class, IntWritable.class, FloatWritable.class, conf);
        job.setReducerClass(PageRankReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PageRankMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CompensatoryMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
