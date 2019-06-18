import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
/**
 * Created by wangyuhan on 6/17/19.
 */
public class PageRankTransition {
    public static class TransitionMatrixMapper extends Mapper<Object, Text, Text, Text>{
        @Override
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            // fromTo: ["pageID", "pageID,pageID"]
            String[] fromTo = value.toString().trim().split("\t");
            if(fromTo.length<2 || fromTo[1].trim().equals("")) return;

            String source = fromTo[0];
            String[] targets = fromTo[1].split(",");
            for(String t: targets){
                context.write(new Text(source), new Text(t+"="+targets.length));
            }
        }
    }

    public static class PageRankStateMapper extends Mapper<Object, Text, Text, Text>{
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] sourceState = value.toString().trim().split("\t");
            context.write(new Text(sourceState[0]), new Text(sourceState[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            // values: [PageRankState, TransitionMatrixComponents]
            List<String> components = new ArrayList<String>();
            Double state = 0.;
            for(Text v:values){
                String value = v.toString();
                if(value.contains("=")){
                    // Condition of TransitionMatrixComponents
                    components.add(value);
                }
                else{
                    state = Double.parseDouble(value);
                }
            }

            for(String value:components){
                String target = value.split("=")[0];
                Double pageNum = Double.parseDouble(value.split("=")[1]);
                context.write(new Text(target), new Text(String.valueOf(state/pageNum)));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(PageRankTransition.class);

        ChainMapper.addMapper(job, TransitionMatrixMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PageRankStateMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMatrixMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PageRankStateMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
