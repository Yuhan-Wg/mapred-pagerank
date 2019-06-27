package format;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import utils.HadoopParams;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by wangyuhan on 6/25/19.
 */
public class StringIntegerMap {
    public static class StringToIntegerMapper extends Mapper<Object, Text,IntWritable, Text>{
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{
            String[] strings = HadoopParams.mapLine(value.toString());
            for(String v: strings){
                context.write(
                        new IntWritable(HadoopParams.hashCode(v)>>16),new Text(v)
                );
            }
        }
    }

    public static class StringToIntegerCombiner extends Reducer<IntWritable, Text, IntWritable, Text>{
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            Set<Text> textSet = new HashSet<Text>();
            for(Text v: values){
                if(!textSet.contains(v)){
                    textSet.add(v);
                    context.write(key,v);
                }
            }
        }
    }

    public static class StringToIntegerPartitioner extends Partitioner<IntWritable, Text>{
        public int getPartition(IntWritable key, Text value, int numOfReduceTasks){
            return key.get()%numOfReduceTasks;
        }
    }

    public static class StringToIntegerReducer extends Reducer<IntWritable, Text, IntWritable,Text>{
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IndexOutOfBoundsException{
            int nextCode = 0;
            Map<String, Integer>hashMap = new HashMap<String, Integer>();
            for(Text t:values){
                String s = t.toString();
                if(!hashMap.containsKey(s)){
                    hashMap.put(s, nextCode++);
                    if(nextCode >= 0xFFFF){
                        throw new IndexOutOfBoundsException("The count of Strings is out of bounds");
                    }
                }
            }

            for(String s:hashMap.keySet()){
                int code = (key.get()<<16) + hashMap.get(s);
            }
        }
    }
}
