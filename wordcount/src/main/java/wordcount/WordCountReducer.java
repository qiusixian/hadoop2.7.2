package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName WordCountReducer
 * @Description TODO
 * @Author qiuhaijun
 * @Date 2019/11/15 17:40
 * @Version 1.0
 **/
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
        //一组数据:atguigu 1
        //		 atguigu 1

        int sum = 0 ;

        for (IntWritable value : values) {

            sum += value.get() ;
        }

        //写出
        v.set(sum);
        context.write(key, v);

    }
}
