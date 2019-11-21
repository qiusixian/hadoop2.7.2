package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName WordCountMapper
 * @Description TODO
 * @Author qiuhaijun
 * @Date 2019/11/15 16:47
 * @Version 1.0
 **/
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

   Text k =  new Text();
    IntWritable v = new IntWritable(1);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取一行数据，转换成Java中的类型
        String line = value.toString();
        //按文件中的分割格式，切分这一行数据
        String[] split = line.split(" ");
        //遍历切分出的数据
        for (String s : split) {
        //因为输出的k和v要使用hadoop中的类型。所以要包装字符串为对应的类型。
            //一种类型要包装成其他类型，即把该类型包装成另一类型的属性。可以通过另一类型new对象的形式，作为参数传进构造方法去
            //也可以另一对象通过调用set方法传进去，
            k.set(s);

            context.write(k,v);
        }
    }
}
