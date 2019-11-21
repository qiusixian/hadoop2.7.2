package flowbean; /**
 * @ClassName FlowCountMapper
 * @Description TODO
 * @Author qiuhaijun
 * @Date 2019/11/15 20:25
 * @Version 1.0
 **/

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    //输出的k,v在这定义
    Text k  = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //获取一行数据并转成Java字符串进行操作
        // 7 	13560436666	120.196.100.99		1116		 954			200
        String line = value.toString();

        //切分
        String [] splits = line.split("\t");

        //获取手机号，这个手机号作为key输出
        String keyString  = splits[1];

        //value，将上下行流量和总流量封装到对象中作为value输出。
        v.setUpFlow(Long.parseLong(splits[splits.length-3]));
        v.setDownFLow(Long.parseLong(splits[splits.length-2]));
        v.setSumFlow(Long.parseLong(splits[splits.length-3]) + Long.parseLong(splits[splits.length-2]));


        //写出（hadoop数据类型方便序列化和反序列化）
        k.set(keyString);
        context.write(k, v);
    }
}

