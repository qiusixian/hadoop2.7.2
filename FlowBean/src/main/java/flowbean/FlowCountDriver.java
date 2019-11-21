package flowbean; /**
 * @ClassName FlowCountDriver
 * @Description TODO
 * @Author qiuhaijun
 * @Date 2019/11/15 20:24
 * @Version 1.0
 **/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCountDriver {
    public static void main(String[] args)  throws Exception{

        args = new String [] {"D:/BigData/test/input/inputPhone/", "D:/BigData/test/output1"};

        //1. 创建Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2. 关联Jar
        job.setJarByClass(FlowCountDriver.class);

        //3. 关联Mapper 和 Reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //4. 设置Mapper输出的key 和 value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5. 设置最终输出的key  和  value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6. 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7. 提交Job
        job.waitForCompletion(true);
    }
}
