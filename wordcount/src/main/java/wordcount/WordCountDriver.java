package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * @ClassName WordCountDriver
 * @Description TODO
 * @Author qiuhaijun
 * @Date 2019/11/15 17:50
 * @Version 1.0
 **/
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"D:/BigData/test/input/inputWord/hello.txt", "D:/BigData/test/output13"};

        //1.创建job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //2.通过反射方式关联jar
        job.setJarByClass(WordCountDriver.class);

        //3.通过反射方式关联Mapper和Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4.设置Mapper的输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置最终的MapReduce输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /*设置使用InputFormat,默认使用的是TextInputFormat,这里使用CombineTextInputFormat
        可以减少大量小文件情况下的切片数。*//*
        job.setInputFormatClass(CombineTextInputFormat.class);
        *//*设置CombineTextInputFormat虚拟存储大小，默认4M*//*
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);*/
        //7.提交作业
        job.waitForCompletion(true);

    }
}
