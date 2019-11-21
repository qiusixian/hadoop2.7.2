package KeyValueTextInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KVMapper  extends Mapper<Text, Text, Text, IntWritable> {
	
	IntWritable v = new IntWritable(1);
	
	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		//写出
		context.write(key,v);
	}

}
