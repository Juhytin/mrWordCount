package cn.zhuhq;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class wordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	int sum ;
	IntWritable v = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
//		累加求和
		sum = 0;
		
		for (IntWritable count : values) {
			sum += count.get();
		}
//		输出
		v.set(sum);
		context.write(key, v);
	}
}
