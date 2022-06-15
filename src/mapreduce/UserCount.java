package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class UserCount {
	private static LongWritable count= new LongWritable(1);
	public static class Map extends Mapper<LongWritable,Text,Text,LongWritable>{
		private Text userText = new Text();
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String temp = value.toString();
			String[] split = temp.split("\t");
			userText.set(split[1]);
			context.write(userText, count);
		}
		
	}
	public static class Reduce extends Reducer<Text,LongWritable,Text,LongWritable>{
		private LongWritable result = new LongWritable();
		public void reduce(Text key,Iterable <LongWritable>values,Context context) throws IOException, InterruptedException{
			Long sums =0L;
			for(LongWritable s :values) {
				sums+=s.get();
		}
			this.result.set(sums);
			context.write(key, this.result);
	}
}
	public static void UCount() throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS","hdfs://Master:9000");
		String[] otherArgs=new String[]{"test1","test4"};
		if(otherArgs.length!=2){
			System.err.println("Usage:UserCount<test1><test4>");
			System.exit(2);
		}
		Job job=Job.getInstance(conf,"UserCount");
		job.setJarByClass(UserCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
		job.waitForCompletion(true);
//		System.exit(job.waitForCompletion(true)?0:1);
	}
}
