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




public class TimeCount {
	private static int startTime=0;
	private static int endTime=15;
	private static LongWritable count= new LongWritable(1);
	private static Text num = new Text("总次数");
	
	public static class Map extends Mapper<LongWritable,Text,Text,LongWritable>{
		private Text wordText = new Text();
		private Text webText = new Text();
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			int sum = 0;
			int flag =-1;
			int i =0;
			String temp = value.toString();
			String[] split = temp.split("\t");
			String[] times = split[0].split(":");
			String[] webName = split[4].split("/");
			String[] real = webName[0].split("\\.");
			for(i=0;i<real.length;i++) {
				if("com".equals(real[i])) {
					flag = i-1;
				}
			}
			if(flag == -1)
				flag=real.length-2;
			sum=sum+(Integer.parseInt(times[0])*60*60);
			sum=sum+(Integer.parseInt(times[1])*60);
			sum=sum+(Integer.parseInt(times[2]));
			if(sum>=TimeCount.startTime&&sum<=TimeCount.endTime) {
				wordText.set(split[2]);
				if(flag>=0)
				{
					webText.set(real[flag]);
					context.write(webText, count);
				}
				context.write(wordText,count);
				context.write(num,count);
			}
			
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
	public static void TCount(String time) throws IOException, ClassNotFoundException, InterruptedException{
		int sum,sum1;
		sum=sum1=0;
		String[] temp=time.split(",");
		String[] st= temp[0].split(":");
		String[] en= temp[1].split(":");
		sum=sum+(Integer.parseInt(st[0])*60*60);
		sum=sum+(Integer.parseInt(st[1])*60);
		sum=sum+(Integer.parseInt(st[2]));
		startTime=sum;
		sum1=sum1+(Integer.parseInt(en[0])*60*60);
		sum1=sum1+(Integer.parseInt(en[1])*60);
		sum1=sum1+(Integer.parseInt(en[2]));
		endTime=sum1;
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS","hdfs://Master:9000");
		String[] otherArgs=new String[]{"test1","test3"};
		if(otherArgs.length!=2){
			System.err.println("Usage:TimeCount<test1><test3>");
			System.exit(2);
		}
		Job job=Job.getInstance(conf,"TimeCount");
		job.setJarByClass(TimeCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
		job.waitForCompletion(true);
		//System.exit(job.waitForCompletion(true)?0:1);
	}
}
