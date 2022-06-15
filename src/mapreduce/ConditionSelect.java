package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class ConditionSelect {
	private static int startTime = 0;
	private static int endTime = 15;
	private static List<String> users= new ArrayList<String>();
	private static List<String> words = new ArrayList<String>();

	private static List<String> webs = new ArrayList<String>();
	public static class Map extends Mapper<LongWritable, Text, LongWritable,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			int sum = 0;
			int i =0;
			String record;
			String temp = value.toString();
			String[] split = temp.split("\t");
			String[] times = split[0].split(":");
			sum=sum+(Integer.parseInt(times[0])*60*60);
			sum=sum+(Integer.parseInt(times[1])*60);
			sum=sum+(Integer.parseInt(times[2]));
			//(sum>=ConditionSelect.startTime&&sum<=ConditionSelect.endTime)&&(user.contains(split[1]))&&(words.contains(split[2]))
			if((sum>=ConditionSelect.startTime&&sum<=ConditionSelect.endTime)&&(users.contains(split[1]))&&(words.contains(split[2]))) {
				for(i=0;i<webs.size();i++) {
					record = webs.get(i);
					if(split[4].contains(record)) {
						context.write(key, value);
						break;
					}
				}
				
			}
		}
	}
	public static class Reduce extends Reducer<LongWritable,Text,LongWritable,Text>{
		public void reduce(LongWritable key,Iterable <Text>values,Context context) throws IOException, InterruptedException{
			for(Text s:values)
			context.write(key, s);
		}
	}

	public static void ConSelect(String time,String user,String word,String url) throws IOException, ClassNotFoundException, InterruptedException{
		int sum,sum1,i;
		sum=sum1=0;
		String[] temp=time.split(",");
		String[] temp1=user.split(",");
		String[] temp2=word.split(",");
		String[] temp3=url.split(",");
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
		for(i=0;i<temp1.length;i++)
			users.add(temp1[i]);
		for(i=0;i<temp2.length;i++)
			words.add(temp2[i]);
		for(i=0;i<temp3.length;i++)
			webs.add(temp3[i]);
		startJob();

	}
	public static void startJob()throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS","hdfs://Master:9000");
		String[] otherArgs=new String[]{"test1","test2"};
		if(otherArgs.length!=2){
			System.err.println("Usage:ConditionSelect <test1><test2>");
			System.exit(2);
		}
		Job job=Job.getInstance(conf,"ConditionSelect");
		job.setJarByClass(ConditionSelect.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
		job.waitForCompletion(true);
	}
}
