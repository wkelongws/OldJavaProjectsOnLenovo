/**
  *****************************************
  *****************************************
  * by Shuo Wang **
  *****************************************
  *****************************************
  */

import java.io.*;
import java.lang.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class DataFilter1 extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new DataFilter1(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = "Shuo/10232015.txt";    // Input
//		String input1 = "Wavetronix_downloadbyShuo/10172015.txt";
//		String input2 = "Wavetronix_downloadbyShuo/10182015.txt";
//		String input3 = "Wavetronix_downloadbyShuo/10192015.txt";
//		String input4 = "Wavetronix_downloadbyShuo/10202015.txt";
//		String input5 = "Wavetronix_downloadbyShuo/10212015.txt";
//		String input6 = "Wavetronix_downloadbyShuo/10222015.txt";
		String output = "Shuo/filtered";       // Round one output
		//String temp1 = "/scr/shuowang/lab3/exp2/temp1/";     // Round two output
		//String output1 = "/scr/shuowang/lab3/exp2/output1/";   // Round three/final output
		//String output2 = "/scr/shuowang/lab3/exp2/output2/";   // Round three/final output
		
		int reduce_tasks = 12;  // The number of reduce tasks that will be assigned to the job
		Configuration conf = new Configuration();		
		
		Job job_one = new Job(conf, "Exp2 Program Round One"); 
		job_one.setJarByClass(DataFilter1.class); 
		job_one.setNumReduceTasks(reduce_tasks);		
//		job_one.setMapOutputKeyClass(Text.class); 
//		job_one.setMapOutputValueClass(Text.class); 
		job_one.setOutputKeyClass(NullWritable.class);         
		job_one.setOutputValueClass(Text.class);
		job_one.setMapperClass(Map_One.class); 
//		job_one.setReducerClass(Reduce_One.class);
		job_one.setInputFormatClass(TextInputFormat.class); 
		job_one.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job_one, new Path(input)); 
//		FileInputFormat.addInputPath(job_one, new Path(input1));
//		FileInputFormat.addInputPath(job_one, new Path(input2));
//		FileInputFormat.addInputPath(job_one, new Path(input3));
//		FileInputFormat.addInputPath(job_one, new Path(input4));
//		FileInputFormat.addInputPath(job_one, new Path(input5));
//		FileInputFormat.addInputPath(job_one, new Path(input6));
		FileOutputFormat.setOutputPath(job_one, new Path(output));
		job_one.waitForCompletion(true); 
			
		return 0;
	
	} // End run
	
	
	public static class Map_One extends Mapper<LongWritable, Text, NullWritable, Text>  {	
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
			String line = value.toString();
			String[] nodes = line.split(",");
						
			int countsum = 0;
			int smallcountsum = 0;
			int middlecountsum = 0;
			int largecountsum = 0;
			
			if(nodes.length>6)
			{
				int numlanes = Integer.parseInt(nodes[5]);
				
				for(int i=0;i<numlanes;i++)
				{
					if (i*11+10<=nodes.length)
					{
					String count = nodes[i*11+7];
					String speed = nodes[i*11+10];
					String occupancy = nodes[i*11+9];
					String smallcount = nodes[i*11+11];
					String middlecount = nodes[i*11+13];
					String largecount = nodes[i*11+15];
					
					if(count.equals("null"))
					{
						count = "0";
					}
					if(smallcount.equals("null"))
					{
						smallcount = "0";
					}
					if(middlecount.equals("null"))
					{
						middlecount = "0";
					}
					if(largecount.equals("null"))
					{
						largecount = "0";
					}
					
					countsum += Integer.parseInt(count);
					smallcountsum += Integer.parseInt(smallcount);
					middlecountsum += Integer.parseInt(middlecount);
					largecountsum += Integer.parseInt(largecount);
						
				
					}
				}

				}
								
				if (smallcountsum + middlecountsum +largecountsum>0)
				{
				context.write(NullWritable.get(), value);
				}									
		} // End method "map"
		
	} // End Class Map_One	
	
	public static class Reduce_One extends Reducer<Text, Text, NullWritable, Text>  {
		public void reduce(Text key, Iterable<Text> values, Context context) 
											throws IOException, InterruptedException  {
			int totalcount = 0;
			double totalspeed = 0.0;
			double totaloccupancy = 0.0;
			int num = 0;
			
			for (Text val : values) {
				
				num++;
				String data = val.toString();
				
				String[] data1 = data.split(",");
				
				totalcount += Integer.parseInt(data1[1]);
				totalspeed += Double.parseDouble(data1[0])*Integer.parseInt(data1[1]);
				totaloccupancy += Double.parseDouble(data1[2]);		
			}
			
			double meanspeed = 0.0;
			if(totalcount>0)
			{
				meanspeed = totalspeed/totalcount;	
			}
			double meanoccupancy = totaloccupancy/num;
			
			context.write(NullWritable.get(),new Text(key.toString()+","+Double.toString(meanspeed)+","+Integer.toString(totalcount)+","+Double.toString(meanoccupancy)));
			
		} // End method "reduce" 
		
	} // End Class Reduce_One
	

 	
}
