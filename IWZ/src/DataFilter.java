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



public class DataFilter extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new DataFilter(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = "Shuo/wavetronix/11112015.txt";    // Input
//		String input1 = "Shuo/wavetronix/11032015.txt";
//		String input2 = "Shuo/wavetronix/11042015.txt";
//		String input3 = "Shuo/wavetronix/11052015.txt";
//		String input4 = "Shuo/wavetronix/11062015.txt";
//		String input5 = "Shuo/wavetronix/11072015.txt";
//		String input6 = "Shuo/wavetronix/11082015.txt";
		String output = "Shuo/filtered";       // Round one output
		//String temp1 = "/scr/shuowang/lab3/exp2/temp1/";     // Round two output
		//String output1 = "/scr/shuowang/lab3/exp2/output1/";   // Round three/final output
		//String output2 = "/scr/shuowang/lab3/exp2/output2/";   // Round three/final output
		
		int reduce_tasks = 12;  // The number of reduce tasks that will be assigned to the job
		Configuration conf = new Configuration();		
		
		Job job_one = new Job(conf, "Exp2 Program Round One"); 
		job_one.setJarByClass(DataFilter.class); 
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
			
			// Split the edge into two nodes 
			String[] nodes = line.split(",");
			
			int weightedspeedsum = 0;			
			int countsum = 0;			
			int occupancysum = 0;
			double avgoccupancy = 0.0;
			double avgspeed = 0.0;
			int smallcountsum = 0;
			int middlecountsum = 0;
			int largecountsum = 0;
			
			String date = nodes[1];
			String yy = date.substring(0,4);
			String m = date.substring(4,6);
			String dd = date.substring(6,8);
			String D = m+"/"+dd+"/"+yy;
			
			String time = nodes[2];
			String hh = time.substring(0,2);
			String mm = time.substring(2,4);
			String ss = time.substring(4,6);
			int minnum = Integer.parseInt(mm)/5;			
			
			
			/*if(nodes[4].equals("failed"))
			{
				context.write(new Text(nodes[0].trim()+","+D+","+hh+","+Integer.toString(minnum)), new Text("nocomma"));				
			}
			if(nodes[4].equals("off"))
			{
				context.write(new Text(nodes[0].trim()+","+D+","+hh+","+Integer.toString(minnum)), new Text("one,comma"));				
			}*/
			
			
			if(nodes.length>6)
			{
				int numlanes = Integer.parseInt(nodes[5]);
				int zerospeednonzerocountflag = 0;
				
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
					if(speed.equals("null"))
					{
						speed = "0";
					}
					if(Integer.parseInt(speed)<0)
					{
						speed = "0";
					}					
					if(occupancy.equals("null"))
					{
						occupancy = "0";
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
					weightedspeedsum += Integer.parseInt(count)*Integer.parseInt(speed);					
					occupancysum += Integer.parseInt(occupancy);
					smallcountsum += Integer.parseInt(smallcount);
					middlecountsum += Integer.parseInt(middlecount);
					largecountsum += Integer.parseInt(largecount);
					
					if (Integer.parseInt(count)>0 & Integer.parseInt(speed)==0)
					{
						zerospeednonzerocountflag++;
					}
					
					}
				}
				avgoccupancy = occupancysum/numlanes;
				if (countsum>0)
				{
					avgspeed = weightedspeedsum/1.6/countsum;
				}				
				/*context.write(new Text(nodes[0].trim()+","+D+","+hh+","+Integer.toString(minnum)), new Text(Double.toString(avgspeed)+","+Integer.toString(countsum)+","+Double.toString(avgoccupancy)));
				if (countsum!=smallcountsum+middlecountsum+largecountsum)
				{
					context.write(new Text(nodes[0].trim()+","+D+","+hh+","+Integer.toString(minnum)), new Text("th,ree,com,ma"));
				}*/
				if (zerospeednonzerocountflag>0)
				{
					context.write(NullWritable.get(), value);
				}		
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
