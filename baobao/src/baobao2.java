/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 3 - baobao2 - Shuo Wang **
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



public class baobao2 extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new baobao2(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = "MarchINRIX/temp3/part-r-00000";    // Input
		
		String output = "MarchINRIX/result";   // Round three/final output
		
		int reduce_tasks = 8;  // The number of reduce tasks that will be assigned to the job
		Configuration conf = new Configuration();
		
		// Create job for round 1: round 1 summarize the all the patents A cites and all the patents A cited by for A.
		
		// Create the job
		Job job_one = new Job(conf, "baobao2 Program Round One"); 	
		
		// Attach the job to this Driver
		job_one.setJarByClass(baobao2.class); 
		
		// Fix the number of reduce tasks to run
		// If not provided, the system decides on its own
		job_one.setNumReduceTasks(reduce_tasks);		
		
		// The datatype of the Output Key 
		// Must match with the declaration of the Reducer Class
		job_one.setOutputKeyClass(Text.class); 		
		
		// The datatype of the Output Value 
		// Must match with the declaration of the Reducer Class
		job_one.setOutputValueClass(Text.class);
		
		// The class that provides the map method
		job_one.setMapperClass(Map_One.class); 
		
		// The class that provides the reduce method
		job_one.setReducerClass(Reduce_One.class);
		
		// Decides how the input will be split
		// We are using TextInputFormat which splits the data line by line
		// This means each map method receives one line as an input
		job_one.setInputFormatClass(TextInputFormat.class);  
		
		// Decides the Output Format
		job_one.setOutputFormatClass(TextOutputFormat.class);
		
		// The input HDFS path for this job
		// The path can be a directory containing several files
		// You can add multiple input paths including multiple directories
		FileInputFormat.addInputPath(job_one, new Path(input)); 
		// FileInputFormat.addInputPath(job_one, new Path(another_input_path)); // This is legal
		
		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job_one, new Path(output));
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path)); // This is not allowed
		
		// Run the job
		job_one.waitForCompletion(true); 
		
		/***
		// Create job for round 2: round 2 finds all the 1-hop and 2-hop citations for A
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
		
		Job job_two = new Job(conf, "Driver Program Round Two"); 
		job_two.setJarByClass(baobao2.class); 
		job_two.setNumReduceTasks(reduce_tasks); 
		
		job_two.setOutputKeyClass(Text.class); 
		job_two.setOutputValueClass(Text.class);
		
		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_two.setMapperClass(Map_Two.class); 
		job_two.setReducerClass(Reduce_Two.class);
		
		job_two.setInputFormatClass(TextInputFormat.class); 
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp)); 
		FileOutputFormat.setOutputPath(job_two, new Path(temp1));
		
		// Run the job
		job_two.waitForCompletion(true); 
		
		
		// Create job for round 3: output the top 10 patent with most citations
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
				
		Job job_Three = new Job(conf, "Driver Program Round Three"); 
		job_Three.setJarByClass(baobao2.class); 
		job_Three.setNumReduceTasks(reduce_tasks); 
				
		job_Three.setOutputKeyClass(NullWritable.class); 
		job_Three.setOutputValueClass(Text.class);
				
		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_Three.setMapperClass(Map_Three.class); 
		job_Three.setReducerClass(Reduce_Three.class);
			
		job_Three.setInputFormatClass(TextInputFormat.class); 
		job_Three.setOutputFormatClass(TextOutputFormat.class);
				
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_Three, new Path(temp1)); 
		FileOutputFormat.setOutputPath(job_Three, new Path(output));
				
		// Run the job
		job_Three.waitForCompletion(true); 		
		***/
	
		return 0;
	
	} // End run
	
	// The round one: round 1 summarize the all the patents A cites and all the patents A cited by for A.

	public static class Map_One extends Mapper<LongWritable, Text, Text, Text>  {		
	
		// The map method 
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
			
			String line = value.toString();
			
			// Split the input line by tabs 
			String[] inputlines = line.split("\t");	
			String XD = inputlines[0];
			String Info = inputlines[1];
			String Index = inputlines[2];
			String road = inputlines[4];
			String direc = inputlines[5];
			
			String length = Info.split(";")[0];
			String Date = Info.split(";")[1];
			String hour = Info.split(";")[2];
			
			
			context.write(new Text(road+":"+direc+";"+Date+":"+hour), new Text(XD+","+length+","+Index));
			
			
		} // End method "map"
		
	} // End Class Map_One
	
	
	// The reduce class	
	public static class Reduce_One extends Reducer<Text, Text, Text, Text>  {		
		
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<Text> values, Context context) 
											throws IOException, InterruptedException  {
			
			int congestion = 0;
			int RoadIndex = 0;
			double length = 0.0;
			double congestedlength = 0.0;			
			double congestedpercent = 0.0;
			
			for (Text value : values) 
			{
	 	        if (value!=null)
	 	        {
	 	        	String[] v = value.toString().split(",");
	 	        	congestion = congestion + Integer.parseInt(v[2]);
					length = length + Double.parseDouble(v[1]);
					congestedlength = congestedlength + Integer.parseInt(v[2])*Double.parseDouble(v[1]);
	 	        }	 	           
			}
		
			if (congestion>0)
			{
				RoadIndex = 1;
			}
			
			congestedpercent = congestedlength/length;
			
			context.write(key, new Text(Integer.toString(RoadIndex)+","+Double.toString(congestedpercent)));		
			
			
		} // End method "reduce" 
		
	} // End Class Reduce_One
	
	
	// The Round Two: round 2 finds all the 1-hop and 2-hop citations for A
	// The second Map Class
		public static class Map_Two extends Mapper<LongWritable, Text, Text, Text>  {		
		
		
		
 		public void map(LongWritable key, Text value, Context context) 
 				throws IOException, InterruptedException  { 			
 			
 			String line = value.toString();
			String[] lines = line.split("\t");
			context.write(new Text(lines[0]),new Text(lines[1]));
 			
 		}  // End method "map"
 		
 	}  // End Class Map_Two
 	
 	// The second Reduce class
 	public static class Reduce_Two extends Reducer<Text, Text, Text, Text>  { 		
 				
 		public void reduce(Text key, Iterable<Text> values, Context context) 
 				throws IOException, InterruptedException  {
 			
 			int sum = 0 ;
			
			for (Text value : values) 
			{
	 	        if (value!=null)
	 	        {
	 	        	String v = value.toString();
	 	        	sum = sum + Integer.parseInt(v); 	
	 	        	
	 	        }
	 	           
			}
			
			if (sum ==0)
			{
				context.write(key,new Text("0"));
			}
			else 
			{
				context.write(key,new Text("1"));
			}	
			
		}  // End method "reduce"
		
	}  // End Class Reduce_Two
 	
 	// The Round Three: output the top 10 patent with most citations
public static class Map_Three extends Mapper<LongWritable, Text, NullWritable, Text>  {
		
		
 		public void map(LongWritable key, Text value, Context context) 
 				throws IOException, InterruptedException  { 			
 			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
 			context.write(NullWritable.get(), value);				// put everything to a same reducer with uniform key
 			
 		}  // End method "map"
 		
 	}  // End Class Map_Three
 	
 	// The second Reduce class
 	public static class Reduce_Three extends Reducer<NullWritable, Text, NullWritable, Text>  { 
 		
 		// use treemap to keep updating the top 10 while tracking each input		
 		private TreeMap<Integer, String> frequentcitations = new TreeMap<Integer, String>(); 		
 		
 		public void reduce(NullWritable key, Iterable<Text> values, Context context) 
 				throws IOException, InterruptedException  { 			

 	        for (Text value : values) {
 	           String[] v = value.toString().split("\t");
 	           int frequency = Integer.parseInt(v[1]);
 	           frequentcitations.put(frequency, value.toString());		// each time put one observation into treemap

 	           if (frequentcitations.size() > 10) {

 	            	frequentcitations.remove(frequentcitations.firstKey());		// when 11 elements in the list, remove the one with smallest frequency
 	            }
 	        }

 	        for (String t : frequentcitations.values()) {
 	            context.write(NullWritable.get(), new Text(t));			// output everything in the list (top 10)
 	        }
			
		}  // End method "reduce"
		
	}  // End Class Reduce_Three
 	
 	
 	
 	
 	
 	
 	
	
}

