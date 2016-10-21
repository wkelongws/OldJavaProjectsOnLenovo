/**
  *****************************************
  *****************************************
  ************* combine *************
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class combine extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new combine(), args);
		System.exit(res); 
		
	} // End main
	
	public static int interval = 5;	
	
	public int run ( String[] args ) throws Exception {
		
		//String input = "NDOR/NB_1.csv"; 
		String input = "NDOR/speedcrash_I80EB2008.txt";    // Change this accordingly
		//String temp = "DesMoinesInrix/Data_SlidingWindow_temp";      // Change this accordingly
		//String output = "NDOR/NB_1"; 
		String output = "NDOR/speedcrash_I80EB2008";  		// Change this accordingly
		
		int reduce_tasks = 1;  // The number of reduce tasks that will be assigned to the job
		
		/***
		
		
		
		***/
		
		Configuration conf = new Configuration();
		
		// pass value to mapper and reducer
		//conf.set("test", "123");
		
		
		// Create job for round 1: round 1 counts the frequencies for each bigram
		
		// Create the job
		Job job_one = new Job(conf, "combine Program Round One"); 
		
		// Attach the job to this Driver
		job_one.setJarByClass(combine.class); 
		
		// Fix the number of reduce tasks to run
		// If not provided, the system decides on its own
		job_one.setNumReduceTasks(reduce_tasks);
		
		job_one.setMapOutputKeyClass(LongWritable.class); 
		job_one.setMapOutputValueClass(Text.class); 
		job_one.setOutputKeyClass(IntWritable.class);
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
		// Create job for round 2: round 2 groups all the bigrams by their initial letters and choose the most frequent bigram 
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
		
		Job job_two = new Job(conf, "combine Program Round Two"); 
		job_two.setJarByClass(combine.class); 
		//job_two.setNumReduceTasks(reduce_tasks); 
		
		job_two.setMapOutputKeyClass(IntWritable.class); 
		job_two.setMapOutputValueClass(Text.class); 
		job_two.setOutputKeyClass(NullWritable.class);
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
		FileOutputFormat.setOutputPath(job_two, new Path(output));
		
		// Run the job
		job_two.waitForCompletion(true); 
		***/
		return 0;
	
	} // End run
	
	// The Map Class
	// The input to the map method would be a LongWritable (long) key and Text (String) value
	// Notice the class declaration is done with LongWritable key and Text value
	// The TextInputFormat splits the data line by line.
	// The key for TextInputFormat is nothing but the line number and hence can be ignored
	// The value for the TextInputFormat is a line of text from the input
	// The map method can emit data using context.write() method
	// However, to match the class declaration, it must emit Text as key and IntWribale as value
	public static class Map_One extends Mapper<LongWritable, Text, LongWritable, Text>  {
		
		// Reuse objects to save overhead of object creation.
				
		// The map method 
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
			
			//
			//Configuration conf = context.getConfiguration();
			//String param = conf.get("test");
			
			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			//String line = value.toString();
			
			// Split the input line by tabs 
			//String[] inputlines = line.split("	");	
			
			
			
			context.write(key, value);		
								
			
		} // End method "map"
		
	} // End Class Map_One
	
	
	// The reduce class
	// The key is Text and must match the datatype of the output key of the map method
	// The value is IntWritable and also must match the datatype of the output value of the map method
	public static class Reduce_One extends Reducer<LongWritable, Text, Text, Text>  {
		
		int rownumber = 0;
		//private HashMap<Integer, String> onedetector = new HashMap<Integer, String>();
		
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(LongWritable key, Iterable<Text> values, Context context) 
											throws IOException, InterruptedException  {
			
						
			for (Text value : values) {
				rownumber++;
				context.write(new Text(Integer.toString(rownumber)+","), new Text(key.toString()+","+value.toString()));
			}
			
								
	    		
		    				
			
		} // End method "reduce" 
		
	} // End Class Reduce_One
	
	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, IntWritable, Text>  {
				
 		public void map(LongWritable key, Text value, Context context) 
 				throws IOException, InterruptedException  {
 			
 			String line = value.toString();
			
			// Split the input line by tabs 
			String[] v = line.split("	");			
			
			int timestamp = Integer.parseInt(v[2])*1000+Integer.parseInt(v[3]);
			
			context.write(new IntWritable(timestamp), new Text(line));		
 				
 			
 		}  // End method "map"
 		
 	}  // End Class Map_Two
 	
 	// The second Reduce class
 	public static class Reduce_Two extends Reducer<IntWritable, Text, NullWritable, Text>  {
 		 		
 		private HashMap<Integer, String> onetimestamp = new HashMap<Integer, String>();
 		
 		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
 				throws IOException, InterruptedException  {
 			
 			// int num = 24*60/interval;
			
			for (Text value : values) {
	 	           String[] v = value.toString().split("	");
	 	           int detector = Integer.parseInt(v[1]);
	 	          onetimestamp.put(detector, value.toString());		// each time put one observation into treemap
			}
 			
			for (int k : onetimestamp.keySet()) 
			{
				String n = onetimestamp.get(k);
				if(onetimestamp.containsKey(k-1))
				{
					String[] upstream = onetimestamp.get(k-1).split("	");
					String Speed_Upstream = upstream[6] + "	" + upstream[7] + "	" + upstream[8];
					n = n + "	" + Speed_Upstream;
					
					if(onetimestamp.containsKey(k+1))
					{
						String[] downstream = onetimestamp.get(k-1).split("	");
						String Speed_Downstream = downstream[6] + "	" + downstream[7] + "	" + downstream[8];
						n = n + "	" + Speed_Downstream;
						
						context.write(NullWritable.get(), new Text(n));
						
					}
					
				}
			}
			
 			
			//context.write(key, out);
			
		}  // End method "reduce"
		
	}  // End Class Reduce_Two
	
}

