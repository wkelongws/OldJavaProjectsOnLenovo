/**
  *****************************************
  *****************************************
  ************* InrixWaveT *************
  *****************************************
  *****************************************
  */

import java.io.*;
import java.lang.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class amplitudeA extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new amplitudeA(), args);
		System.exit(res); 
		
	} // End main
		
	public int run ( String[] args ) throws Exception {
		
		//String input1 = "InrixWaveT/result.txt";    // Change this accordingly	
		String input1 = "freeway/result.txt";    // Change this accordingly	
		//String output = "Yaw/amplitude/arterial";  		// Change this accordingly
		String output = "Yaw/amplitude/freeway";  		// Change this accordingly
		
		int reduce_tasks = 16;  // The number of reduce tasks that will be assigned to the job
		
		
		Configuration conf = new Configuration();
						
		// Create job for round 1: round 1 counts the frequencies for each bigram
		
		// Create the job
		Job job_one = new Job(conf, "InrixWaveT Program Round One"); 
		
		// Attach the job to this Driver
		job_one.setJarByClass(amplitudeA.class); 
		
		// Fix the number of reduce tasks to run
		// If not provided, the system decides on its own
		job_one.setNumReduceTasks(reduce_tasks);
		
		job_one.setMapOutputKeyClass(Text.class); 
		job_one.setMapOutputValueClass(Text.class); 
		job_one.setOutputKeyClass(Text.class);
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
		FileInputFormat.addInputPath(job_one, new Path(input1)); 
		// FileInputFormat.addInputPath(job_one, new Path(input2)); 
		// FileInputFormat.addInputPath(job_one, new Path(another_input_path)); // This is legal
		
		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job_one, new Path(output));
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path)); // This is not allowed
		
		// Run the job
		job_one.waitForCompletion(true); 
				
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
	public static class Map_One extends Mapper<LongWritable, Text, Text, Text>  {
		
		// Reuse objects to save overhead of object creation.
				
		// The map method 
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
			
						
			Map<String, String> Inrix = new HashMap<String, String>();
			Map<String, String> Wave = new HashMap<String, String>();
									
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString();
			
			// Split the input line by tabs 
			String[] kv = line.split("\t");	
			String k = kv[0];
			String v = kv[1];
			String ts = v.split(",")[0];
			int hh = Integer.parseInt(ts.split(":")[0]);
			int mm = Integer.parseInt(ts.split(":")[1]);
			int min15 = (hh*60+mm)/15;
						
			context.write(new Text(k+"|"+Integer.toString(min15)), new Text(v));													
			
		} // End method "map"
		
	} // End Class Map_One
	
	
	// The reduce class
	// The key is Text and must match the datatype of the output key of the map method
	// The value is IntWritable and also must match the datatype of the output value of the map method
	public static class Reduce_One extends Reducer<Text, Text, Text, Text>  {
				
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<Text> values, Context context) 
											throws IOException, InterruptedException  {
			Double diff = 0.0;
			int largediff = 0;
			int count = 0;
			int volumesum = 0;
			double trucksum = 0;
						
			for (Text value : values) 
			{
	 	        if (value!=null)
	 	        {
	 	        	String[] v = value.toString().split(",");
	 	        	diff = Integer.parseInt(v[1])/1.6-Integer.parseInt(v[10]);
	 	        	if (diff<=-10|diff>=10)
	 	        	{
	 	        		largediff++;
	 	        	}
	 	        	count++;
	 	        	volumesum+=Integer.parseInt(v[2]); 
	 	        	trucksum+=Double.parseDouble(v[3]); 
	 	        }
	 	           
			}
			
			Double timepercent = (double)largediff/(double)count;
	        Double avgvolume = (double)volumesum/(double)count;
	        Double avgtruck = trucksum/(double)count;
			
			String[] k = key.toString().split("\\|");
			int from = Integer.parseInt(k[2]);
			int to = from+1;
			int hh_from = from/4;
			int mm_from = from%4*15;
			int hh_to = to/4;
			int mm_to = to%4*15;
			
			String interval = Integer.toString(hh_from)+":"+Integer.toString(mm_from)+":00-"+Integer.toString(hh_to)+":"+Integer.toString(mm_to)+":00";
			
			context.write(key, new Text(interval+","+Double.toString(timepercent)+","+Double.toString(avgvolume)+","+Double.toString(avgtruck)));
					
						
			
		} // End method "reduce" 
		
	} // End Class Reduce_One
		
}

