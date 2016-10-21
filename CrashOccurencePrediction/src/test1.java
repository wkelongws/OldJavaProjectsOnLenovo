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

public class test1 extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new test1(), args);
		System.exit(res); 
		
	} // End main
		
	public int run ( String[] args ) throws Exception {
		
		//String input1 = "InrixWaveT/Inrix/Inrix.txt";    // Change this accordingly
		String input1 = "InrixWaveT/Inrix/inrix/1-11/1-11.txt";    // Change this accordingly
		//String input1 = "iw/i.txt"; 
		//String input2 = "InrixWaveT/Wave/Wave.txt";    // Change this accordingly
		String input2 = "InrixWaveT/Wave/wavetronix/1-11/1-11.txt";    // Change this accordingly
		//String input2 = "iw/w.txt";   
		//String temp = "DesMoinesInrix/Data_SlidingWindow_temp";      // Change this accordingly
		String output = "InrixWaveT/result";  		// Change this accordingly
		//String output = "result"; 
		String matchtable = "InrixWaveT/relation.txt";
		
		int reduce_tasks = 1;  // The number of reduce tasks that will be assigned to the job
		
		// read matching table into memory		
		FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(matchtable))));
        String line;
        String IDs = "start";
        while ((line = br.readLine()) != null) {
        	IDs = IDs + ";" + line;            
        }
        br.close();             	
		
		Configuration conf = new Configuration();
		
		// pass value to mapper and reducer
		conf.set("match", IDs);
				
		// Create job for round 1: round 1 counts the frequencies for each bigram
		
		// Create the job
		Job job_one = new Job(conf, "InrixWaveT Program Round One"); 
		
		// Attach the job to this Driver
		job_one.setJarByClass(test1.class); 
		
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
		FileInputFormat.addInputPath(job_one, new Path(input2)); 
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
			
			//
			Configuration conf = context.getConfiguration();
			
			Map<String, Integer> Inrix = new HashMap<String, Integer>();
			Map<String, Integer> Wave = new HashMap<String, Integer>();
			
			//conf.set("test","");
			String param = conf.get("match");
			String[] param1 = param.split(";");
			
			for (int i=1;i<param1.length;i++)
			{
				Wave.put(param1[i].split("\t")[0],i);
				Inrix.put(param1[i].split("\t")[1],i);
			}
						
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString();
			
			// Split the input line by tabs 
			String[] inputlines = line.split(",");	
			String TS = inputlines[inputlines.length-1];
			String Date = TS.split("T")[0];
			String Time = TS.split("T")[1];
			int TimeSec = Integer.parseInt(Time.split("-")[0])*60*60+Integer.parseInt(Time.split("-")[1])*60+Integer.parseInt(Time.split("-")[2]);
						
			if (inputlines.length == 4)
			{
				int k1 = Inrix.get(inputlines[0]);
				context.write(new Text(Integer.toString(k1)+"|"+Date), new Text(line+","+Integer.toString(TimeSec)));
			}
			else if (inputlines.length == 5)
			{
				int k2 = Wave.get(inputlines[0]);
				context.write(new Text(Integer.toString(k2)+"|"+Date), new Text(line+","+Integer.toString(TimeSec)));
			}										
			
		} // End method "map"
		
	} // End Class Map_One
	
	
	// The reduce class
	// The key is Text and must match the datatype of the output key of the map method
	// The value is IntWritable and also must match the datatype of the output value of the map method
	public static class Reduce_One extends Reducer<Text, Text, Text, Text>  {
		
		int lower = 90000;
		int higher = 99999;
		
		Text a = new Text();
		
		
		TreeMap<Integer, Integer> Inrix = new TreeMap<Integer, Integer>();
		TreeMap<Integer, Integer> Wave = new TreeMap<Integer, Integer>();
		
		
		
		
		
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<Text> values, Context context) 
											throws IOException, InterruptedException  {
						
			
			
			for (Text value : values) 
			{
	 	        
				String[] v = value.toString().split(",");
	 	           if (v.length==5)
	 	           {
	 	        	  if(!v[1].equals(" "))
	 	        	  {
	 	        		 
	 	        			Inrix.put(Integer.parseInt(v[4]), Integer.parseInt(v[1]));  
	 	        		 
	 	        	  }
	 	        	   
	 	           }
	 	           else if (v.length==6)
	 	           {
	 	        	  if(!v[1].equals(" "))
	 	        	  {
	 	        		  
	 	        			 Wave.put(Integer.parseInt(v[5]), Integer.parseInt(v[1]));
	 	        		  
	 	        	  }
	 	           }
	 	        }
	 	           
			
		context.write(key, new Text(Integer.toString(Wave.size())+" "+Integer.toString(Inrix.size())));
			
						
			
		} // End method "reduce" 
		
	} // End Class Reduce_One
		
}

