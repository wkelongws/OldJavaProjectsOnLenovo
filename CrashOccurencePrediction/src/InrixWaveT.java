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

public class InrixWaveT extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new InrixWaveT(), args);
		System.exit(res); 
		
	} // End main
		
	public int run ( String[] args ) throws Exception {
		
		String input1 = "InrixWaveT/Inrix/Inrix.txt";    // Change this accordingly
		//String input1 = "InrixWaveT/Inrix/inrix/1-11/1-11.txt";    // Change this accordingly
		//String input1 = "iw/i.txt"; 
		String input2 = "InrixWaveT/Wave/Wave.txt";    // Change this accordingly
		//String input2 = "InrixWaveT/Wave/wavetronix/1-11/1-11.txt";    // Change this accordingly
		//String input2 = "iw/w.txt";   
		//String temp = "DesMoinesInrix/Data_SlidingWindow_temp";      // Change this accordingly
		String output = "InrixWaveT/result";  		// Change this accordingly
		//String output = "result"; 
		String matchtable = "InrixWaveT/relation.txt";
		
		int reduce_tasks = 16;  // The number of reduce tasks that will be assigned to the job
		
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
		job_one.setJarByClass(InrixWaveT.class); 
		
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
			
			Map<String, String> Inrix = new HashMap<String, String>();
			Map<String, String> Wave = new HashMap<String, String>();
			
			//conf.set("test","");
			String param = conf.get("match");
			String[] param1 = param.split(";");
			
			for (int i=1;i<param1.length;i++)
			{
				if (Wave.containsKey(param1[i].split("\t")[0]))
				{
					String n = Wave.get(param1[i].split("\t")[0]);
					Wave.put(param1[i].split("\t")[0], n + "," + Integer.toString(i));					
				}
				else 
				{
					Wave.put(param1[i].split("\t")[0],Integer.toString(i));
				}
				
				if (Inrix.containsKey(param1[i].split("\t")[1]))
				{
					String m = Inrix.get(param1[i].split("\t")[1]);
					Inrix.put(param1[i].split("\t")[1], m + "," + Integer.toString(i));					
				}
				else 
				{
					Inrix.put(param1[i].split("\t")[1],Integer.toString(i));
				}
							
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
				String k1 = Inrix.get(inputlines[0]);
				String[] k_1 = k1.split(",");
				for (int i=0;i<k_1.length;i++)
				{
					context.write(new Text(k_1[i]+"|"+Date), new Text(line+","+Integer.toString(TimeSec)));
				}				
			}
			else if (inputlines.length == 5)
			{
				String k2 = Wave.get(inputlines[0]);
				String[] k_2 = k2.split(",");
				for (int i=0;i<k_2.length;i++)
				{
					context.write(new Text(k_2[i]+"|"+Date), new Text(line+","+Integer.toString(TimeSec)));
				}				
			}										
			
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
						
			TreeMap<Integer, String> Inrix = new TreeMap<Integer, String>();
			TreeMap<Integer, String> Wave = new TreeMap<Integer, String>();
			
			int lower = 90000;
			int higher = 99999;
			
			Text a = new Text();
			
			
			for (Text value : values) 
			{
	 	        if (value!=null)
	 	        {
				String[] v = value.toString().split(",");
	 	           if (v.length==5)
	 	           {
	 	        	  if(!v[1].equals(" "))
	 	        	  {
	 	        		 
	 	        			Inrix.put(Integer.parseInt(v[4]), v[1]+","+v[2]);  
	 	        		 
	 	        	  }
	 	        	   
	 	           }
	 	           else if (v.length==6)
	 	           {
	 	        	  if(!v[1].equals(" "))
	 	        	  {
	 	        		  
	 	        			 Wave.put(Integer.parseInt(v[5]), v[1]+","+v[2]+","+v[3]);
	 	        		  
	 	        	  }
	 	           }
	 	        }
	 	           
			}
		
			for (int k : Wave.keySet()) 
			{
				lower = 90000;
				higher = 99999;
								
				if (Inrix.containsKey(k))
				{
					String Wavespeed = Wave.get(k).split(",")[0];
					String Inrixspeed = Inrix.get(k).split(",")[0];
										
					//a.set(Integer.toString(k) + "," + Wavespeed + "," + Integer.toString(k) + "," + Integer.toString(k) + 
					//		"," + Inrixspeed + "," + Inrixspeed + "," + Inrixspeed);
					a.set(Integer.toString(k/3600)+":"+Integer.toString((k%3600)/60)+":"+Integer.toString((k%3600)%60) + "," + Wave.get(k) + "," + 
					Integer.toString(k/3600)+":"+Integer.toString((k%3600)/60)+":"+Integer.toString((k%3600)%60) + "," + 
							Integer.toString(k/3600)+":"+Integer.toString((k%3600)/60)+":"+Integer.toString((k%3600)%60) + 
							"," + Inrix.get(k) + "," + Inrix.get(k) + "," + Inrixspeed);
					context.write(key, a);
				}
								
				else 
				{
					if (Inrix.lowerKey(k)!=null)
					{
						lower = Inrix.lowerKey(k);
					}
					if (Inrix.higherKey(k)!=null)
					{
						higher = Inrix.higherKey(k);
					}
					
					if (higher-lower<60 & higher-lower>0)
					{
						int Inrixspeed1 = Integer.parseInt(Inrix.get(lower).split(",")[0]);
						int Inrixspeed2 = Integer.parseInt(Inrix.get(higher).split(",")[0]);
						int Interpspeed = (Inrixspeed2-Inrixspeed1)*(k-lower)/(higher-lower)+Inrixspeed1;
												
						//a.set(Integer.toString(k) + "," + Integer.toString(Wave.get(k)) + "," + Integer.toString(lower) + "," + Integer.toString(higher) + 
						//		"," + Integer.toString(Inrixspeed1) + "," + Integer.toString(Inrixspeed2) + "," + Integer.toString(Interpspeed));
						a.set(Integer.toString(k/3600)+":"+Integer.toString((k%3600)/60)+":"+Integer.toString((k%3600)%60) + "," + Wave.get(k) + "," + 
								Integer.toString(lower/3600)+":"+Integer.toString((lower%3600)/60)+":"+Integer.toString((lower%3600)%60) + "," + 
								Integer.toString(higher/3600)+":"+Integer.toString((higher%3600)/60)+":"+Integer.toString((higher%3600)%60) + 
								"," + Inrix.get(lower) + "," + Inrix.get(higher) + "," + Integer.toString(Interpspeed));
						context.write(key, a);
					}
									
				}
				
			}
						
			
		} // End method "reduce" 
		
	} // End Class Reduce_One
		
}

