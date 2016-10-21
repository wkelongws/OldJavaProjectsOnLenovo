/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 3 - exp1 - Shuo Wang **
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



public class Exp1 extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Exp1(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = "/class/s15419x/lab3/patents.txt";    // Input
		String temp = "/scr/shuowang/lab3/exp1/temp/";       // Round one output
		String temp1 = "/scr/shuowang/lab3/exp1/temp1/";     // Round two output
		String output = "/scr/shuowang/lab3/exp1/output/";   // Round three/final output
		
		int reduce_tasks = 2;  // The number of reduce tasks that will be assigned to the job
		Configuration conf = new Configuration();
		
		// Create job for round 1: round 1 summarize the all the patents A cites and all the patents A cited by for A.
		
		// Create the job
		Job job_one = new Job(conf, "Exp1 Program Round One"); 	
		
		// Attach the job to this Driver
		job_one.setJarByClass(Exp1.class); 
		
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
		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path)); // This is not allowed
		
		// Run the job
		job_one.waitForCompletion(true); 
		
		
		// Create job for round 2: round 2 finds all the 1-hop and 2-hop citations for A
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
		
		Job job_two = new Job(conf, "Driver Program Round Two"); 
		job_two.setJarByClass(Exp1.class); 
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
		job_Three.setJarByClass(Exp1.class); 
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
	
		return 0;
	
	} // End run
	
	// The round one: round 1 summarize the all the patents A cites and all the patents A cited by for A.

	public static class Map_One extends Mapper<LongWritable, Text, Text, Text>  {		
	
		// The map method 
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line (edge) from the input
			String edge = value.toString();
			
			// Split the edge into two nodes 
			String[] nodes = edge.split("\t");			
			
			context.write(new Text(nodes[0]), new Text(nodes[1] + " 1")); //(a, b 1) means a cites b 	
			context.write(new Text(nodes[1]), new Text(nodes[0] + " 2")); //(a, b 2) means a is cited by b 			
			
		} // End method "map"
		
	} // End Class Map_One
	
	
	// The reduce class	
	public static class Reduce_One extends Reducer<Text, Text, Text, Text>  {		
		
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<Text> values, Context context) 
											throws IOException, InterruptedException  {
			
			String cite = "";
			String cited = "";
			String[] read;		
			
			for (Text val : values) {
				context.progress();
				read = val.toString().split(" ");	
				if(Integer.parseInt(read[1])==1){
					cite = cite + " " + read[0]; // put every thing A cites into one group
				}//end if
				if(Integer.parseInt(read[1])==2){
					cited = cited + " " + read[0];// put every thing A is cited by into one group
				}//end if

			}//end for

			// Use context.write to emit values
			context.write(key, new Text("cite" + cite + "|" + "citedby" + cited));
			
		} // End method "reduce" 
		
	} // End Class Reduce_One
	
	
	// The Round Two: round 2 finds all the 1-hop and 2-hop citations for A
	// The second Map Class
		public static class Map_Two extends Mapper<LongWritable, Text, Text, Text>  {		
		
		private Text outputkey = new Text();
		private Text outputvalue = new Text();
		
 		public void map(LongWritable key, Text value, Context context) 
 				throws IOException, InterruptedException  { 			
 			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
 			// input format example: 1234	cite 2345|citedby 4567
			String[] line = value.toString().split("[|]");		//["1234	cite 2345", "citedby 4567"]
			String[] a = line[0].split("\t");					//["1234", "cite 2345"]
			String[] citegroup = a[1].split(" ");				//["cite", "2345"]			
			
			if(citegroup.length>1){ 
			for (int i = 1; i < citegroup.length; i++){
				outputkey.set(citegroup[i]);
				outputvalue.set(a[0] + " " + line[1]);
				context.write(outputkey, outputvalue);			//<2345, 1234 citedby 4567>
			} // end for
			} // end if
 			
 		}  // End method "map"
 		
 	}  // End Class Map_Two
 	
 	// The second Reduce class
 	public static class Reduce_Two extends Reducer<Text, Text, Text, Text>  { 		
 				
 		public void reduce(Text key, Iterable<Text> values, Context context) 
 				throws IOException, InterruptedException  {
 			
 			String[] buffer;										// create a container
 			List<String> list = new ArrayList<String>();			// create an empty list
 			
 			for (Text val : values) {
				context.progress();
				 buffer = val.toString().split(" ");				//
				 list.addAll(Arrays.asList(buffer));				// put all the 1-hop and 2-hop citations into the list 
 			} // end for
 			
 			Set<String> set = new HashSet<String>(list);			// remove all the duplicates in the list
 			int value = set.size()-1;								// there is "citedby" in the list, so -1
 			context.write(key, new Text(Integer.toString(value)));	// <1234, #of 1/2-hop citations>		
			
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

