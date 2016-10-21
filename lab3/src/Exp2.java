/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 3 - exp2 - Shuo Wang **
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



public class Exp2 extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Exp2(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = "/class/s15419x/lab3/patents.txt";    // Input
		String temp = "/scr/shuowang/lab3/exp2/temp/";       // Round one output
		String temp1 = "/scr/shuowang/lab3/exp2/temp1/";     // Round two output
		String output1 = "/scr/shuowang/lab3/exp2/output1/";   // Round three/final output
		String output2 = "/scr/shuowang/lab3/exp2/output2/";   // Round three/final output
		
		int reduce_tasks = 2;  // The number of reduce tasks that will be assigned to the job
		Configuration conf = new Configuration();
		
		// Create job for round 1: round 1 gets all the neighbors of A and the number of triplets with A in the middle, output to 'temp'
		
		// Create the job
		Job job_one = new Job(conf, "Exp2 Program Round One"); 	
		
		// Attach the job to this Driver
		job_one.setJarByClass(Exp2.class); 
		
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
		
		
		// Create job for round 2: round 2 finds all the triangles having A for each A. Output to 'temp1'
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
		
		Job job_two = new Job(conf, "Driver Program Round Two"); 
		job_two.setJarByClass(Exp2.class); 
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
		
		// Create job for round 3: Round 3 counts all the triangles in the network. Output to 'output1'
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
				
		Job job_Three = new Job(conf, "Driver Program Round Three"); 
		job_Three.setJarByClass(Exp2.class); 
		job_Three.setNumReduceTasks(1); 
				
		job_Three.setOutputKeyClass(Text.class); 
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
		FileOutputFormat.setOutputPath(job_Three, new Path(output1));
				
		// Run the job
		job_Three.waitForCompletion(true); 		
		
		// Create job for round 4: Round 4 counts all the triplets in the network. Output to 'output2'
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
				
		Job job_Four = new Job(conf, "Driver Program Round Four"); 
		job_Four.setJarByClass(Exp2.class); 
		job_Four.setNumReduceTasks(1); 
				
		job_Four.setOutputKeyClass(Text.class); 
		job_Four.setOutputValueClass(Text.class);
				
		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_Four.setMapperClass(Map_Four.class); 
		job_Four.setReducerClass(Reduce_Four.class);
			
		job_Four.setInputFormatClass(TextInputFormat.class); 
		job_Four.setOutputFormatClass(TextOutputFormat.class);
				
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_Four, new Path(temp)); 
		FileOutputFormat.setOutputPath(job_Four, new Path(output2));
				
		// Run the job
		job_Four.waitForCompletion(true); 	
	
		return 0;
	
	} // End run
	
	// The round one: round 1 gets all the neighbors of A and the number of triplets with A in the middle

	public static class Map_One extends Mapper<LongWritable, Text, Text, Text>  {		
	
		// The map method 
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line (edge) from the input
			String edge = value.toString();
			
			// Split the edge into two nodes 
			String[] nodes = edge.split("\t");			
			
			// output <v1,v2> and <v2,v1>
			context.write(new Text(nodes[0]), new Text(nodes[1])); 
			context.write(new Text(nodes[1]), new Text(nodes[0])); 		
			
		} // End method "map"
		
	} // End Class Map_One
	
	
	// The reduce class	
	public static class Reduce_One extends Reducer<Text, Text, Text, Text>  {		
		
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fashion.
		public void reduce(Text key, Iterable<Text> values, Context context) 
											throws IOException, InterruptedException  {
			
			String neighbor = "";						
			
			for (Text val : values) {
				context.progress();
				
				neighbor = neighbor + " " + val.toString();		// gather all A's neighbors together						
				
			}//end for
			int num = neighbor.split(" ").length-1;             // throw out the title 'neighbor', so -1
						
			// Use context.write to emit values
			
			// The number of triplets = num*(num-1)/2; 
			// for example: B,C,D are the 3 neighbors of A, 
			// then there are 3*(3-1)/2=3 triplets with A in the middle are BAC,BAD,CAD
			// output example: <1234, neighbor 2 triplet 1 2345 3456>  
			context.write(key, new Text("neighbor " + Integer.toString(num) + " triplet " + Integer.toString(num*(num-1)/2) + neighbor));		
			
		} // End method "reduce" 
		
	} // End Class Reduce_One
	
	// The Round Two: round 2 finds all the triangles having A for each A.
		public static class Map_Two extends Mapper<LongWritable, Text, Text, Text>  {		
		
			// Initialize output key and value
		private Text outputkey = new Text();
		private Text outputvalue = new Text();
		
 		public void map(LongWritable key, Text value, Context context) 
 				throws IOException, InterruptedException  { 			
 			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
 			// input format example: <1234, neighbor 2 triplet 1 2345 3456> 
			String[] line = value.toString().split("\t");			// [1234, neighbor 2 triplet 1 2345 3456]
			String[] lines = line[1].split(" ");					// [neighbor, 2, triplet, 1, 2345, 3456]
			
			
			for (int i = 4; i < lines.length; i++){
				outputkey.set(lines[i]);	
				outputvalue.set(line[0] + " " + line[1]);
				context.write(outputkey, outputvalue);				//<2345, 1234 neighbor 2 triplet 1 2345 3456>
			} // end for			
 			
 		}  // End method "map"
 		
 	}  // End Class Map_Two
 	
 	// The second Reduce class
 	public static class Reduce_Two extends Reducer<Text, Text, Text, Text>  { 		
 				
 		public void reduce(Text key, Iterable<Text> values, Context context) 
 				throws IOException, InterruptedException  { 			
 			
 			// Initialization
 			String[] inputs;
 			String[] NeighborsOfNeighbor;
 			String Vertex = key.toString(); // 2345
 			String Neighbor;
 			String OtherNeighbor;
 			String[] TriangleVertex = new String[3];
 			String Triangle;
 			List<String> TriangleList = new ArrayList<String>();
 			
 			for (Text val : values) {// for 2
				context.progress();
				inputs = val.toString().split(" ");  // [1234, neighbor, 2, triplet, 1, 2345, 3456]							
				Neighbor = inputs[0];				// 1234
				NeighborsOfNeighbor = Arrays.copyOfRange(inputs, 5, inputs.length); //[2345, 3456]
				for (Text val1 : values) {// for 1
					OtherNeighbor = val1.toString().split(" ")[0];  // "1234" out of [1234, neighbor, 2, triplet, 1, 2345, 3456]
					if (Arrays.asList(NeighborsOfNeighbor).contains(OtherNeighbor)){
						TriangleVertex[0]=Vertex;			// [1234, , ]	make-up numbers.... for illustration
						TriangleVertex[1]=Neighbor;			// [1234, 4567, ]	
						TriangleVertex[2]=OtherNeighbor;	// [1234, 4567, 2345]	
						Arrays.sort(TriangleVertex);		// [1234, 2345, 4567]
						Triangle = TriangleVertex[0]+TriangleVertex[1]+TriangleVertex[2];// "123423454567" = "A"
						TriangleList.add(Triangle);			// TriangleList: [A, A, B, ....]
					} // end if
					
				} //end for 1
								
			}//end for 2	
 			
 			Set<String> set = new HashSet<String>(TriangleList); // [A, B, ...] no duplicates
 			String[] TriangleSet = set.toArray(new String[set.size()]); // [A, B]
 			String out = Arrays.toString(TriangleSet);				// "[A, B]"
 			
 			context.write(key,new Text(out));// 1234	[A, B]
			
		}  // End method "reduce"
		
	}  // End Class Reduce_Two
 	
 	// The Round Three: Round 3 counts all the triangles in the network.
public static class Map_Three extends Mapper<LongWritable, Text, Text, Text>  {		
		
 		public void map(LongWritable key, Text value, Context context) 
 				throws IOException, InterruptedException  { 			
 			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
 			String[] in = value.toString().split("\t");  // [1234, [A, B]]
 			
 			// put everything to a same reducer with uniform key
 			context.write(new Text("All"), new Text(in[1]));	// <All, [A, B]>			
 			
 		}  // End method "map"
 		
 	}  // End Class Map_Three
 	
 	// The third Reduce class (similar to the second reducer)
 	public static class Reduce_Three extends Reducer<Text, Text, Text, Text>  { 	
 		
 		public void reduce(Text key, Iterable<Text> values, Context context) 
 				throws IOException, InterruptedException  { 			

 			List<String> TriangleList = new ArrayList<String>(); // a list for all triangles
 			
 			for (Text value : values) {
 			String in = value.toString();  // "[A, B]"
 			String[] a = in.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll(" ", "").split(",");// [A, B]
 			TriangleList.addAll(Arrays.asList(a));
 			} // end for
 			
 			Set<String> set = new HashSet<String>(TriangleList); // remove duplicates
 			
 			context.write(new Text("Number of Triangles:"),new Text(Integer.toString(set.size()))); // output the number of triangles
 			
		}  // End method "reduce"
		
	}  // End Class Reduce_Three
 	
 	
 	// The Round Four: Round 4 counts all the triplets in the network.
public static class Map_Four extends Mapper<LongWritable, Text, Text, Text>  {		
		
 		public void map(LongWritable key, Text value, Context context) 
 				throws IOException, InterruptedException  { 			
 			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
 			
			String[] line = value.toString().split("\t");			// [1234, neighbor 2 triplet 1 2345 3456]
			String[] lines = line[1].split(" ");					// [neighbor, 2, triplet, 1, 2345, 3456]
			 			
 			context.write(new Text("All"), new Text(lines[3]));				// put everything to a same reducer with uniform key <All, 1>
 			
 		}  // End method "map"
 		
 	}  // End Class Map_Four

 	// The forth Reduce class
 	public static class Reduce_Four extends Reducer<Text, Text, Text, Text>  { 	
 		
 		public void reduce(Text key, Iterable<Text> values, Context context) 
 				throws IOException, InterruptedException  { 			

 			int sum=0;
 			
 	        for (Text value : values) {
 	           sum += Integer.parseInt(value.toString()); 	           
 	        }

 	        
 	            context.write(new Text("Number of Triplets:"), new Text(Integer.toString(sum)));
 	        
			
		}  // End method "reduce"
		
	}  // End Class Reduce_Four
 	
 	
 	
 	
 	
	
}

