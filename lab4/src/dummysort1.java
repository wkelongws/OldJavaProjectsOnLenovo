/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 4 - Shuo Wang - part1**
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
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;



public class dummysort1 extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		
        // Process all MapReduce options if any and then run the MapReduce program
		int res = ToolRunner.run(new Configuration(), new dummysort1(), args);
		System.exit(res); 
	}
	

	public static Double percent = 0.001; // the sampling ratio
	
	
	public int run ( String[] args ) throws Exception {
		        
		int reduce_tasks = 1;
		Configuration conf = new Configuration();
		
		Job job_one = new Job(conf, "dummysort1 round one");   
		job_one.setJarByClass(dummysort1.class); 
		job_one.setNumReduceTasks(reduce_tasks);
		FileInputFormat.addInputPath(job_one, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job_one, new Path("/scr/shuowang/lab4/exp1/temp"));
		job_one.setMapOutputKeyClass(Text.class); 
		job_one.setMapOutputValueClass(Text.class); 
		job_one.setOutputKeyClass(NullWritable.class);         
		job_one.setOutputValueClass(Text.class);
		job_one.setMapperClass(Map_one.class);          
		job_one.setReducerClass(Reduce_one.class);
		job_one.setInputFormatClass(TextInputFormat.class); 
		job_one.setOutputFormatClass(TextOutputFormat.class);
		job_one.waitForCompletion(true);
		
		return 0;
	} 
	
    // round one
	public static class Map_one extends Mapper<LongWritable, Text, Text, Text>  {
		
        // The map method 
		public void map(LongWritable key, Text value, Context context)
							throws IOException, InterruptedException  {			
			
			Random rn = new Random();
		      Double a = rn.nextDouble();
		      if(a<percent){
		    	  context.write(new Text("sample:"), value);
		      }//end if					
		} 	
	} //end map_one
	
	public static class Reduce_one extends Reducer<Text, Text, NullWritable, Text>  {
		
		private TreeMap<String, String> orderedlist = new TreeMap<String, String>();

		public void reduce(Text key, Iterable<Text> values, Context context) 
								throws IOException, InterruptedException  {
			
			for (Text value : values) {
	 	           String[] v = value.toString().split("[ 	]");
	 	           
	 	          orderedlist.put(v[0], value.toString());		
	 	          
	 	        }

	 	       String[] a = orderedlist.keySet().toArray((new String[orderedlist.size()]));	       

	 	       String partition1 = a[orderedlist.size()/4];
	 	       String partition2 = a[orderedlist.size()/2];
	 	       String partition3 = a[orderedlist.size()*3/4];

	 	       context.write(NullWritable.get(), new Text(partition1 + " " + partition2 + " " + partition3));	
	 	    
		} 		
		
	} // end reduce one
	
	
    } 
		