/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 4 - Shuo Wang - part2**
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


public class dummysort2 extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		
        // Process all MapReduce options if any and then run the MapReduce program
		int res = ToolRunner.run(new Configuration(), new dummysort2(), args);
		System.exit(res); 
	}
	
	// customize these values based on the output from dummysort1
	public static String partition1 = "8P;O>+4qA!"; // the first break point
	public static String partition2 = "P-_]/{}Y=\""; // the second break point
	public static String partition3 = "gP3CA7&eV="; // the third break point
	
	
	public int run ( String[] args ) throws Exception {
		
		int reduce_tasks = 4;
        
        
		Configuration conf = new Configuration();


		Job job_two = new Job(conf, "dummysort2 round two");   
		job_two.setJarByClass(dummysort2.class); 
		job_two.setNumReduceTasks(reduce_tasks);
		FileInputFormat.addInputPath(job_two, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job_two, new Path(args[1]));
		job_two.setMapOutputKeyClass(Text.class); 
		job_two.setMapOutputValueClass(Text.class); 
		job_two.setOutputKeyClass(NullWritable.class);         
		job_two.setOutputValueClass(Text.class);
		job_two.setMapperClass(Map_two.class);  
        job_two.setPartitionerClass(Partition.class);        
		job_two.setReducerClass(Reduce_two.class);
		job_two.setInputFormatClass(TextInputFormat.class); 
		job_two.setOutputFormatClass(TextOutputFormat.class);
		job_two.waitForCompletion(true);
		
		return 0;
	} 
	

		public static class Map_two extends Mapper<LongWritable, Text, Text, Text>  {
	
	        // The map method 
			public void map(LongWritable key, Text value, Context context)
								throws IOException, InterruptedException  {			

				String[] lines = value.toString().split("[ 	]");
	                                	                
	                context.write(new Text(lines[0]), value);
						
			} 	
		} //end map_two
		
		public static class Reduce_two extends Reducer<Text, Text, NullWritable, Text>  {			

			public void reduce(Text key, Iterable<Text> values, Context context) 
									throws IOException, InterruptedException  {
				
				
				for (Text val : values) {					

				context.write(NullWritable.get(), val);
				
				}
			} 
			
		} // end reduce_two
	
	public static class Partition extends Partitioner<Text, Text> {
		
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks){        
        	
     	String ID = key.toString();
         
         if(ID.compareTo(partition1)<=0){
			return 0;
         }
         if(ID.compareTo(partition1)>0 & ID.compareTo(partition2)<=0){
			return 1;
         }
         if(ID.compareTo(partition2)>0 & ID.compareTo(partition3)<=0){
			return 2;
         }
         else
			return 3;
        }

    		
        } // end partitioner
           
	
    } 
		