/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 4 - Shuo Wang *********
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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

// A simple Hadoop MapReduce program to count the number of occurence of all distinct words
// Learn more about Hadoop at: http://hadoop.apache.org/docs/r1.2.1/api/index.html


// The MapReduce sort program using the Tool interface
// The tool interface helps in handling of generic command line options
// If you provide any option that is specific for Hadoop,
// this would be read by the system and discarded.
// So your program need not process the generic MapReduce command line options
// Your program can directly use the specific command line arguments for your program
// and ignore the ones for the MapReduce framework
// Read more about the Tool Interface in : 
// http://hadoop.apache.org/docs/r1.2.1/api/org/apache/hadoop/util/Tool.html

public class inputformattest extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		
        // Process all MapReduce options if any and then run the MapReduce program
		int res = ToolRunner.run(new Configuration(), new inputformattest(), args);
		System.exit(res); 
	}
	

	//public static Double percent = 1.0; // the sampling ratio
	public static String temppath = "/scr/shuowang/lab4/exp1/temp";
	
	public int run ( String[] args ) throws Exception {
		
		int reduce_tasks = 4;
		Path partitionFile = new Path(temppath);		
		InputSampler.Sampler<Text, Text> sampler =
		        new InputSampler.RandomSampler<Text,Text>(0.3,10000);
        // Get system configuration
		Configuration conf = new Configuration();
		//JobConf conf = new JobConf();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
		
		
        // Round1
		
		Job job_one = new Job(conf, "inputformattest round one");   
		job_one.setJarByClass(inputformattest.class); 
		job_one.setNumReduceTasks(reduce_tasks);
		FileInputFormat.addInputPath(job_one, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job_one, new Path(args[1]));
		job_one.setMapOutputKeyClass(Text.class); 
		job_one.setMapOutputValueClass(Text.class); 
		job_one.setOutputKeyClass(Text.class);         
		job_one.setOutputValueClass(Text.class);
		job_one.setMapperClass(Map_one.class);         
        //job_one.setCombinerClass(Reduce_one.class);
		
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		InputSampler.writePartitionFile(job_one, sampler);
		
		
        job_one.setPartitionerClass(TotalOrderPartitioner.class);        
		job_one.setReducerClass(Reduce_one.class);
		
		job_one.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job_one.setOutputFormatClass(TextOutputFormat.class);
		job_one.waitForCompletion(true);

		
		
		
		return 0;
	} 
	
    // round one
	public static class Map_one extends Mapper<Text, Text, Text, Text>  {
		

		
        // The map method 
		public void map(Text key, Text value, Context context)
							throws IOException, InterruptedException  {			
			
			//Random rn = new Random();
		      //Double a = rn.nextDouble();
		      //if(a<percent){
		    	  context.write(key, value);
		      //}//end if
                
					
		} 	
	} //end map_one
	
	public static class Reduce_one extends Reducer<Text, Text, Text, Text>  {		
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
								throws IOException, InterruptedException  {
			
			for (Text value : values) {
	 	           	 	           
	 	          context.write(key, value);	
	 	          
	 	        }

	 	       
	 	      
	 	      // context.getConfiguration().set("partition1", a[firstquartile]);
	 	      //context.getConfiguration().set("partition2", a[median]);
	 	      //context.getConfiguration().set("partition3", a[thirdquartile]);
		} 

		
		
	} // end reduce one
	
	
        
    }
		