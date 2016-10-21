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

public class sorttest extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		
        // Process all MapReduce options if any and then run the MapReduce program
		int res = ToolRunner.run(new Configuration(), new sorttest(), args);
		System.exit(res); 
	}
	

	public static Double percent = 0.5; // the sampling ratio
	public static String temppath = "/scr/shuowang/lab4/exp1/temp";
	
	public int run ( String[] args ) throws Exception {
		
		int reduce_tasks = 4;
		Path partitionFile = new Path(temppath);
        // Get system configuration
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
		//conf.set("partition1", "a");
		//conf.set("partition2", "a");
		//conf.set("partition3", "a");
		
		/*
        // Round1		
		Job job_one = new Job(conf, "sorttest round one");   
		job_one.setJarByClass(sorttest.class); 
		job_one.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job_one, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job_one, partitionFile);
		job_one.setMapOutputKeyClass(Text.class); 
		job_one.setMapOutputValueClass(Text.class); 
		job_one.setOutputKeyClass(NullWritable.class);         
		job_one.setOutputValueClass(Text.class);
		job_one.setMapperClass(Map_one.class);         
        //job_one.setCombinerClass(Reduce_one.class);
        //job_one.setPartitionerClass(Partition_one.class);        
		job_one.setReducerClass(Reduce_one.class);
		job_one.setInputFormatClass(KeyValueTextInputFormat.class); 
		job_one.setOutputFormatClass(TextOutputFormat.class);
		job_one.waitForCompletion(true);*/

		
		
		

        // Round2
		Job job_two = new Job(conf, "sorttest round two");   
		job_two.setJarByClass(sorttest.class); 
		job_two.setNumReduceTasks(reduce_tasks);
		FileInputFormat.addInputPath(job_two, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job_two, new Path(args[1]));
		job_two.setMapOutputKeyClass(Text.class); 
		job_two.setMapOutputValueClass(Text.class); 
		job_two.setOutputKeyClass(NullWritable.class);         
		job_two.setOutputValueClass(Text.class);
		job_two.setMapperClass(Map_two.class);         
        //job_two.setCombinerClass(Reduce_two.class);
		TotalOrderPartitioner.setPartitionFile(conf, new Path("/scr/shuowang/lab4/exp1/temp/"));
        job_two.setPartitionerClass(TotalOrderPartitioner.class);        
		job_two.setReducerClass(Reduce_one.class);
		job_two.setInputFormatClass(KeyValueTextInputFormat.class); 
		job_two.setOutputFormatClass(TextOutputFormat.class);
		job_two.waitForCompletion(true);
		
		return 0;
	} 
	
    // round one
	public static class Map_one extends Mapper<Text, Text, Text, Text>  {
		
        // The map method 
		public void map(Text key, Text value, Context context)
							throws IOException, InterruptedException  {			
			
			Random rn = new Random();
		      Double a = rn.nextDouble();
		      if(a<percent){
		    	  context.write(key, key);
		      }//end if
                
					
		} 	
	} //end map_one
	
	public static class Reduce_one extends Reducer<Text, Text, NullWritable, Text> {
	     
		public void reduce(Text key, Iterable<Text> values, Context context) 
								throws IOException, InterruptedException  {
			
			for (Text value : values) {
	 	           
				context.write(NullWritable.get(), value);
	 	        }

		}		
		
	} // end reduce one
	
	// round two
		public static class Map_two extends Mapper<Text, Text, Text, Text>  {
			
	        // The map method 
			public void map(Text key, Text value, Context context)
								throws IOException, InterruptedException  {
			
				context.write(key, new Text(key.toString()+" "+value.toString()));
						
			} 	
		} //end map_two
		
		public static class Reduce_two extends Reducer<Text, Text, NullWritable, Text>  {
			
	        // The reduce method
			// For key, we have an Iterable over all values associated with this key
			// The values come in a sorted fasion.
			public void reduce(Text key, Iterable<Text> values, Context context) 
									throws IOException, InterruptedException  {
				
				
				for (Text val : values) {					

				context.write(NullWritable.get(), val);
				
				}
			} 
			
		} // end reduce_two
	
	public static class Partition extends Partitioner<Text, Text> implements Configurable {
	     private static Configuration conf;

	     @Override
	      public void setConf(Configuration conf) {
	         Partition.conf = conf;
	      }
	 
	      @Override
	      public Configuration getConf() {
	         return conf;
	      }
	      
 	      String partition1 = conf.get("partition1");
 	      String partition2 = conf.get("partition2");
 	      String partition3 = conf.get("partition3");
	      
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks){

        	
        	
        	//if(b!=null){
        	/*String [] b = methodx();
			String partition1=b[0];
			String partition2=b[1];
			String partition3=b[2];*/
        	//}
        	/*else{
        		String partition1=" ";
    			String partition2=" ";
    			String partition3=" ";
        	}*/
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
        

        

    		
        }// end partitioner

	/*public static String[] methodx (){
	try{
		//Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(configuration);
		Path inFile = new Path("/scr/shuowang/lab4/exp1/temp/part-r-00000");
		FSDataInputStream in = fs.open(inFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String[] line = br.readLine().split(" ");
		return line;
	}
	catch(IOException e){}
	return null;
	
}*/
	
    } 
		