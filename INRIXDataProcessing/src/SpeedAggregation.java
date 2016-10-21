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

public class SpeedAggregation extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new SpeedAggregation(), args);
		System.exit(res); 
		
	} // End main
		
	public int run ( String[] args ) throws Exception {
		
		String input1 = "Shuo/INRIX_Nebraska/2013-2014-I-80/2013-2014-I-80.csv";    // Change this accordingly		
		String input2 = "Shuo/INRIX_Nebraska/2013-2014-I-480/2013-2014-I-480.csv";    // Change this accordingly		
		String input3 = "Shuo/INRIX_Nebraska/2013-2014-I-680/2013-2014-I-680.csv";
		String input4 = "Shuo/INRIX_Nebraska/2013-2014-US-75/2013-2014-I-480.csv";
		String input5 = "Shuo/INRIX_Nebraska/2013-2014-west-dodge-st/2013-2014-west-dodge-st.csv";
		String output = "Shuo/INRIX_Nebraska/AggregatedData/5roads";  		// Change this accordingly		
		
		
		int reduce_tasks = 16;  // The number of reduce tasks that will be assigned to the job
	
		Configuration conf = new Configuration();
				
		
		Job job_one = new Job(conf, "InrixWaveT Program Round One"); 		
		job_one.setJarByClass(SpeedAggregation.class); 		
		job_one.setNumReduceTasks(reduce_tasks);		
		job_one.setMapOutputKeyClass(Text.class); 
		job_one.setMapOutputValueClass(Text.class); 
		job_one.setOutputKeyClass(NullWritable.class);
		job_one.setOutputValueClass(Text.class);
		job_one.setMapperClass(Map_One.class); 
		job_one.setReducerClass(Reduce_One.class);
		job_one.setInputFormatClass(TextInputFormat.class);  
		job_one.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job_one, new Path(input1)); 
		FileInputFormat.addInputPath(job_one, new Path(input2)); 
		FileInputFormat.addInputPath(job_one, new Path(input3));
		FileInputFormat.addInputPath(job_one, new Path(input4));
		FileInputFormat.addInputPath(job_one, new Path(input5));
		FileOutputFormat.setOutputPath(job_one, new Path(output));
		job_one.waitForCompletion(true); 
				
		return 0;
	
	} // End run
	
	// The Map Class
	public static class Map_One extends Mapper<LongWritable, Text, Text, Text>  {
		
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
		
			String line = value.toString();
			String[] elements = line.split(",");
			
			if (elements.length==8)
			{
				
			String timestamp = elements[1];
			if (timestamp.split(" ").length==2)
			{
			String date = timestamp.split(" ")[0];
			String time = timestamp.split(" ")[1];
			String dd = date.split("-")[0]+"/"+date.split("-")[1]+"/"+date.split("-")[2];
			String ddd = date.split("-")[0]+date.split("-")[1]+date.split("-")[2];
			int hh = Integer.parseInt(time.split(":")[0]);
			int mm = Integer.parseInt(time.split(":")[1]);
			int mm5 = mm/5;
			if (elements[2] == null || elements[2].trim().equals("")){}
			else
			{
			context.write(new Text(elements[0]+","+dd+","+ddd+","+Integer.toString(hh)+","+Integer.toString(mm5))
			,new Text(elements[2]+","+elements[6]+","+elements[7]));
			}
			}					
			}										
			
			/*if (elements.length==8)
			{
			if (elements[2] == null || elements[2].trim().equals(""))
			{
						
			context.write(NullWritable.get(),value);
			}					
			}	*/
			
			
		} // End method "map"
		
	} // End Class Map_One
		
	// The reduce class
	public static class Reduce_One extends Reducer<Text, Text, NullWritable, Text>  {
				
		public void reduce(Text key, Iterable<Text> values, Context context) 
											throws IOException, InterruptedException  {
			Double speedsum = 0.0;
			Double scoresum = 0.0;
			int count = 0;
			Double speed5min = 0.0;
			Double score5min = 0.0;
			
			for (Text value : values) 
			{
				count++;
				speedsum+= Double.parseDouble(value.toString().split(",")[0]);	
				scoresum+= Double.parseDouble(value.toString().split(",")[1]);
			}
			speed5min = speedsum/count;
			score5min = scoresum/count;
			
			context.write(NullWritable.get(), new Text(key+","+Double.toString(speed5min)+","+Double.toString(score5min)));
			
		} // End method "reduce" 
		
	} // End Class Reduce_One
		
}

