/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 5 - Exp2 - Shuo Wang **
  *****************************************
  *****************************************
  */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;


public class Exp2 extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new Exp2(), args);
		System.exit(res);

	}

	public int run(String[] args) throws Exception {

		String input = "/class/s15419x/lab5/oscars.json";
		String temp = "/scr/shuowang/lab5/exp2/temp";
		String output = "/scr/shuowang/lab5/exp2/output";
		
		int reduce_tasks = 10;

		// Create job for round 1: round 1 find the max followers for each tweeter
		Job job_one = new Job(super.getConf(), "Lab5 Exp2 Round One");
		job_one.setJarByClass(Exp2.class);
		job_one.setNumReduceTasks(reduce_tasks);

		job_one.setMapOutputKeyClass(Text.class); 
		job_one.setMapOutputValueClass(LongWritable.class); 
		job_one.setOutputKeyClass(Text.class);
		job_one.setOutputValueClass(LongWritable.class);

		job_one.setMapperClass(Map_One.class);
		job_one.setReducerClass(Reduce_One.class);

		job_one.setInputFormatClass(ShuoInputFormat.class);
		job_one.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_one, new Path(input));
		FileOutputFormat.setOutputPath(job_one, new Path(temp));

		job_one.waitForCompletion(true);

		// Create job for round 2: round 2 finds the top 10 tweeters with most followers 
		Job job_two = new Job(super.getConf(), "Lab5 Exp2 Round Two");
		job_two.setJarByClass(Exp2.class);
		job_two.setNumReduceTasks(1);

		job_two.setOutputKeyClass(IntWritable.class);
		job_two.setOutputValueClass(Text.class);

		job_two.setMapperClass(Map_Two.class);
		job_two.setReducerClass(Reduce_Two.class);

		job_two.setInputFormatClass(TextInputFormat.class);
		job_two.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_two, new Path(temp));
		FileOutputFormat.setOutputPath(job_two, new Path(output));

		job_two.waitForCompletion(true);

		return 0;

	} // End run

	// The Map Class
	public static class Map_One extends Mapper<LongWritable, Text, Text, LongWritable> {

		// The map method
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// one line is a json object
			String line = value.toString();
			// parse JSON
			JSONObject obj = (JSONObject) JSONValue.parse(line);
			// get user name and number of followers
			JSONObject userobj = (JSONObject) obj.get("user");
			String name = (String) userobj.get("screen_name");
			Long count = (Long) userobj.get("followers_count");
			// output <user name, number of followers>
			context.write(new Text(name), new LongWritable(count));
			
		} // End method "map"

	} // End Class Map_One

	// The reduce class
	public static class Reduce_One extends Reducer<Text, LongWritable, Text, LongWritable> {

		// The reduce method
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			
			long max = 0;
			// find the maximum followers_count
			for (LongWritable val : values) {
				if (val.get() > max) {
					max = val.get();
				}
			}

			context.write(key, new LongWritable(max));

		} // End method "reduce"

	} // End Class Reduce_One

	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			context.write(new IntWritable(1), new Text(value)); // push everything to the same reducer

		} // End method "map"

	} // End Class Map_Two

	// The second Reduce class
	public static class Reduce_Two extends Reducer<IntWritable, Text, NullWritable, Text> {
		
		private TreeMap<Long, String> tophashtag = new TreeMap<Long, String>(); 	

		// find top 10 by treemap, same as in Exp1
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text value : values) {
				String line = value.toString();   
				String[] v = line.split("\\s+");
	 	           Long frequency = Long.parseLong(v[1]);
	 	          tophashtag.put(frequency, line);		// each time put one observation into treemap

	 	           if (tophashtag.size() > 10) {

	 	        	  tophashtag.remove(tophashtag.firstKey());		// when 11 elements in the list, remove the one with smallest frequency
	 	            }
	 	        }

	 	        for (String t : tophashtag.values()) {
	 	            context.write(NullWritable.get(), new Text(t));			// output everything in the list (top 10)
	 	        }

		} // End method "reduce"

	} // End Class Reduce_Two

	
	// my InputFormat
	public static class ShuoInputFormat extends FileInputFormat<LongWritable, Text> {

		public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) 
				throws IOException, InterruptedException {

			return new ShuoRecordReader();
		}

	}
	
	// my RecordReader
	public static class ShuoRecordReader extends RecordReader<LongWritable, Text> {
		
		private long start;
		private long end;
		private long pos;
		private LineReader in;
		private int maxLineLength;
		
		private LongWritable key = new LongWritable();
		private Text value = new Text();
		
		private boolean firstflag = true;
		
		public void close() throws IOException {
			if (in != null) {
				in.close();
			}	
		}

		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}
		
		public float getProgress() throws IOException, InterruptedException {
			if (start == end) {
	            return 0.0f;
	        } else {
	            return Math.min(1.0f, (pos - start) / (float) (end - start));
	        }
		}

		
		public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
			// This InputSplit is a FileInputSplit
			FileSplit split = (FileSplit) inputSplit;
			// Retrieve configuration, and Max allowed bytes for a single record
			Configuration job = context.getConfiguration();
			this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
			
			// Get the start and end position of this split
			start = split.getStart();
			end = start + split.getLength();
			
			// Retrieve file containing split
			final Path file = split.getPath();
			FileSystem fs = file.getFileSystem(job);
			// Retrieve the InputStream of the split
			FSDataInputStream fileIn = fs.open(split.getPath());
			
			in = new LineReader(fileIn, job);
			Text line = new Text();
			
			// Find the starting point of a JSON object
			// Use " {" as the criteria, flag is true when start is found 
			// pos is the actual start
			pos = start;
			int offset;
			boolean flag = false;
			while (flag == false) {
				line.clear();
				offset = in.readLine(line);
				String str = line.toString();
				if (str.equals(" {")) {
					flag = true;
				}
				else {
					pos += offset;
				}
			}
		}

		
		public boolean nextKeyValue() throws IOException, InterruptedException {
			
			// Current offset is the key
	        key.set(pos);
			
	        // newSize is the size of each line that we read
	        int newSize = 0;
	        Text line = new Text();
	        String jsonObj = "";	        		
	        // 1 represents "{", -1 represents "}"
			int brace_count = 0;
	        
			if (firstflag == true) {
				jsonObj = " {";
				brace_count = 1;
				firstflag = false;
			}
			
			
			while (pos < end) {
				line.clear();
				// Read a new line and store its content to "line"
	            newSize = in.readLine(line, maxLineLength,
	                    Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
	            String str = line.toString();	         
	            //String trimStr = str.replaceAll("\\s+",""); 	
	            String trimStr = str.trim();
	            
	            // overlook the "][" between two jSON objects
	            if (trimStr.equals("][")) {
	            	continue;
	            }
	            
				if (trimStr.startsWith("}")) {
					brace_count--;
				} 
				else if (trimStr.endsWith("{")) {
					brace_count++;
				}         
	            
	            jsonObj += str;
	            pos += newSize;
	            
	            // find a JSON object
	            if (trimStr.startsWith("}") && brace_count == 0) {
	            	// remove the characters after the last "}"
	            	int last = jsonObj.lastIndexOf("}");
	            	value.set(jsonObj.substring(0, last+1));	   
	            	return true;
	            }
			}
			
			return false;
		}
		
	}

}