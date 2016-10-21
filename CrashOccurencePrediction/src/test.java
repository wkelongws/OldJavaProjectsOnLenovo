/**
  *****************************************
  *****************************************
  ************* test *************
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

public class test extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new test(), args);
		System.exit(res); 
		
	} // End main
		
	public int run ( String[] args ) throws Exception {
		
		String input1 = "InrixWaveT/Inrix/Inrix.txt";    // Change this accordingly
		String input2 = "InrixWaveT/Wave/Wave.txt";    // Change this accordingly
		//String temp = "DesMoinesInrix/Data_SlidingWindow_temp";      // Change this accordingly
		String output = "InrixWaveT/Interp";  		// Change this accordingly
		String matchtable = "InrixWaveT/relation.txt";
		
		int reduce_tasks = 10;  // The number of reduce tasks that will be assigned to the job
		
		// read matching table into memory
		
		FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(matchtable))));
        String line;
        String IDs = "start";
        while ((line = br.readLine()) != null) {
        	IDs = IDs + ";" + line;            
        }
        br.close();      
     	
		
        System.out.println(IDs);
        
        Map<String, Integer> Inrix = new HashMap<String, Integer>();
		Map<String, Integer> Wave = new HashMap<String, Integer>();
		
		//conf.set("test","");
		
		String[] param1 = IDs.split(";");
		
		
		for (int i=1;i<param1.length;i++)
		{
			
			Wave.put(param1[i].split("\t")[0],i);
			Inrix.put(param1[i].split("\t")[1],i);
		}
		
		System.out.println(Inrix);
		System.out.println(Wave);

				
		return 0;
	
	} // End run
	
	
}

