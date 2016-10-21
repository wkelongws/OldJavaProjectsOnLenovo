import java.io.*;
import java.lang.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

public class readwritetest {
	public static String temppath = "/scr/shuowang/lab4/exp1/temp/part-r-00000";
public static void main ( String [] args ) throws Exception {
        
  
	String [] b = methodx();
	
	//if(b!=null){
	String partition1=b[0]+"something";
	String partition2=b[1]+"something";
	String partition3=b[2]+"something";
	
		
	// The system configuration
        Configuration conf = new Configuration(); 
        
        // Get an instance of the Filesystem
        FileSystem fs = FileSystem.get(conf);
        
        String path_name = "/scr/shuowang/lab4/newfile";
        
        Path path = new Path(path_name);
        
        // The Output Data Stream to write into
        FSDataOutputStream file = fs.create(path);
        
        // Write some data
        file.writeChars(partition1);
        
        // Close the file and the file system instance
        file.close(); 
        fs.close();
        
    }
public static String[] methodx (){
	try{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path inFile = new Path(temppath);
		FSDataInputStream in = fs.open(inFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String[] line = br.readLine().split(" ");
		return line;
	}
	catch(IOException e){}
	return null;
	
}   

}