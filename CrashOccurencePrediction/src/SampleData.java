
import java.io.*;
import java.lang.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;


public class SampleData  {
    
    public static void main ( String [] args ) throws Exception {
        
        // The system configuration
        Configuration conf = new Configuration(); 
        
        // Get an instance of the Filesystem
        FileSystem fs = FileSystem.get(conf);
        
			
        String inFile_path_name = "DesMoinesInrix/Inrix_Crash/part-r-00000";
		String outFile_path_name = "DesMoinesInrix/Inrix_Crash/sample";
        
        Path inFile = new Path(inFile_path_name);
        Path outFile = new Path(outFile_path_name);
		
		// Open inFile for reading
		FSDataInputStream in = fs.open(inFile);
		
		// Open outFile for writing
		FSDataOutputStream out = fs.create(outFile);
		
		byte[] buffer = new byte[1000];
		
        // Read from input stream and write to output stream until EOF.
        int bytesRead = in.read(0L,buffer,0,1000);
		out.write(buffer, 0, bytesRead);
		
				            
        // Close the file and the file system instance
        in.close(); 
		out.close(); 
        fs.close();
        
    }
    
}
