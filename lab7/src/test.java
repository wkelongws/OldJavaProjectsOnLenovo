import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;

public class test 
{
    public static void main( String[] args )
    {
    	
    	String timestamp = "2013-03-17 00:43:54";
    	String date = timestamp.split(" ")[0];
    	String time = timestamp.split(" ")[1];
    	String ts = date.split("-")[0]+date.split("-")[1]+date.split("-")[2]
    			+ time.split(":")[0] + time.split(":")[1] + time.split(":")[2];
    	long TimeStamp = Long.parseLong(ts);
    	System.out.println(TimeStamp);
    	System.out.println(ts);
		
		
    }
}
