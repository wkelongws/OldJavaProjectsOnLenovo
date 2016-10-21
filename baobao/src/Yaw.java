import java.io.IOException;
import java.util.*;


import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;


public class Yaw extends EvalFunc<Integer> {
	@Override
	public Integer exec(Tuple input) throws IOException {
				
		Object values = input.get(0);
		
		String timestamp = (String)values;//10|01122015	00:43:54
		String IDdate = timestamp.split("	")[0];
		
		String date = IDdate.split("\\|")[1];
		String mmdd = date.substring(0,4);
		String yyyy = date.substring(4);
		String day = yyyy+mmdd;
		
		
		int TimeStamp = Integer.parseInt(day);
		return TimeStamp;
		
		
	}
}
