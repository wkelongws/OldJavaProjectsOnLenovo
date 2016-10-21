import java.io.IOException;
import java.util.*;


import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;


public class GROWTH extends EvalFunc<Double> {

	@Override
	public Double exec(Tuple input) throws IOException {
		double ratio = 0.0;
		
		Object values = input.get(0);
		if (values instanceof DataBag) {
			DataBag bag = (DataBag) values;
			// ignore a company doesn't exist for more than 1 day
			if (bag.size() < 2) {
				return null;
			}
			
			else {
				Iterator<Tuple> it = ((DataBag) values).iterator();
				// create a new treemap to sort all the records by time
				TreeMap<Integer, Double> orderedrecords = new TreeMap<Integer, Double>(); 	
				
				while (it.hasNext()) {
					// tuple: ticker,date,open
					Tuple t = it.next();
					// get the date and price of that tuple
					int date = (Integer)t.get(1);
					double price = (Double)t.get(2);
					
					orderedrecords.put(date, price);
					
				}
				
				// in the next loop (scaning by time), keep tracking the minimum price happened so far
				Double MinSoFar = Double.MAX_VALUE;
				
				for (Double t : orderedrecords.values()) {
					// up date the minimum price
			        if(t<MinSoFar){MinSoFar=t;}
			        // update the largest growth factor
					if(t/MinSoFar>ratio){ratio=t/MinSoFar;}
			     }
					
				return ratio;
			}
		}
		
		return null;
	}
	
	

}

