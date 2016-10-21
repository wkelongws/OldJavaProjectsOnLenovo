
public class test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String line = "118+04752,2013-01-01 03:30:34,65.0,65.0,65.0,0.6,30.0,100.0";
		String[] elements = line.split(",");
				
		String timestamp = elements[1];
		String date = timestamp.split(" ")[0];
		String time = timestamp.split(" ")[1];
		String dd = date.split("-")[0]+date.split("-")[1]+date.split("-")[2];
		int hh = Integer.parseInt(time.split(":")[0]);
		int mm = Integer.parseInt(time.split(":")[1]);
		int mm5 = mm/5;
		
		System.out.println(elements.length);
		System.out.println(timestamp);
		System.out.println(date);
		System.out.println(time);
		System.out.println(dd);
		System.out.println(hh);
		System.out.println(mm);
		System.out.println(mm5);
		
		
	}

}
