import java.text.SimpleDateFormat;


public class test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String today = new SimpleDateFormat("MMddyyyy hh:mm:ss").format(new java.util.Date());
		System.out.println(today);
	}

}
