import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import javax.swing.text.Document;


public class test {

	public static void main(String[] args) throws IOException {
		HashMap<String,String> a = new HashMap<String,String>();
		
		a.put("a","1,2");
		System.out.println(a);
		a.put("b","2,3");
		System.out.println(a);
		a.put("b","3,3");
		System.out.println(a);
		
		a.remove("a");
		System.out.println(a);
		
		
	}

}
