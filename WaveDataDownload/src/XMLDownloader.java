import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class XMLDownloader {

	public static void main(String[] args) throws IOException {
		
		int counter=0;
		do{
			long timeBefore = System.currentTimeMillis();    		// 1444766579486
			
		DateFormat dateFormat = new SimpleDateFormat("MMddyyyy");
		Date today = Calendar.getInstance().getTime();        		// Tue Oct 13 15:02:59 CDT 2015
		String reportDate = dateFormat.format(today);				// 10132015
		
		//String Folderpath = "C:/Users/yawa/Documents/Data Download/wavexml/" + reportDate +"/";
		String Folderpath = "C:/Users/Shuo/Desktop/WavetronixData/waveXML/"+ reportDate +"/";
		File filex=new File(Folderpath);
		filex.mkdir();
		URL link = new URL ("http://205.221.97.102//Iowa.Sims.AllSites.C2C.Geofenced/IADOT_SIMS_AllSites_C2C.asmx/OP_ShareTrafficDetectorData?MSG_TrafficDetectorDataRequest=stringHTTP/1.1");
		
		try{
			DateFormat dateFormat2 = new SimpleDateFormat("HH-mm-ss");
			Date today2 = Calendar.getInstance().getTime();        
			String reportDate2 = dateFormat2.format(today2);
			
			InputStream in = new BufferedInputStream(link.openStream());
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			byte[] buf = new byte[1024];
			int n=0;
		
			while(-1!=(n=in.read(buf)))
			{
				out.write(buf,0,n);
			}
			out.close();
			in.close();
			byte[] response = out.toByteArray();
		
			FileOutputStream fos = new FileOutputStream(Folderpath + reportDate2 + ".xml/");
			fos.write(response);
			fos.close();
			}catch(IOException exception)
			{
			}
		
		long timeAfter = System.currentTimeMillis();
		long elapsedtime = timeAfter - timeBefore;
		if (elapsedtime<20000){
		try {
			Thread.sleep(20000-elapsedtime);
		} catch (InterruptedException e) {
		}
		}
		}while(counter<2);

	}

}
