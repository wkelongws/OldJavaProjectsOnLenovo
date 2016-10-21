import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;



public class WavetronixInventoryTracker {

	public static void main(String[] args) throws IOException {
		
		HashMap<String,String> wavetronix = new HashMap<String,String>();
		// ArrayList<String> presensors = new ArrayList<String>();
		int counter=0;
		do{
			//counter++;
			long timeBefore = System.currentTimeMillis();    		// 1444766579486
			
		DateFormat dateFormat = new SimpleDateFormat("MMddyyyy");
		Date today = Calendar.getInstance().getTime();        		// Tue Oct 13 15:02:59 CDT 2015
		String reportDate = dateFormat.format(today);				// 10132015
		
		String Folderpath = "C:/Users/shuowang/Desktop/WaveDownload/WaveInventory/XML/";
		//String Folderpath = "C:/Users/Shuo/Desktop/WavetronixData/WaveInventory/XML/";
		File filex=new File(Folderpath);
		filex.mkdir();
		URL link = new URL ("http://205.221.97.102//Iowa.Sims.AllSites.C2C.Geofenced/IADOT_SIMS_AllSites_C2C.asmx/OP_ShareTrafficDetectorInventoryInformation?MSG_TrafficDetectorInventoryRequest=stringHTTP/1.1");
		
		String output="C:/Users/shuowang/Desktop/WaveDownload/WaveInventory/";
		//String output="C:/Users/Shuo/Desktop/WavetronixData/WaveInventory/";
		
		PrintStream outxx2 = new PrintStream(new FileOutputStream(output+  "WavetronixInventoryTracking.txt", true));
		System.setOut(outxx2);		
						
		try{
			DateFormat dateFormat2 = new SimpleDateFormat("HH-mm-ss");
			Date today2 = Calendar.getInstance().getTime();        
			String reportDate2 = dateFormat2.format(today2);		// 16-01-01
			
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
			
			FileOutputStream fos = new FileOutputStream(Folderpath + reportDate + "_" + reportDate2 + ".xml/");
			fos.write(response);
			fos.close();
			
			DocumentBuilderFactory factory =DocumentBuilderFactory.newInstance();
			DocumentBuilder builder =factory.newDocumentBuilder();	
			Document document = builder.parse(new File(Folderpath + reportDate + "_" + reportDate2 + ".xml/"));
			//C:\Users\Shuo\Desktop\WavetronixData\waveXML\10142015\10-15-01.xml
			
			// xml -> document -(getElementsByTagName)-> NodeList -(item)-> Node -(getTextContent)-> string
			//Node -(element)-> element -(getElementsByTagName)-> NodeList -(item)-> Node -(getTextContent)-> string
			document.getDocumentElement().normalize();
			
			if (wavetronix.isEmpty())
			{
				System.out.println(reportDate + " " + reportDate2);
				System.out.println("tracking started");
				System.out.println();				
				NodeList detectors = document.getElementsByTagName("detector");
				for (int ixa=0; ixa<detectors.getLength(); ixa++)
				{
					Element onedetector = (Element) detectors.item(ixa);
					String detectorID = onedetector.getElementsByTagName("detector-id").item(0).getTextContent();
					String Lat = "null";
					String Long = "null";
					int cdetectorloc = onedetector.getElementsByTagName("detector-location").getLength();
					if (cdetectorloc>0)
					{
						Element detectorloc = (Element) onedetector.getElementsByTagName("detector-location").item(0);
						int clat = detectorloc.getElementsByTagName("latitude").getLength();
						int clong = detectorloc.getElementsByTagName("longitude").getLength();
						if (clat>0){Lat = detectorloc.getElementsByTagName("latitude").item(0).getTextContent();}
						if (clong>0){Long = detectorloc.getElementsByTagName("longitude").item(0).getTextContent();}
					}
					String loc = Lat + "," + Long;
					wavetronix.put(detectorID,loc);				
				}
			}
			else 
			{			
			String removed = "";
			String added = "";
			String changedloc = "";
			ArrayList<String> sensors = new ArrayList<String>();
			NodeList detectors = document.getElementsByTagName("detector");
			for (int ixa=0; ixa<detectors.getLength(); ixa++)
			{
				Element onedetector = (Element) detectors.item(ixa);
				String detectorID = onedetector.getElementsByTagName("detector-id").item(0).getTextContent();
				sensors.add(detectorID);
				String Lat = "null";
				String Long = "null";
				int cdetectorloc = onedetector.getElementsByTagName("detector-location").getLength();
				if (cdetectorloc>0)
				{
					Element detectorloc = (Element) onedetector.getElementsByTagName("detector-location").item(0);
					int clat = detectorloc.getElementsByTagName("latitude").getLength();
					int clong = detectorloc.getElementsByTagName("longitude").getLength();
					if (clat>0){Lat = detectorloc.getElementsByTagName("latitude").item(0).getTextContent();}
					if (clong>0){Long = detectorloc.getElementsByTagName("longitude").item(0).getTextContent();}
				}
				String loc = Lat + "," + Long;
				if (wavetronix.containsKey(detectorID))
				{
					String preloc = wavetronix.get(detectorID);
					if (!preloc.equals(loc))
					{
						changedloc = changedloc+"\t"+detectorID+": from "+preloc+" to "+loc+";\n";											
					}
				}
				else 
				{
					added = added+"\t"+detectorID+","+loc+";\n";
				}
				wavetronix.put(detectorID,loc);									
			}
			for (String s:wavetronix.keySet())
			{
				if(!sensors.contains(s))
				{
					removed = removed+"\t"+s+","+wavetronix.get(s)+";\n";
					wavetronix.remove(s);
				}
			}
			System.out.println("\t"+reportDate + " " + reportDate2);
			if(added.length()>0){System.out.println("sensor added:\n"+added);}			
			if(removed.length()>0){System.out.println("sensor removed:\n"+removed);}
			if(changedloc.length()>0){System.out.println("location changed:\n"+changedloc);}
			if(added.length()==0 & removed.length()==0 & changedloc.length()==0)
			{System.out.println("\t"+"no change");}
			System.out.println();			
			}
			
			}catch(Exception exception)
			{
			}
		
		long timeAfter = System.currentTimeMillis();
		long elapsedtime = timeAfter - timeBefore;
		//System.out.println(elapsedtime);
		if (elapsedtime<24*60*60*1000){
		try {
			Thread.sleep(24*60*60*1000-elapsedtime);
		} catch (InterruptedException e) {
		}
		}
		}while(counter<2);

	}

}
