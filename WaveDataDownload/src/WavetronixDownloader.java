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
import java.util.Calendar;
import java.util.Date;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class WavetronixDownloader {

	public static void main(String[] args) throws IOException {
		
		int counter=0;
		do{
			long timeBefore = System.currentTimeMillis();    		// 1444766579486
			
		DateFormat dateFormat = new SimpleDateFormat("MMddyyyy");
		Date today = Calendar.getInstance().getTime();        		// Tue Oct 13 15:02:59 CDT 2015
		String reportDate = dateFormat.format(today);				// 10132015
		
		String Folderpath = "C:/Users/Shuo/Desktop/WavetronixData/waveXML/"+ reportDate +"/";
		File filex=new File(Folderpath);
		filex.mkdir();
		URL link = new URL ("http://205.221.97.102//Iowa.Sims.AllSites.C2C.Geofenced/IADOT_SIMS_AllSites_C2C.asmx/OP_ShareTrafficDetectorData?MSG_TrafficDetectorDataRequest=stringHTTP/1.1");
		
		String output="C:/Users/Shuo/Desktop/WavetronixData/waveCSV/";
		
		PrintStream outxx2 = new PrintStream(new FileOutputStream(output+  reportDate + ".txt", true));
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
			
			FileOutputStream fos = new FileOutputStream(Folderpath + reportDate2 + ".xml/");
			fos.write(response);
			fos.close();
						
			DocumentBuilderFactory factory =DocumentBuilderFactory.newInstance();
			DocumentBuilder builder =factory.newDocumentBuilder();	
			Document document = builder.parse(new File(Folderpath + reportDate2 + ".xml/"));
			//C:\Users\Shuo\Desktop\WavetronixData\waveXML\10142015\10-15-01.xml
			
			// xml -> document -(getElementsByTagName)-> NodeList -(item)-> Node -(getTextContent)-> string
			//Node -(element)-> element -(getElementsByTagName)-> NodeList -(item)-> Node -(getTextContent)-> string
			document.getDocumentElement().normalize();
			NodeList timestamps = document.getElementsByTagName("detection-time-stamp");
			Element timestamp = (Element) timestamps.item(0);
			String date = timestamp.getElementsByTagName("local-date").item(0).getTextContent();
			//System.out.println();
			
			NodeList starttime = document.getElementsByTagName("start-time");
			Node stNode = starttime.item(0);
			String starttimer = stNode.getTextContent();
			
			NodeList endtime = document.getElementsByTagName("end-time");
			Node enNode = endtime.item(0);
			String endtimer = enNode.getTextContent();
			
			NodeList detectorReports = document.getElementsByTagName("detector-report");
			for (int ixa=0; ixa<detectorReports.getLength(); ixa++){
				Element detectorReport = (Element) detectorReports.item(ixa);
				String detectorID = detectorReport.getElementsByTagName("detector-id").item(0).getTextContent();
				String status = detectorReport.getElementsByTagName("status").item(0).getTextContent();
				
				
				NodeList Lanes = detectorReport.getElementsByTagName("lanes");
				Element Ln = (Element) Lanes.item(0);
				int tl = Ln.getElementsByTagName("lane").getLength();
				String numOfLanes = Integer.toString(tl);

				System.out.print(detectorID +","+date+","+starttimer+","+endtimer+","+status +","+ numOfLanes +",");
				
				NodeList curLanes = Ln.getElementsByTagName("lane");
				String laneId = "null";String count = "null";String volume = "null";String occupancy = "null";String speed = "null";
				for (int ixb=0; ixb<tl; ++ixb){ // loop through all lanes
					
					Element Lns = (Element) curLanes.item(ixb);
					int claneid =Lns.getElementsByTagName("lane-id").getLength();
					if (claneid>0){ laneId = Lns.getElementsByTagName("lane-id").item(0).getTextContent();}else{ laneId = "null";}
					
					int ccount =Lns.getElementsByTagName("count").getLength();
					if (ccount>0){ count = Lns.getElementsByTagName("count").item(0).getTextContent();}else{ count = "null";}
					
					int cvolume =Lns.getElementsByTagName("volume").getLength();
					if (cvolume>0){ volume = Lns.getElementsByTagName("volume").item(0).getTextContent();}else{ volume = "null";}
					
					int coccupancy =Lns.getElementsByTagName("occupancy").getLength();
					if (coccupancy>0){ occupancy = Lns.getElementsByTagName("occupancy").item(0).getTextContent();}else{ occupancy = "null";}
					
					int cspeed =Lns.getElementsByTagName("speed").getLength();
					if (cspeed>0){ speed = Lns.getElementsByTagName("speed").item(0).getTextContent();}else{ speed = "null";}

					System.out.print(laneId +","+count +","+volume +","+occupancy +","+speed +",");
					
					String smallCount="null"; String smallVolume="null"; String mediumCount = "null";String mediumVolume = "null";
					String largeCount = "null";String largeVolume = "null";
					
					NodeList Classes = Lns.getElementsByTagName("classes");
					Element Cl = (Element) Classes.item(0);
					if (Classes.getLength()>0){
					NodeList curClass = Cl.getElementsByTagName("class");
					
					if (curClass.getLength()>0)
					{
						for (int qa=0; qa<curClass.getLength(); qa++) // loop through all classes
						{
							Element Cls = (Element) curClass.item(qa);
							if (Cls.getElementsByTagName("class-id").getLength()>0)
							{
								String classid = Cls.getElementsByTagName("class-id").item(0).getTextContent();
								if (classid.equals("Small"))
								{
									if (Cls.getElementsByTagName("count").getLength()>0)
									{
										smallCount = Cls.getElementsByTagName("count").item(0).getTextContent();
									}
									if (Cls.getElementsByTagName("volume").getLength()>0)
									{
										smallVolume = Cls.getElementsByTagName("volume").item(0).getTextContent();
									}
								}
								if (classid.equals("Medium"))
								{
									if (Cls.getElementsByTagName("count").getLength()>0)
									{
										mediumCount = Cls.getElementsByTagName("count").item(0).getTextContent();
									}
									if (Cls.getElementsByTagName("volume").getLength()>0)
									{
										mediumVolume = Cls.getElementsByTagName("volume").item(0).getTextContent();
									}
								}
								if (classid.equals("Large"))
								{
									if (Cls.getElementsByTagName("count").getLength()>0)
									{
										largeCount = Cls.getElementsByTagName("count").item(0).getTextContent();
									}
									if (Cls.getElementsByTagName("volume").getLength()>0)
									{
										largeVolume = Cls.getElementsByTagName("volume").item(0).getTextContent();
									}
								}
							}								
						}
					}						
				}						
					System.out.print(smallCount +"," + smallVolume +"," + mediumCount +"," + mediumVolume +"," + largeCount +"," + largeVolume +",");
				}
				System.out.println();
			}
			
			
			
			}catch(Exception exception)
			{
			}
		

		//C:\Users\Shuo\Desktop\WavetronixData\waveXML\10142015\10-15-01.xml
		
		
			

		
		
		long timeAfter = System.currentTimeMillis();
		long elapsedtime = timeAfter - timeBefore;
		//System.out.println(elapsedtime);
		if (elapsedtime<20000){
		try {
			Thread.sleep(20000-elapsedtime);
		} catch (InterruptedException e) {
		}
		}
		}while(counter<2);

	}

}
