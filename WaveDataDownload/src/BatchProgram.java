import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
public class BatchProgram {
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

     String output="C:/Users/Shuo/Desktop/WavetronixData/waveCSV/";
     
     String Directory = "C:/Users/Shuo/Desktop/WavetronixData/wavexml/";
         
     String[] bk = new File (Directory).list();
     
     for (int axy=0; axy<bk.length; axy++){ // loop through all daily folders
    	 
    	 String xmlpath = Directory + bk[axy] +"/";
    	 // C:\Users\Shuo\Desktop\WavetronixData\waveXML\10142015\
     
     File folder = new File(xmlpath);
		
		File [] listOfFiles = folder.listFiles(new FilenameFilter(){
			
			@Override
			public boolean accept(File folder, String name){
				return name.endsWith(".xml");
			}
			
		});
		//C:\Users\Shuo\Desktop\WavetronixData\waveXML\10142015\10-15-01.xml
		
		for (int ijk=0; ijk<listOfFiles.length; ijk++){ //loop though all xml files in one day folder
			String inputfile = listOfFiles[ijk].getName(); //10-15-01.xml
			PrintStream outxx2 = new PrintStream(new FileOutputStream(output+  bk[axy] + ".txt", true));
			System.setOut(outxx2);
			try{
				DocumentBuilderFactory factory =DocumentBuilderFactory.newInstance();
				DocumentBuilder builder =factory.newDocumentBuilder();	
				Document document = builder.parse(new File(xmlpath+inputfile));
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
												
						/*if (curClass.getLength()==3){
							for (int qa=0; qa<curClass.getLength(); qa++){ // loop through all classes
								 smallCount="null";  smallVolume="null";  mediumCount = "null"; mediumVolume = "null";
								 largeCount = "null"; largeVolume = "null";
								Element Cls = (Element) curClass.item(qa);
								if (Cls.getElementsByTagName("count").getLength()>0){smallCount = Cls.getElementsByTagName("count").item(0).getTextContent();}
								if (Cls.getElementsByTagName("volume").getLength()>0){smallVolume = Cls.getElementsByTagName("volume").item(0).getTextContent();}
								System.out.print(smallCount +"," + smallVolume +"," );
							}
						}else if(curClass.getLength()==2){
							for (int qa=0; qa<curClass.getLength(); qa++){
								 smallCount="null";  smallVolume="null";  mediumCount = "null"; mediumVolume = "null";
								 largeCount = "null"; largeVolume = "null";
								Element Cls = (Element) curClass.item(qa);
								if (Cls.getElementsByTagName("count").getLength()>0){smallCount = Cls.getElementsByTagName("count").item(0).getTextContent();}
								if (Cls.getElementsByTagName("volume").getLength()>0){smallVolume = Cls.getElementsByTagName("volume").item(0).getTextContent();}
								System.out.print(smallCount +"," + smallVolume +"," );
							}
							System.out.print(smallCount +"," + smallVolume +"," );
							
						}else if(curClass.getLength()==1){
							for (int qa=0; qa<curClass.getLength(); qa++){
								 smallCount="null";  smallVolume="null";  mediumCount = "null"; mediumVolume = "null";
								 largeCount = "null"; largeVolume = "null";
								Element Cls = (Element) curClass.item(qa);
								if (Cls.getElementsByTagName("count").getLength()>0){smallCount = Cls.getElementsByTagName("count").item(0).getTextContent();}
								if (Cls.getElementsByTagName("volume").getLength()>0){smallVolume = Cls.getElementsByTagName("volume").item(0).getTextContent();}
								System.out.print(smallCount +"," + smallVolume +"," );
							}
							System.out.print(smallCount +"," + smallVolume +"," );
							System.out.print(smallCount +"," + smallVolume +"," );
							
						}*/
					}						
						System.out.print(smallCount +"," + smallVolume +"," + mediumCount +"," + mediumVolume +"," + largeCount +"," + largeVolume +",");
					}
					System.out.println();
				}
				
			}catch (Exception e) {
				e.printStackTrace();
		    }
		}
     }
     
	}

}
