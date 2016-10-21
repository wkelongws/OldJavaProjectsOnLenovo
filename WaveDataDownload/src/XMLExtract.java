import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;

public class XMLExtract {
public static void main(String argv[]) throws IOException {
		
		
		//String path_xml ="C:/Users/yawa/Desktop/Wavetronix Database/day1.Database/data/";
		String path_xml ="C:/Users/Shuo/Desktop/WavetronixData/wavexml/10142015/";
		//String path_xml ="C:/Users/shuowang/Desktop/dataCollection/data/wavetronix/";
		String output ="C:/Users/Shuo/Desktop/WavetronixData/wavecsv/";
		
		String files_out;

		File xml_folder = new File(path_xml);

		File [] list_Files = xml_folder.listFiles(new FilenameFilter(){

			@Override
			public boolean accept(File folder, String name){
				return name.endsWith(".xml");				
			}
		});
		for (int ab=0;ab<list_Files.length;ab++){
		files_out = list_Files[ab].getName();
		String inputfile = path_xml+files_out;
		PrintStream out = new PrintStream(new FileOutputStream(output +"Wave_Data2"+".txt", true));
		System.setOut(out);
		try {
			DocumentBuilderFactory factory =DocumentBuilderFactory.newInstance();
			 DocumentBuilder builder =factory.newDocumentBuilder();
			 Document document = builder.parse(new File(inputfile));
			 document.getDocumentElement().normalize();
			 
			 NodeList timestamps= document.getElementsByTagName("detection-time-stamp");
			 //System.out.print(timestamps.getLength() + ",");
			 
			 NodeList detectorReports = document.getElementsByTagName("detector-report");
			 
			 Element timestamp = (Element) timestamps.item(0);
			 String localdata = timestamp.getElementsByTagName("local-date").item(0).getTextContent();
			 String localtime = timestamp.getElementsByTagName("local-time").item(0).getTextContent();
			 

				 			 
			for (int j=0; j<detectorReports.getLength(); ++j){
				
				 Element detectorReport = (Element) detectorReports.item(j);
				 String Detectors = detectorReport.getElementsByTagName("detector-id").item(0).getTextContent();
				 if(Detectors.equals("EB to 86th STREET-EB")||Detectors.equals("I-35/80 EB @ 86th St")||Detectors.equals("I-35/80 EB from 2nd AVENUE-WB")||Detectors.equals("I-35/80 EB to 2nd AVENUE-WB")||Detectors.equals("I-35/80 @ MP 128.5")||Detectors.equals("I-35/80 WB to 14th STREET-WB")){
				//if(Detectors.equals("I-35 SB @ MP 94")){
				System.out.print(Detectors + ",");
				 NodeList lanes = detectorReport.getElementsByTagName("lane");

				 if(lanes.getLength()>0){
					 for (int ja=0;ja<lanes.getLength();ja++){
						 String speed = detectorReport.getElementsByTagName("speed").item(ja).getTextContent();
						 System.out.print(speed + ",");
						 System.out.print(localdata + localtime +",");
						 String volume = detectorReport.getElementsByTagName("occupancy").item(ja).getTextContent();
						 System.out.print(volume + ",");
						 
					 }
				 }else{
					 System.out.print(" ");
				 }
				 System.out.println(",");
			 }
			}
		}catch (Exception e) {
			e.printStackTrace();
	    }
		}
		
	}

}
