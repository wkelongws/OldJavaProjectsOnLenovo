import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
 
public class DownloadFileFromURL {
 
    public static void main(String[] args) {
        //String url = "http://205.221.97.102//Iowa.Sims.AllSites.C2C.Geofenced/IADOT_SIMS_AllSites_C2C.asmx/OP_ShareTrafficDetectorData?MSG_TrafficDetectorDataRequest=stringHTTP/1.1";
        String url = "http://205.221.97.102//Iowa.Sims.AllSites.C2C.Geofenced/IADOT_SIMS_AllSites_C2C.asmx/OP_ShareTrafficDetectorInventoryInformation?MSG_TrafficDetectorInventoryRequest=stringHTTP/1.1";
        try {
            downloadUsingNIO(url, "C:/Users/Shuo/Desktop/WavetronixData/sample_NIO.xml");
             
            downloadUsingStream(url, "C:/Users/Shuo/Desktop/WavetronixData/sample_stream.xml");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
 
    private static void downloadUsingStream(String urlStr, String file) throws IOException{
        URL url = new URL(urlStr);
        BufferedInputStream bis = new BufferedInputStream(url.openStream());
        FileOutputStream fis = new FileOutputStream(file);
        byte[] buffer = new byte[1024];
        int count=0;
        while((count = bis.read(buffer,0,1024)) != -1)
        {
            fis.write(buffer, 0, count);
        }
        fis.close();
        bis.close();
    }
 
    private static void downloadUsingNIO(String urlStr, String file) throws IOException {
        URL url = new URL(urlStr);
        ReadableByteChannel rbc = Channels.newChannel(url.openStream());
        FileOutputStream fos = new FileOutputStream(file);
        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        fos.close();
        rbc.close();
    }
 
}