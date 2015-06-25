import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

import cn.b2b.common.search.segment.SegmentManager;


public class TestSegment {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
  
        SegmentManager manager = SegmentManager.getInstance();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(args[0]), Charset.forName("GBK")));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args[1]), Charset.forName("GBK")));
        String line = null;
        while ((line = reader.readLine()) != null) {
            String seg = manager.segment(line.toLowerCase());
            writer.write(seg + "\n");
        }
        reader.close();
        writer.close();
    }

}
