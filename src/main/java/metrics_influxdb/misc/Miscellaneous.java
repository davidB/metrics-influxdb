package metrics_influxdb.misc;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;

public class Miscellaneous {
    public static Charset UTF8 = Charset.forName("UTF-8");

    public static String escape(String name, char... toEscape) {
        String result = name;

        for (char c : toEscape) {
            result = result.replace(Character.toString(c), "\\" + c);
        }

        return result;
    }

    public static String urlEncode(String s) throws UnsupportedEncodingException {
        return URLEncoder.encode(s, UTF8.name());
    }

    public static String readFrom(InputStream is) throws IOException {
        try (InputStreamReader isr = new InputStreamReader(is, UTF8)) {
            StringBuffer sb = new StringBuffer();
            char[] buffer = new char[2048];

            int read;
            while ((read = isr.read(buffer, 0, 2048)) != -1) {
                sb.append(buffer, 0, read);
            }
            
            return sb.toString();
        }
    }
}
