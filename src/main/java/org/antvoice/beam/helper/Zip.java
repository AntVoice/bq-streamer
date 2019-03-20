package org.antvoice.beam.helper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class Zip {

    public static String Unzip(byte[] content) throws IOException {
        String message;
        ByteArrayInputStream bytein = new ByteArrayInputStream(content);
        GZIPInputStream gzip = new GZIPInputStream(bytein);
        ByteArrayOutputStream byteout = new ByteArrayOutputStream();

        int res = 0;
        byte buf[] = new byte[1024];
        while (res >= 0) {
            res = gzip.read(buf, 0, buf.length);
            if (res > 0) {
                byteout.write(buf, 0, res);
            }
        }

        byte uncompressed[] = byteout.toByteArray();
        message = new String(uncompressed, "UTF-8");
        return message;
    }

}
