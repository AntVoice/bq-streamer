package org.antvoice.beam.formatter;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.AbstractMap;

public class JsonRowFormatter implements SerializableFunction<AbstractMap.SimpleImmutableEntry<String, ByteString>, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonRowFormatter.class);

    @Override
    public TableRow apply(AbstractMap.SimpleImmutableEntry<String, ByteString> input) {
        try {
            String message = new String(input.getValue().toByteArray(), "UTF-8");
            JSONObject obj = new JSONObject(message);
            TableRow row = new TableRow();
            for(String key : obj.keySet()) {
                if(!obj.isNull(key)){
                    row.set(key, obj.get(key));
                }
            }

            return row;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return null;
    }
}
