package org.antvoice.beam.formatter;

import com.google.api.services.bigquery.model.TableRow;
import org.antvoice.beam.entities.BigQueryRow;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

public class JsonRowFormatter implements SerializableFunction<String, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonRowFormatter.class);

    private TableRow convertRow(JSONObject object) {
        TableRow row = new TableRow();
        for(String key : object.keySet()) {
            if(object.isNull(key)) {
                continue;
            }

            Object value = object.get(key);
            if(!(value instanceof JSONArray)) {
                row.set(key, value);
                continue;
            }

            try {
                JSONArray arr = (JSONArray) value;
                List<TableRow> subRow = new ArrayList<>();
                if(arr.length() > 0 && arr.get(0) instanceof JSONObject) {
                    for (Object elem : arr) {
                        subRow.add(convertRow((JSONObject) elem));
                    }

                    row.set(key, subRow);
                } else {
                    List<Object> arrayContent = new ArrayList<>();
                    for (Object elem : arr) {
                        arrayContent.add(elem);
                    }

                    row.set(key, arrayContent);
                }
            } catch (Exception e) {
                LOG.error("Cannot parse nested data");
            }
        }

        return row;
    }

    @Override
    public TableRow apply(String input) {
        try {
            JSONObject obj = new JSONObject(input);

            TableRow row = convertRow(obj);

            return row;
        } catch (JSONException e) {
            LOG.error("Row cannot be parsed", e);
        }

        return null;
    }
}
