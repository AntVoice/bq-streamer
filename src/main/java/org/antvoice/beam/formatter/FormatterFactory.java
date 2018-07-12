package org.antvoice.beam.formatter;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.util.AbstractMap;

public class FormatterFactory {

    private String _format;

    public FormatterFactory(String format) {
        _format = format;
    }

    public SerializableFunction<AbstractMap.SimpleImmutableEntry<String, String>, TableRow> getFormatter() {
        // Only known format at the moment
        return new JsonRowFormatter();
    }
}
