package org.antvoice.beam;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;

public class BigQueryWriter extends PTransform<PCollection<AbstractMap.SimpleImmutableEntry<String, ByteString>>, PDone> {

    private SerializableFunction<AbstractMap.SimpleImmutableEntry<String, ByteString>, TableRow> _formatter;

    public BigQueryWriter(SerializableFunction<AbstractMap.SimpleImmutableEntry<String, ByteString>, TableRow> formatter) {
        _formatter = formatter;
    }

    private static class DestinationComputer extends DynamicDestinations<AbstractMap.SimpleImmutableEntry<String, ByteString>, String>{

        @Override
        public String getDestination(ValueInSingleWindow<AbstractMap.SimpleImmutableEntry<String, ByteString>> element) {
            return element.getValue().getKey();
        }

        @Override
        public TableDestination getTable(String destination) {
            return new TableDestination(destination, null);
        }

        @Override
        public TableSchema getSchema(String destination) {
            return new TableSchema();
        }
    }

    @Override
    public PDone expand(PCollection<AbstractMap.SimpleImmutableEntry<String, ByteString>> input) {
        input.apply(BigQueryIO.<AbstractMap.SimpleImmutableEntry<String, ByteString>>write()
                .to(new DestinationComputer())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withFormatFunction(_formatter));

        return PDone.in(input.getPipeline());
    }
}


