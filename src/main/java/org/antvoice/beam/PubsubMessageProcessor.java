package org.antvoice.beam;

import org.antvoice.beam.entities.BigQueryRow;
import org.antvoice.beam.helper.Zip;
import org.antvoice.beam.metrics.CounterProvider;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

public class PubsubMessageProcessor
        extends PTransform<PCollection<PubsubMessage>, PCollection<BigQueryRow>> {

    private static final Logger LOG = LoggerFactory.getLogger(PubsubMessageProcessor.class);

    public PubsubMessageProcessor() {
    }

    public static class ExtractMessagesFn extends DoFn<PubsubMessage, BigQueryRow> {

        private final CounterProvider _counterProvider = new CounterProvider();
        public ExtractMessagesFn() {
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            if(c.element().getAttributeMap() == null){
                LOG.error("Dataset and table must be set in metadata");
                return;
            }

            Map<String, String> metadata = c.element().getAttributeMap();
            if(!metadata.containsKey("dataset") || !metadata.containsKey("table")){
                LOG.error("Dataset and table must be set in metadata");
                return;
            }

            String dataset = metadata.get("dataset");
            String table = metadata.get("table");
            _counterProvider.getCounter("PubsubMessageProcessor", dataset + "." + table)
                    .inc(c.element().getPayload().length);

            String message;
            if(metadata.containsKey("compression")){
                String compression = metadata.get("compression");
                if(!compression.equals("gzip")){
                    LOG.error("Unkown compression given. Only known gzip at the moment");
                    return;
                }

                try {
                    message = Zip.Unzip(c.element().getPayload());
                } catch (IOException e) {
                    LOG.error("Cannot uncompress gzip message", e);
                    return;
                }
            } else {
                try {
                    message = new String(c.element().getPayload(), "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    LOG.error("Cannot read UTF-8 string from byte array", e);
                    return;
                }
            }

            c.output(new BigQueryRow(dataset, table, message));
        }
    }

    @Override
    public PCollection<BigQueryRow> expand(PCollection<PubsubMessage> input) {
        return input.apply(ParDo.of(new ExtractMessagesFn()));
    }
}

