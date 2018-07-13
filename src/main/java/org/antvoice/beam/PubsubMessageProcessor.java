package org.antvoice.beam;

import com.google.protobuf.ByteString;
import org.antvoice.beam.metrics.CounterProvider;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class PubsubMessageProcessor extends PTransform<PCollection<PubsubMessage>, PCollection<AbstractMap.SimpleImmutableEntry<String, String>>> {

    private static final Logger LOG = LoggerFactory.getLogger(PubsubMessageProcessor.class);
    private String _project;

    public PubsubMessageProcessor(String project) {
        _project = project;
    }

    public static class ExtractMessagesFn extends DoFn<PubsubMessage, AbstractMap.SimpleImmutableEntry<String, String>> {
        private String _project;

        private final CounterProvider _counterProvider = new CounterProvider();
        public ExtractMessagesFn(String project) {
            _project = project;
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
            _counterProvider.getCounter("PubsubMessageProcessor", dataset + "." + table).inc();

            String message;
            if(metadata.containsKey("compression")){
                String compression = metadata.get("compression");
                if(!compression.equals("gzip")){
                    LOG.error("Unkown compression given. Only known gzip at the moment");
                    return;
                }

                try {
                    ByteArrayInputStream bytein = new ByteArrayInputStream(c.element().getPayload());
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

            AbstractMap.SimpleImmutableEntry row = new AbstractMap.SimpleImmutableEntry<>(_project + ":" + dataset + "." + table, message);
            c.output(row);
        }
    }

    @Override
    public PCollection<AbstractMap.SimpleImmutableEntry<String, String>> expand(PCollection<PubsubMessage> input) {
        return input.apply(ParDo.of(new ExtractMessagesFn(_project)));
    }
}

