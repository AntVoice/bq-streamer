package org.antvoice.beam;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;

public class PubsubMessageProcessor extends PTransform<PCollection<PubsubMessage>, PCollection<AbstractMap.SimpleImmutableEntry<String, ByteString>>> {

    private static final Logger LOG = LoggerFactory.getLogger(PubsubMessageProcessor.class);
    private String _project;

    public PubsubMessageProcessor(String project) {
        _project = project;
    }

    public static class ExtractMessagesFn extends DoFn<PubsubMessage, AbstractMap.SimpleImmutableEntry<String, ByteString>> {
        private String _project;

        public ExtractMessagesFn(String project) {
            _project = project;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            if(c.element().getAttributeMap() == null){
                LOG.error("Dataset and table must be set in metadata");
                return;
            }

            String dataset = c.element().getAttribute("dataset");
            String table = c.element().getAttribute("table");
            if(dataset.isEmpty() || table.isEmpty()){
                LOG.error("Dataset and table must be set in metadata");
                return;
            }

            LOG.info("Received a row for " + dataset + "." + table);
            AbstractMap.SimpleImmutableEntry row = new AbstractMap.SimpleImmutableEntry<>(_project + ":" + dataset + "." + table, ByteString.copyFrom(c.element().getPayload()));
            c.output(row);
        }
    }

    @Override
    public PCollection<AbstractMap.SimpleImmutableEntry<String, ByteString>> expand(PCollection<PubsubMessage> input) {
        return input.apply(ParDo.of(new ExtractMessagesFn(_project)));
    }
}

