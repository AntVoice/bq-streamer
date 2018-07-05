package org.antvoice.beam;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubsubReader extends PTransform<PBegin, PCollection<PubsubMessage>> {

    private static final Logger LOG = LoggerFactory.getLogger(PubsubReader.class);

    private String _topic;
    private String _subscription;

    public PubsubReader(String topic, String subscription) {
        _topic = topic;
        _subscription = subscription;
    }

    @Override
    public PCollection<PubsubMessage> expand(PBegin input) {
        if((_topic == null || _topic.isEmpty()) &&
                (_subscription == null || _subscription.isEmpty())){
            LOG.error("Either topic or subscription parameters must be set");
        }

        if(_topic != null && !_topic.isEmpty() && _subscription != null && !_subscription.isEmpty()){
            LOG.error("Both topic and subscription parameters cannot be set at the same time");
        }

        PubsubIO.Read<PubsubMessage> pubsubMessageRead = PubsubIO.readMessagesWithAttributes();
        if(_topic != null && !_topic.isEmpty()){
            pubsubMessageRead = pubsubMessageRead.fromTopic(_topic);
        }

        LOG.info("Starting to listen to " + _subscription + " subrscription");
        pubsubMessageRead = pubsubMessageRead.fromSubscription(_subscription);

        return input.apply(pubsubMessageRead.withIdAttribute("uniqueId"));
    }
}
