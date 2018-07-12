package org.antvoice.beam;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface StreamerOptions extends GcpOptions {
    @Description("Source topic")
    String getTopic();
    void setTopic(String value);

    @Description("Source subscription")
    String getSubscription();
    void setSubscription(String value);

    @Description("Serializtion type. Uses uncompressed JSON as a default. No other value at the moment")
    String getFormat();
    void setFormat(String value);

    @Description("The duration of the messages window in seconds.")
    @Default.Integer(10)
    Integer getWindowDuration();
    void setWindowDuration(Integer durationSeconds);

    @Description("Should the deployment hang until finished")
    Boolean getAttached();
    void setAttached(Boolean value);
}
