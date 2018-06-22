package org.antvoice.beam;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface StreamerOptions extends GcpOptions {
    /*@Description("Google cloud project")
    @Validation.Required
    @Default.InstanceFactory(GcpOptions.DefaultProjectFactory.class)
    String getProject();
    void setProject(String value);

    @Description("A Cloud Storage path for Cloud Dataflow to stage any temporary files")
    @Default.InstanceFactory(GcpOptions.GcpTempLocationFactory.class)
    String getGcpTempLocation();
    void setGcpTempLocation(String value);

    @Description("A Cloud Storage bucket for Cloud Dataflow to stage your binary files")
    @Default.String("")
    String getStagingLocation();
    void setStagingLocation(String value);*/

    @Description("Source topic")
    String getTopic();
    void setTopic(String value);

    @Description("Source subscription")
    String getSubscription();
    void setSubscription(String value);

    @Description("Serializtion type. Uses uncompressed JSON as a default. No other value at the moment")
    String getFormat();
    void setFormat(String value);
}
