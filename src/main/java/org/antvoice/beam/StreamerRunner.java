/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.antvoice.beam;

import org.antvoice.beam.entities.BigQueryRow;
import org.antvoice.beam.formatter.FormatterFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A process to read incoming data from pubsub and write them in
 * specified bigquery tables.
 *
 * <p>To run this process using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class StreamerRunner {

    public static void main(String[] args) throws Exception {
        StreamerOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(StreamerOptions.class);
        Pipeline p = Pipeline.create(options);
        long currentTimeMillis = System.currentTimeMillis();

        PCollection<BigQueryRow> rows = p.apply("ReadLines", new PubsubReader(options.getTopic(), options.getSubscription()))
                .apply("ExtractMessages", new PubsubMessageProcessor())
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowDuration()))));

        if(options.getDumpGoogleStorage()) {
            if(options.getDumpLocation().isEmpty() || !options.getDumpLocation().startsWith("gs://")){
                throw new Exception("Invalid dumpLocation parameter. Must be a valid Google Storage URI.");
            }
            rows.apply("WriteGS", new GoogleStorageWriter(options.getDumpLocation(), currentTimeMillis));
        }else {
            rows.apply("WriteBq", new BigQueryWriter(options.getProject(),
                    new FormatterFactory(options.getFormat()).getFormatter()));
        }

        if(options.getAttached()){
            p.run().waitUntilFinish();
        }else {
            p.run();
        }
    }
}

