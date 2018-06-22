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

import org.antvoice.beam.formatter.FormatterFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A process to read incoming data from pubsub and write them in
 * specified bigqueyr tables.
 *
 * <p>To run this process using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class StreamerRunner {
  private static final Logger LOG = LoggerFactory.getLogger(StreamerRunner.class);

  public static void main(String[] args) {
    StreamerOptions options = PipelineOptionsFactory
        .fromArgs(args)
        .withValidation()
        .as(StreamerOptions.class);
    Pipeline p = Pipeline.create(options);

    p.apply("ReadLines", new PubsubReader(options.getTopic(), options.getSubscription()))
        .apply("ExtractMessages", new PubsubMessageProcessor(options.getProject()))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply("WriteBq", new BigQueryWriter(new FormatterFactory(options.getFormat()).getFormatter()));

    p.run().waitUntilFinish();
  }
}
