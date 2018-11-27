package org.antvoice.beam;

import org.antvoice.beam.entities.BigQueryRow;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class GoogleStorageWriter
        extends PTransform<PCollection<BigQueryRow>, PDone> {

    private String _dumpLocation;
    private long _currentTimeMillis;

    public GoogleStorageWriter(String dumpLocation, long currentTimeMillis) {
        _dumpLocation = dumpLocation;
        this._currentTimeMillis = currentTimeMillis;
    }

    @Override
    public PDone expand(PCollection<BigQueryRow> input) {
        input.apply(
                FileIO
                        .<String, BigQueryRow>writeDynamic()
                        .by((SerializableFunction<BigQueryRow, String>)
                                row -> String.format("%s/%s/", row.getDataset(), row.getTable()))
                        .via(Contextful.fn((SerializableFunction<BigQueryRow, String>) BigQueryRow::getRow),
                                TextIO.sink())
                        .to(_dumpLocation)
                        .withNaming(partition -> FileIO.Write.defaultNaming(partition, ""))
                        .withDestinationCoder(StringUtf8Coder.of())
                        .withNumShards(10));

        return PDone.in(input.getPipeline());
    }
}
