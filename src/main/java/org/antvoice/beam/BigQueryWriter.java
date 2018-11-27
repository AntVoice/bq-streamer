package org.antvoice.beam;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.antvoice.beam.entities.BigQueryRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.ValueInSingleWindow;

public class BigQueryWriter extends PTransform<PCollection<BigQueryRow>, PDone> {

    private String _project;
    private SerializableFunction<String, TableRow> _formatter;

    public BigQueryWriter(String project, SerializableFunction<String, TableRow> formatter) {
        _project = project;
        _formatter = formatter;
    }

    private static class DestinationComputer
            extends DynamicDestinations<BigQueryRow, String>{

        private String _project;

        public DestinationComputer(String project) {
            _project = project;
        }

        @Override
        public String getDestination(ValueInSingleWindow<BigQueryRow> element) {
            return _project + ":" + element.getValue().getDataset() + "." + element.getValue().getTable() ;
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
    public PDone expand(PCollection<BigQueryRow> input) {
        input.apply(BigQueryIO.<BigQueryRow>write()
                .to(new DestinationComputer(_project))
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withFormatFunction(row -> _formatter.apply(row.getRow())));

        return PDone.in(input.getPipeline());
    }
}


