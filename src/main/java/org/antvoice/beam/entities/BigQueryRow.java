package org.antvoice.beam.entities;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class BigQueryRow {
    private String dataset;
    private String table;
    private String row;

    public BigQueryRow(){

    }

    public BigQueryRow(String dataset, String table, String row) {
        this.dataset = dataset;
        this.table = table;
        this.row = row;
    }

    public String getDataset() {
        return dataset;
    }

    public String getTable() {
        return table;
    }

    public String getRow() {
        return row;
    }
}
