package org.datasets.plug;

import java.util.Map;

import javax.enterprise.context.Dependent;

import org.dashbuilder.dataset.ColumnType;
import org.dashbuilder.dataset.DataSet;
import org.dashbuilder.dataset.DataSetFactory;
import org.dashbuilder.dataset.DataSetGenerator;

@Dependent
public class SampleDataSet implements DataSetGenerator {

    @Override
    public DataSet buildDataSet(Map<String, String> params) {
        System.out.println("Generating sample data...");
        return DataSetFactory.newDataSetBuilder()
                             .column("ID", ColumnType.LABEL)
                             .column("name", ColumnType.LABEL)
                             .column("value", ColumnType.NUMBER)
                             .row(10, "V1", 5)
                             .row(20, "V1", 6)
                             .row(30, "V2", 7)
                             .row(40, "V2", 8)
                             .buildDataSet();
    }

}
