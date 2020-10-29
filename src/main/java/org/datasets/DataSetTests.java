package org.datasets;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.dashbuilder.DataSetCore;
import org.dashbuilder.dataprovider.csv.CSVDataSetProvider;
import org.dashbuilder.dataset.DataSet;
import org.dashbuilder.dataset.DataSetLookup;
import org.dashbuilder.dataset.filter.FilterFactory;
import org.dashbuilder.dataset.group.AggregateFunctionType;
import org.dashbuilder.dataset.sort.SortOrder;
import org.datasets.plug.CDIBeanDataSetProvider;

import static org.dashbuilder.dataset.DataSetLookupFactory.newDataSetLookupBuilder;
import static org.dashbuilder.dataset.def.DataSetDefFactory.newBeanDataSetDef;
import static org.dashbuilder.dataset.def.DataSetDefFactory.newCSVDataSetDef;
import static org.dashbuilder.dataset.group.AggregateFunctionType.AVERAGE;
import static org.dashbuilder.dataset.group.AggregateFunctionType.MAX;
import static org.dashbuilder.dataset.group.AggregateFunctionType.MIN;
import static org.dashbuilder.dataset.group.AggregateFunctionType.SUM;

@QuarkusMain
public class DataSetTests implements QuarkusApplication {

    private static final String SAMPLE_UUID = "Sample Dataset";

    private static final String CSV_UUID = "CSV Dataset";

    @Inject
    CDIBeanDataSetProvider cdiBeanProvider;

    @Override
    public int run(String... args) throws Exception {
        collections();
        //     dataSetSample();
        //        csvDataSet();
        return 0;
    }

    enum Position {
        PROGRAMMER,
        DIRECTOR,
        QE,
        DOCS,
        SUPPORT,
        MANAGER;
    }

    class Employee {

        String name;
        int age;
        double salary;
        Position position;

        public Employee(String name, int age, double salary, Position position) {
            super();
            this.name = name;
            this.age = age;
            this.salary = salary;
            this.position = position;
        }
        
        @Override
        public String toString() {
            return name;
        }
    }

    private void collections() {
        System.out.println("\n\n");
        var employees = List.of(new Employee("John", 35, 10000, Position.PROGRAMMER),
                                new Employee("Mary", 32, 5000, Position.PROGRAMMER),
                                new Employee("Steve", 20, 2000, Position.PROGRAMMER),
                                new Employee("Anton", 45, 15000, Position.MANAGER),
                                new Employee("Bill", 29, 8000, Position.SUPPORT),
                                new Employee("Sam", 28, 3000, Position.SUPPORT),
                                new Employee("Lisa", 35, 1000, Position.QE),
                                new Employee("Debora", 35, 5500, Position.DOCS),
                                new Employee("Mark", 46, 20000, Position.DIRECTOR));

        System.out.println("Company employees Positions" );
        System.out.println(employees.stream().collect(Collectors.groupingBy(e -> e.position, Collectors.counting())));
        
        System.out.println("The company has " + employees.stream().filter(e -> e.position == Position.PROGRAMMER).count() + " programmers");
        System.out.println("Average Employee salary: " + employees.stream().filter(e -> e.position == Position.PROGRAMMER).mapToDouble(e -> e.salary).average().orElse(-1));

        
        System.out.println("\n\n");

    }

    private void csvDataSet() {
        DataSetCore.get().getDataSetProviderRegistry().registerDataProvider(CSVDataSetProvider.get());

        var manager = DataSetCore.get().getDataSetManager();
        var url = this.getClass().getResource("/stackoverflow_minified.csv").toExternalForm();

        DataSetCore.get()
                   .getDataSetDefRegistry()
                   .registerDataSetDef(newCSVDataSetDef().uuid(CSV_UUID)
                                                         .fileURL(url)
                                                         .separatorChar(',')
                                                         .quoteChar('"').buildDef());

        var fullDs = manager.lookupDataSet(newDataSetLookupBuilder().dataset(CSV_UUID).buildLookup());

        Function<String, DataSetLookup> lookup = column -> newDataSetLookupBuilder().dataset(CSV_UUID)
                                                                                    .group(column)
                                                                                    .column(column)
                                                                                    .column(column, AggregateFunctionType.COUNT, "Total")
                                                                                    .sort("Total", SortOrder.DESCENDING)
                                                                                    .buildLookup();

        System.out.println("Available Columns:");
        fullDs.getColumns().forEach(c -> System.out.print(c.getId() + "|"));
        System.out.println();

        print(manager.lookupDataSet(lookup.apply("UndergradMajor")));
        System.out.println("\n\n");
    }

    private void dataSetSample() {
        DataSetCore.get().getDataSetProviderRegistry().registerDataProvider(cdiBeanProvider);

        var manager = DataSetCore.get().getDataSetManager();

        DataSetCore.get()
                   .getDataSetDefRegistry()
                   .registerDataSetDef(newBeanDataSetDef().generatorClass("SampleDataSet")
                                                          .uuid(SAMPLE_UUID)
                                                          .buildDef());

        print(manager.lookupDataSet(newDataSetLookupBuilder().dataset(SAMPLE_UUID).buildLookup()));

        print(manager.lookupDataSet(newDataSetLookupBuilder().dataset(SAMPLE_UUID)
                                                             .group("name")
                                                             .column("name", "Name")
                                                             .column("value", AVERAGE, "Avg")
                                                             .column("value", SUM, "Sum")
                                                             .column("value", MIN, "Min")
                                                             .column("value", MAX, "Max")
                                                             .buildLookup()));

        print(manager.lookupDataSet(newDataSetLookupBuilder().dataset(SAMPLE_UUID)
                                                             .filter("name", FilterFactory.equalsTo("V1"))
                                                             .column("name", "Name")
                                                             .column("value", "Value")
                                                             .buildLookup()));
    }

    public void print(DataSet ds) {
        System.out.println();
        var columns = ds.getColumns();
        var n = ds.getRowCount();
        var m = columns.size();
        columns.forEach(c -> System.out.print(c.getId() + "\t"));
        System.out.println();
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                System.out.print(ds.getValueAt(i, j) + "\t");
            }
            System.out.println();
        }
        System.out.println();
    }
}
