package dataLoader;

import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import filereader.CsvReader;
import person.Person;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CustomerDataLoader {

    private static final String COLUMN_FAMILY = "Person";
    private static final String COLUMN_QUALIFIER_ID = "id";
    private static final String COLUMN_QUALIFIER_FIRSTNAME = "firstName";
    private static final String COLUMN_QUALIFIER_LASTNAME = "lastName";
    private static final String COLUMN_QUALIFIER_GENDER = "gender";
    private static final String COLUMN_QUALIFIER_BIRTHDAY = "birthday";
    private static final String COLUMN_QUALIFIER_CREATION_DATE = "creationDate";
    private static final String COLUMN_QUALIFIER_LOCATION_IP = "locationIP";
    private static final String COLUMN_QUALIFIER_BROWSER_USED = "browserUsed";
    private static final String COLUMN_QUALIFIER_PLACE = "place";
    private static final String ROW_KEY_PREFIX = "rowKey";
    private final String tableId;
    private final BigtableDataClient dataClient;
    private final BigtableTableAdminClient adminClient;
    private static final String projectId = "projetbddm1";
    private static final String instanceId = "projetbddm";
    private CsvReader csvReader;
    private ArrayList<Person> persons;

    public CustomerDataLoader(String projectId, String instanceId, String tableId) throws IOException {
        this.tableId = tableId;
        this.csvReader = new CsvReader();
        this.persons = csvReader.getPersonsDataListFromCsvFile();


        // [START bigtable_hw_connect]
        // Creates the settings to configure a bigtable data client.
        BigtableDataSettings settings =
                BigtableDataSettings.newBuilder().setProjectId(projectId).setInstanceId(instanceId).build();

        // Creates a bigtable data client.
        dataClient = BigtableDataClient.create(settings);

        // Creates the settings to configure a bigtable table admin client.
        BigtableTableAdminSettings adminSettings =
                BigtableTableAdminSettings.newBuilder()
                        .setProjectId(projectId)
                        .setInstanceId(instanceId)
                        .build();

        // Creates a bigtable table admin client.
        adminClient = BigtableTableAdminClient.create(adminSettings);
        // [END bigtable_hw_connect]
    }

    public static void main(String[] args) throws Exception {
        CustomerDataLoader loader = new CustomerDataLoader(projectId, instanceId, "customer");
        loader.run();
    }

    public void run() throws Exception {
        createTable();
        writeToTable();
        readSingleRow();
        readSpecificCells();
        readTable();
//        deleteTable();
        close();
    }

    public void close() {
        dataClient.close();
        adminClient.close();
    }

    /** Demonstrates how to create a table. */
    public void createTable() {
        if (!adminClient.exists(tableId)) {
            System.out.println("Creating table: " + tableId);
            CreateTableRequest createTableRequest =
                    CreateTableRequest.of(tableId).addFamily(COLUMN_FAMILY);
            adminClient.createTable(createTableRequest);
            System.out.printf("Table %s created successfully%n", tableId);
        }
    }

    public void writeToTable() {
        try {
            int i = 0;
            this.persons.forEach((person) -> {
                RowMutation rowMutation =
                        RowMutation.create(tableId, ROW_KEY_PREFIX + i)
                                .setCell(COLUMN_FAMILY, COLUMN_QUALIFIER_ID, person.getId())
                                .setCell(COLUMN_FAMILY, COLUMN_QUALIFIER_FIRSTNAME, person.getFirstName())
                                .setCell(COLUMN_FAMILY, COLUMN_QUALIFIER_LASTNAME, person.getLastName())
                                .setCell(COLUMN_FAMILY, COLUMN_QUALIFIER_GENDER, person.getGender())
                                .setCell(COLUMN_FAMILY, COLUMN_QUALIFIER_BIRTHDAY, person.getBirthday())
                                .setCell(COLUMN_FAMILY, COLUMN_QUALIFIER_CREATION_DATE, person.getCreationDate())
                                .setCell(COLUMN_FAMILY, COLUMN_QUALIFIER_LOCATION_IP, person.getLocationIP())
                                .setCell(COLUMN_FAMILY, COLUMN_QUALIFIER_BROWSER_USED, person.getBrowserUsed())
                                .setCell(COLUMN_FAMILY, COLUMN_QUALIFIER_PLACE, person.getPlace());
                dataClient.mutateRow(rowMutation);
            });
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }


    public Row readSingleRow() {
        try {
            System.out.println("\nReading a single row by row key");
            Row row = dataClient.readRow(tableId, ROW_KEY_PREFIX + 0);
            System.out.println("Row: " + row.getKey().toStringUtf8());
            for (RowCell cell : row.getCells()) {
                System.out.printf("Family:"+cell.getFamily()+"    Qualifier: "+cell.getQualifier().toStringUtf8()+"    " +
                        "Value: "+ cell.getValue().toStringUtf8());
            }
            return row;
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
            return null;
        }
    }


    public List<RowCell> readSpecificCells() {
        try {
            System.out.println("\nReading specific cells by family and qualifier");
            Row row = dataClient.readRow(tableId, ROW_KEY_PREFIX + 0);
            System.out.println("Row: " + row.getKey().toStringUtf8());
            List<RowCell> cells = row.getCells(COLUMN_FAMILY, COLUMN_QUALIFIER_FIRSTNAME);
            for (RowCell cell : cells) {
                System.out.printf("Family:"+cell.getFamily()+"    Qualifier: "+cell.getQualifier().toStringUtf8()+"    " +
                        "Value: "+ cell.getValue().toStringUtf8());
            }
            return cells;
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
            return null;
        }
    }

    /** Demonstrates how to read an entire table. */
    public List<Row> readTable() {
        try {
            System.out.println("\nReading the table");
            Query query = Query.create(tableId);
            ServerStream<Row> rowStream = dataClient.readRows(query);
            List<Row> tableRows = new ArrayList<>();
            for (Row r : rowStream) {
                System.out.println("Row Key: " + r.getKey().toStringUtf8());
                tableRows.add(r);
                for (RowCell cell : r.getCells()) {
                    System.out.printf("Family:"+cell.getFamily()+"    Qualifier: "+cell.getQualifier().toStringUtf8()+"    " +
                            "Value: "+ cell.getValue().toStringUtf8());
                }
            }
            return tableRows;
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
            return null;
        }
    }

    /** Demonstrates how to delete a table. */
    public void deleteTable() {
        System.out.println("\nDeleting table: " + tableId);
        try {
            adminClient.deleteTable(tableId);
            System.out.printf("Table"+tableId+ "deleted successfully");
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
