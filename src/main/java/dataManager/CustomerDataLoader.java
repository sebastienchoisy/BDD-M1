package dataManager;


import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import filereader.CsvFileReader;
import lombok.Getter;
import lombok.Setter;
import person.Person;

import java.io.IOException;
import java.util.ArrayList;

public class CustomerDataLoader {
    private static final String COLUMN_FAMILY = "Customer";
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
    private static final String tableId = "data";
    private final BigtableDataClient dataClient;
    @Setter
    @Getter
    private BigTableClient bigTableClient;
    private ArrayList<Person> persons;

    public CustomerDataLoader() throws IOException {
        this.bigTableClient = new BigTableClient();
        this.dataClient = this.bigTableClient.getDataClient();
        CsvFileReader reader = new CsvFileReader();
        this.persons = reader.getPersonsDataListFromCsvFile();
    }

    public void writeToTable() {
        try {
            if(!this.bigTableClient.isColumnFamilyExisting(COLUMN_FAMILY)) {
                this.bigTableClient.addColumnFamily(COLUMN_FAMILY);
            }
            this.persons.forEach((person) -> this.addDataRow(person));
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void addDataRow(Person person) {
        try {
            RowMutation rowMutation =
                    RowMutation.create(this.tableId, this.ROW_KEY_PREFIX + person.getId())
                            .setCell(this.COLUMN_FAMILY, this.COLUMN_QUALIFIER_ID, person.getId())
                            .setCell(this.COLUMN_FAMILY, this.COLUMN_QUALIFIER_FIRSTNAME, person.getFirstName())
                            .setCell(this.COLUMN_FAMILY, this.COLUMN_QUALIFIER_LASTNAME, person.getLastName())
                            .setCell(this.COLUMN_FAMILY, this.COLUMN_QUALIFIER_GENDER, person.getGender())
                            .setCell(this.COLUMN_FAMILY, this.COLUMN_QUALIFIER_BIRTHDAY, person.getBirthday())
                            .setCell(this.COLUMN_FAMILY, this.COLUMN_QUALIFIER_CREATION_DATE, person.getCreationDate())
                            .setCell(this.COLUMN_FAMILY, this.COLUMN_QUALIFIER_LOCATION_IP, person.getLocationIP())
                            .setCell(this.COLUMN_FAMILY, this.COLUMN_QUALIFIER_BROWSER_USED, person.getBrowserUsed())
                            .setCell(this.COLUMN_FAMILY, this.COLUMN_QUALIFIER_PLACE, person.getPlace());
            this.dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void updateDataRow(String rowkey, String qualifier, String value) {
        try {
            RowMutation rowMutation =
                    RowMutation.create(this.tableId, rowkey).setCell(this.COLUMN_FAMILY, qualifier, value);
            this.dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void deleteDataRow(String rowkey) {
        try {
            RowMutation rowMutation =
                    RowMutation.create(this.tableId, rowkey).deleteRow();
            this.dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
