package dataManager;
//https://console.cloud.google.com/bigtable/instances?authuser=3&project=projetbddm1
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import lombok.Data;

import java.io.IOException;

@Data
public class BigTableClient {
    private final BigtableDataClient dataClient;
    private final BigtableTableAdminClient adminClient;
    private final String projectId = "projetbddm1";
    private final String instanceId = "projetbddm1";
    private final String tableId = "data";

    public BigTableClient() throws IOException {
        BigtableDataSettings settings =
                BigtableDataSettings.newBuilder().setProjectId(this.projectId).setInstanceId(this.instanceId).build();
        this.dataClient = BigtableDataClient.create(settings);
        BigtableTableAdminSettings adminSettings =
                BigtableTableAdminSettings.newBuilder()
                        .setProjectId(this.projectId)
                        .setInstanceId(this.instanceId)
                        .build();
        this.adminClient = BigtableTableAdminClient.create(adminSettings);
        this.createTableIfDoesntExist();
    }

    private void createTableIfDoesntExist() {
        if(!this.adminClient.exists(this.tableId)) {
            System.out.println("La table "+ this.tableId + " n'existe pas ! -> Création de la table");
            CreateTableRequest createTableRequest =
                    CreateTableRequest.of(this.tableId);
            this.adminClient.createTable(createTableRequest);
            System.out.println("Table "+ this.tableId+ " creee avec succès !" );
        } else {
            System.out.println("Table déjà existante !");
        }
    }

    public boolean isColumnFamilyExisting(String columnFamilyName) {
        return this.adminClient.getTable(this.tableId).getColumnFamilies().contains(columnFamilyName);
    }

    public void addColumnFamily(String columnFamilyName) {
        ModifyColumnFamiliesRequest familiesRequest = ModifyColumnFamiliesRequest
                .of(this.tableId).addFamily(columnFamilyName);
        this.adminClient.modifyFamilies(familiesRequest);
    }

    public void deleteTable() {
        System.out.println("\nSuppression de la table "+this.tableId);
        try {
            adminClient.deleteTable(this.tableId);
            System.out.printf("Table "+this.tableId+" supprimée avec succès");
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public ServerStream getRowWithFilter(Filters.Filter filter) {
            Query query = Query.create(tableId);
            ServerStream<Row> rows = this.dataClient.readRows(query);
            return rows;
    }

    private static void printRow(Row row) {
        System.out.printf("Reading data for %s%n", row.getKey().toStringUtf8());
        String colFamily = "";
        for (RowCell cell : row.getCells()) {
            if (!cell.getFamily().equals(colFamily)) {
                colFamily = cell.getFamily();
                System.out.printf("Column Family %s%n", colFamily);
            }
            String labels =
                    cell.getLabels().size() == 0 ? "" : " [" + String.join(",", cell.getLabels()) + "]";
            System.out.printf(
                    "\t%s: %s @%s%s%n",
                    cell.getQualifier().toStringUtf8(),
                    cell.getValue().toStringUtf8(),
                    cell.getTimestamp(),
                    labels);
        }
        System.out.println();
    }
}
