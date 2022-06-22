package dataManager;

import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.ColumnFamily;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.*;
import lombok.Data;

import java.io.IOException;
import java.util.List;
import java.util.Objects;


@Data
public class BigTableClient {
    private final BigtableDataClient dataClient;
    private final BigtableTableAdminClient adminClient;
    private final String projectId = "projetbddm1";
    private final String instanceId = "projetbddm1";
    private final String tableId = "data";

    public BigTableClient() throws IOException {
        // Initialisation des clients BigTable avec les différentes ID's pour les différentes opérations
        BigtableDataSettings settings =
                BigtableDataSettings.newBuilder().setProjectId(this.projectId).setInstanceId(this.instanceId).build();
        this.dataClient = BigtableDataClient.create(settings);
        BigtableTableAdminSettings adminSettings =
                BigtableTableAdminSettings.newBuilder()
                        .setProjectId(this.projectId)
                        .setInstanceId(this.instanceId)
                        .build();
        this.adminClient = BigtableTableAdminClient.create(adminSettings);
    }

    // Création de la table si elle n'existe pas
    public void createTableIfDoesntExist() {
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

    // Renvoit true si la column family existe deja
    public boolean isColumnFamilyExisting(String columnFamilyName) {
        boolean doesExist = false;
        List<ColumnFamily> columnFamilies = this.adminClient.getTable(this.tableId).getColumnFamilies();
        for(int i = 0; i < columnFamilies.size(); i++) {
            if(Objects.equals(columnFamilies.get(i).getId(), columnFamilyName)) {
                doesExist = true;
            }
        }
        return doesExist;
    }

    // Ajout d'une column family
    public void addColumnFamily(String columnFamilyName) {
        ModifyColumnFamiliesRequest familiesRequest = ModifyColumnFamiliesRequest
                .of(this.tableId).addFamily(columnFamilyName);
        this.adminClient.modifyFamilies(familiesRequest);
    }

    // Suppression de la table
    public void deleteTable() {
        System.out.println("\nSuppression de la table "+this.tableId);
        try {
            adminClient.deleteTable(this.tableId);
            System.out.printf("Table "+this.tableId+" supprimée avec succès");
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    // Récuperation des différentes lignes selon le filtre donné en parametre
    public ServerStream getRowsWithFilter(Filters.Filter filter) {
            Query query = Query.create(this.tableId).filter(filter);
            ServerStream<Row> rows = this.dataClient.readRows(query);
            return rows;
    }

    // Affichage d'une ligne
    public void printRow(Row row) {
        System.out.printf("Reading data for %s%n", row.getKey().toStringUtf8());
        String colFamily = "";
        for (RowCell cell : row.getCells()) {
            if (!cell.getFamily().equals(colFamily)) {
                colFamily = cell.getFamily();
                System.out.printf("Column Family %s%n", colFamily);
            }
            System.out.printf(
                    "\t%s: %s %n",
                    cell.getQualifier().toStringUtf8(),
                    cell.getValue().toStringUtf8());
        }
        System.out.println();
    }

    // Update d'une cellule d'une ligne spécifique en donnant la column family, la clé de ligne, le qualifier et la nouvelle valuer
    public void updateDataRow(String columnFamilyName, String rowkey, String qualifier, String value) {
        try {
            RowMutation rowMutation =
                    RowMutation.create(this.tableId, rowkey).setCell(columnFamilyName, qualifier, value);
            this.dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    // Suppresion d'une cellule specifique
    public void deleteSpecificCell(String rowkey, String ColumnFamilyName, String qualifier) {
        try {
            RowMutation rowMutation =
                    RowMutation.create(this.tableId, rowkey).deleteCells(ColumnFamilyName,qualifier);
            this.dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
