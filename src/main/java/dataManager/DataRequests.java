package dataManager;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

public class DataRequests {
    private final BigTableClient bigTableClient;
    private final BigtableDataClient dataClient;

    public DataRequests() throws IOException {
        this.bigTableClient = new BigTableClient();
        this.dataClient = this.bigTableClient.getDataClient();
    }

    public void Query1(String CustomerFirstName, String CustomerLastName) {
        Filters.Filter filter = FILTERS.chain().filter(FILTERS.family().exactMatch("Customer"))
                .filter(FILTERS.qualifier().exactMatch("firstName")).filter(FILTERS.value().exactMatch(CustomerFirstName))
                .filter(FILTERS.qualifier().exactMatch("lastName")).filter(FILTERS.value().exactMatch(CustomerLastName));
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        for(Row row : rows) {
            this.bigTableClient.printRow(row);
        }
    }

    public String getMostOccurencesInList(ArrayList<String> list) {
        HashMap<String,Integer> occurences = new HashMap<>();
        Integer mostOccurences = 0;
        String mostOccurencesValue = "";
        list.forEach(elt -> {
            if(occurences.get(elt) != null) {
                occurences.put(elt, occurences.get(elt)+1);
            } else {
                occurences.put(elt,1);
            }
        });
        for(Map.Entry<String,Integer> entry : occurences.entrySet()) {
            if(entry.getValue() > mostOccurences) {
                mostOccurencesValue = entry.getKey();
            }
        }
        return mostOccurencesValue;
    }
}
