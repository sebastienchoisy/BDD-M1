package dataManager;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Row;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

public class DataRequests {
    private final BigTableClient bigTableClient;
    private final BigtableDataClient dataClient;

    public DataRequests() throws IOException {
        this.bigTableClient = new BigTableClient();
        this.dataClient = this.bigTableClient.getDataClient();
    }

    public static void main(String[] args) throws Exception {
        DataRequests requests = new DataRequests();
//        requests.query1("Bill","Brown");
        requests.query2("Armasight Spark CORE Multi-Purpose Night Vision Monocular");
    }

    public void query1(String customerFirstName, String customerLastName) {
        Filters.Filter filter = FILTERS.family().exactMatch("Customer");
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        ArrayList<String> boughtBrandsOccurence = new ArrayList<>();
        ArrayList<Row> results = new ArrayList<>();
        String customerId = "";
        for(Row row : rows) {
                if(checkIfRowAsValueForQualifier(row,"Customer","firstName", customerFirstName)
                && checkIfRowAsValueForQualifier(row,"Customer","lastName", customerLastName)) {
                    customerId = getValueForQualifier(row,"Customer","id");
                    this.bigTableClient.printRow(row);
                }
        }
        System.out.println("Orders relative to "+customerFirstName+" "+customerLastName);
        ArrayList<Row> dataRows = getFamilyRowsForSpecificCustomer("Order",customerId,"personId");
        if(dataRows.size() > 0) {
            dataRows.forEach(rowOrder -> {
                this.bigTableClient.printRow(rowOrder);
                boughtBrandsOccurence.addAll(this.getProductsBrandBoughtByOrder(getValueForQualifier(rowOrder,"Order","orderId")));
            });
        } else {
            System.out.println("No order registered for this customer");
        }
        System.out.println("Feedbacks relative to "+customerFirstName+" "+customerLastName);
        dataRows = getFamilyRowsForSpecificCustomer("Feedback",customerId,"personId");
        if(dataRows.size() > 0) {
            dataRows.forEach(rowFeedback -> this.bigTableClient.printRow(rowFeedback));
        } else {
            System.out.println("No feedback registered for this customer");
        }
        System.out.println("Invoices relative to "+customerFirstName+" "+customerLastName);
        dataRows = getFamilyRowsForSpecificCustomer("Invoice",customerId,"personId");
        if(dataRows.size() > 0) {
            dataRows.forEach(rowInvoice -> this.bigTableClient.printRow(rowInvoice));
        } else {
            System.out.println("No invoice registered for this customer");
        }
        System.out.println("Posts posted by "+customerFirstName+" "+customerLastName);
        dataRows = getFamilyRowsForSpecificCustomer("Post",customerId,"authorId");
        if(dataRows.size() > 0) {
            dataRows.forEach(rowPost -> {
                if(this.checkIfDateInRange(getValueForQualifier(rowPost,"Post","creationDate"),"2022-06-01","2022-06-30")) {
                    results.add(rowPost);
                }
            });
        }
        if(results.size() > 0 ) {
            results.forEach(result -> this.bigTableClient.printRow(result));
        } else {
            System.out.println("No post registered for this customer");
        }
        System.out.println("Most Bought Brand/Category for "+customerFirstName+" "+customerLastName);
        System.out.println("**** "+this.getMostOccurencesInList(boughtBrandsOccurence)+" ****");
        System.out.println("Most used tag for "+customerFirstName+" "+customerLastName);
        System.out.println("**** "+this.getMostUsedTagForCustomer("4145")+" ****");
    }

    public void query2(String productName) {
        ArrayList<Row> peopleWhoBoughtIt = new ArrayList<>();
        String customerId;
        ArrayList<Row> peopleWhoPosted = this.getPeopleWhoPostedOnProduct(productName, "2022-06-05","2022-06-25");
        if(peopleWhoPosted.size() > 0) {
            for(Row row : peopleWhoPosted){
                customerId = getValueForQualifier(row,"Customer","id");
                ArrayList<Row> dataRows = getFamilyRowsForSpecificCustomer("Order",customerId,"personId");
                for(Row dataRow : dataRows) {
                    ArrayList<String> productNames = getProductsNameByOrder(getValueForQualifier(dataRow,"Order","orderId"));
                    if(productNames.contains(productName)) {
                        peopleWhoBoughtIt.add(row);
                    }
                }
            }
        }
        if(peopleWhoBoughtIt.size() > 0 ) {
            System.out.println("\nPeople who talked about our product and bought it: " + productName.toUpperCase());
            for(Row row : peopleWhoBoughtIt) {
                this.bigTableClient.printRow(row);
            }
        }
    }

    public ArrayList<Row> getPeopleWhoPostedOnProduct(String name, String fromDate, String toDate) {
        Filters.Filter filter = FILTERS.family().exactMatch("Post");
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        ArrayList<String> authorsId = new ArrayList<>();
        ArrayList<Row> people = new ArrayList<>();
        rows.forEach(row -> {
            if(getValueForQualifier(row,"Post","content").contains(name) && checkIfDateInRange(
                    getValueForQualifier(row,"Post","creationDate"),fromDate,toDate)
            ) {
                authorsId.add(getValueForQualifier(row,"Post","authorId"));
            }
        });
        filter = FILTERS.family().exactMatch("Customer");
        rows = this.bigTableClient.getRowsWithFilter(filter);
        rows.forEach(row -> {
            if(authorsId.contains(getValueForQualifier(row,"Customer","id"))) {
                people.add(row);
            }
        });
        return people;
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

    public boolean checkIfRowAsValueForQualifier(Row row, String family, String qualifier, String value) {
        return row.getCells(family, qualifier).get(0).getValue().toStringUtf8().equals(value);
    }

    public String getValueForQualifier(Row row, String family, String qualifier) {
        return row.getCells(family, qualifier).get(0).getValue().toStringUtf8();
    }

    public boolean checkIfDateInRange(String dateToChecked, String dateFrom, String dateTo) {
        return getDateFromString(dateToChecked.substring(0,10)).after(getDateFromString(dateFrom))
                && getDateFromString(dateToChecked).before(getDateFromString(dateTo));
    }

    public Date getDateFromString(String date) {
        return Date.from(LocalDate.parse(date).atStartOfDay(ZoneId.systemDefault()).toInstant());
    }

    public ArrayList<String> getProductsBrandBoughtByOrder(String id) {
        ArrayList<String> productsRow = new ArrayList<>();
        Filters.Filter filter = FILTERS.family().exactMatch("Order_orderlines");
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        rows.forEach(row -> {
            if(getValueForQualifier(row,"Order_orderlines","orderId").equals(id)){
                productsRow.add(getValueForQualifier(row,"Order_orderlines","brand"));
            }
        });
        return productsRow;
    }

    public ArrayList<String> getProductsNameByOrder(String id) {
        ArrayList<String> productsRow = new ArrayList<>();
        Filters.Filter filter = FILTERS.family().exactMatch("Order_orderlines");
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        rows.forEach(row -> {
            if(getValueForQualifier(row,"Order_orderlines","orderId").equals(id)){
                productsRow.add(getValueForQualifier(row,"Order_orderlines","title"));
            }
        });
        return productsRow;
    }

    public ArrayList<Row> getFamilyRowsForSpecificCustomer(String family, String id, String qualifier) {
        Filters.Filter filter = FILTERS.family().exactMatch(family);
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        ArrayList<Row> FilteredRows = new ArrayList<>();
        for(Row row : rows) {
            if(checkIfRowAsValueForQualifier(row,family,qualifier,id)) {
                FilteredRows.add(row);
            }
        }
        return FilteredRows;
    }

    public String getMostUsedTagForCustomer(String id) {
        ArrayList<String> tags = new ArrayList<>();
        Filters.Filter filter = FILTERS.family().exactMatch("Person_tags");
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        rows.forEach(row -> {
            if(getValueForQualifier(row,"Person_tags","personId").equals(id)){
                tags.add(getValueForQualifier(row,"Person_tags","tagId"));
            }
        });
        return this.getMostOccurencesInList(tags);
    }
}
