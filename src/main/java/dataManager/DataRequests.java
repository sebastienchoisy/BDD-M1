package dataManager;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Row;
import org.checkerframework.checker.units.qual.A;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

public class DataRequests {
    private final BigTableClient bigTableClient;
    private final BigtableDataClient dataClient;
    private final String CUSTOMER_COLUMN_FAMILY = "Customer";
    private final String FEEDBACK_COLUMN_FAMILY = "Feedback";
    private final String INVOICE_COLUMN_FAMILY = "Invoice";
    private final String INVOICE_ORDERLINE_COLUMN_FAMILY = "Invoice_orderlines";
    private final String ORDER_COLUMN_FAMILY = "Order";
    private final String ORDER_ORDERLINE_COLUMN_FAMILY = "Order_orderlines";
    private final String PRODUCT_COLUMN_FAMILY = "Product";
    private final String POST_COLUMN_FAMILY = "Post";
    private final String KNOWN_PERSON_COLUMN_FAMILY = "Person_Known";
    private final String PERSON_TAGS_COLUMN_FAMILY = "Person_tags";
    private final String POST_TAGS_COLUMN_FAMILY = "Post_tags";

    public DataRequests() throws IOException {
        this.bigTableClient = new BigTableClient();
        this.dataClient = this.bigTableClient.getDataClient();
    }

    public static void main(String[] args) throws Exception {
        DataRequests requests = new DataRequests();
//        requests.query1("Bill","Brown");
//        requests.query2("Armasight Spark CORE Multi-Purpose Night Vision Monocular");
//        requests.query3("B007SYGLZO","2000-06-06","2030-06-25");
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

    public void query3(String asin, String fromDate, String toDate) {
        String productName = getNameforProduct(asin);
        ArrayList<Row> peopleWhoPosted = getPeopleWhoPostedOnProduct(productName,fromDate,toDate);
        ArrayList<String> negativeFeedbacks;
        for(Row row : peopleWhoPosted) {
            ArrayList<Row> feedBacks = getFeedbacksForSpecificCustomerAndSpecificProduct(getValueForQualifier(row,CUSTOMER_COLUMN_FAMILY,"id"),asin);
            if(feedBacks.size() > 0) {
                String firstName = getValueForQualifier(row,CUSTOMER_COLUMN_FAMILY,"firstName");
                String lastName = getValueForQualifier(row,CUSTOMER_COLUMN_FAMILY,"lastName");
                System.out.println(firstName+" "+lastName+" reviewed this product");
                negativeFeedbacks = getNegativeFeedbacks(feedBacks);
                if(negativeFeedbacks.size() > 0) {
                    negativeFeedbacks.forEach(feedback -> System.out.println(feedback));
                }

            }
        }
    }

    public ArrayList<Row> getPeopleWhoPostedOnProduct(String name, String fromDate, String toDate) {
        Filters.Filter filter = FILTERS.family().exactMatch(POST_COLUMN_FAMILY);
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        ArrayList<String> authorsId = new ArrayList<>();
        ArrayList<Row> people = new ArrayList<>();
        rows.forEach(row -> {
            if(getValueForQualifier(row,POST_COLUMN_FAMILY,"content").contains(name) && checkIfDateInRange(
                    getValueForQualifier(row,POST_COLUMN_FAMILY,"creationDate"),fromDate,toDate)
            ) {
                authorsId.add(getValueForQualifier(row,POST_COLUMN_FAMILY,"authorId"));
            }
        });
        filter = FILTERS.family().exactMatch(CUSTOMER_COLUMN_FAMILY);
        rows = this.bigTableClient.getRowsWithFilter(filter);
        rows.forEach(row -> {
            if(authorsId.contains(getValueForQualifier(row,CUSTOMER_COLUMN_FAMILY,"id"))) {
                people.add(row);
            }
        });
        return people;
    }

    public ArrayList<Row> getFeedbacksForSpecificCustomerAndSpecificProduct(String customerId, String productAsin) {
        ArrayList<Row> results = new ArrayList<>();
        Filters.Filter filter = FILTERS.family().exactMatch(FEEDBACK_COLUMN_FAMILY);
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        for(Row row : rows){
            if(getValueForQualifier(row,FEEDBACK_COLUMN_FAMILY,"personId").equals(customerId)
            && getValueForQualifier(row,FEEDBACK_COLUMN_FAMILY,"asin").equals(productAsin)) {
                results.add(row);
            }
        }
        return results;
    }
    public ArrayList<String> getNegativeFeedbacks(ArrayList<Row> feedBacks) {
        ArrayList<String> negativeFeedbacks = new ArrayList<>();
        String feedback;
        for(Row row: feedBacks) {
            feedback = getValueForQualifier(row,FEEDBACK_COLUMN_FAMILY,"comment");
            if(Integer.parseInt(feedback.substring(1,2)) < 2) {
                negativeFeedbacks.add(feedback);
            }
        }
        return negativeFeedbacks;
    }

    public String getNameforProduct(String asin) {
        String name = "";
        Filters.Filter filter = FILTERS.family().exactMatch(PRODUCT_COLUMN_FAMILY);
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        for(Row row : rows) {
            if(getValueForQualifier(row,PRODUCT_COLUMN_FAMILY,"asin").equals(asin)){
                name = getValueForQualifier(row,PRODUCT_COLUMN_FAMILY,"title");
            }
        }
        return name;
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
                && getDateFromString(dateToChecked.substring(0,10)).before(getDateFromString(dateTo));
    }

    public Date getDateFromString(String date) {
        return Date.from(LocalDate.parse(date).atStartOfDay(ZoneId.systemDefault()).toInstant());
    }

    public ArrayList<String> getProductsBrandBoughtByOrder(String id) {
        ArrayList<String> productsRow = new ArrayList<>();
        Filters.Filter filter = FILTERS.family().exactMatch(ORDER_ORDERLINE_COLUMN_FAMILY);
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        rows.forEach(row -> {
            if(getValueForQualifier(row,ORDER_ORDERLINE_COLUMN_FAMILY,"orderId").equals(id)){
                productsRow.add(getValueForQualifier(row,ORDER_ORDERLINE_COLUMN_FAMILY,"brand"));
            }
        });
        return productsRow;
    }

    public ArrayList<String> getProductsNameByOrder(String id) {
        ArrayList<String> productsRow = new ArrayList<>();
        Filters.Filter filter = FILTERS.family().exactMatch(ORDER_ORDERLINE_COLUMN_FAMILY);
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        rows.forEach(row -> {
            if(getValueForQualifier(row,ORDER_ORDERLINE_COLUMN_FAMILY,"orderId").equals(id)){
                productsRow.add(getValueForQualifier(row,ORDER_ORDERLINE_COLUMN_FAMILY,"title"));
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
        Filters.Filter filter = FILTERS.family().exactMatch(PERSON_TAGS_COLUMN_FAMILY);
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        rows.forEach(row -> {
            if(getValueForQualifier(row,PERSON_TAGS_COLUMN_FAMILY,"personId").equals(id)){
                tags.add(getValueForQualifier(row,PERSON_TAGS_COLUMN_FAMILY,"tagId"));
            }
        });
        return this.getMostOccurencesInList(tags);
    }
}
