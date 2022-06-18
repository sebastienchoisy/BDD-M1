package dataManager;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import feedback.FeedBack;
import filereader.CsvFileReader;
import filereader.JsonFileReader;
import filereader.XmlFileReader;
import lombok.Getter;
import order.Invoice;
import order.Order;
import order.OrderLine;
import person.Person;
import product.Product;
import socialNetwork.PersonLink;
import socialNetwork.Post;
import socialNetwork.TagsByPerson;
import socialNetwork.TagsByPost;

import java.io.IOException;
import java.util.ArrayList;

public class DataLoader {

    private final String ROW_KEY_PREFIX = "rowKey";
    private final String tableId = "data";
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
    private final BigtableDataClient dataClient;
    @Getter
    private final BigTableClient bigTableClient;
    private final CsvFileReader csvReader;
    private final XmlFileReader xmlReader;
    private final JsonFileReader jsonFileReader;

    public DataLoader() throws IOException {
        this.bigTableClient = new BigTableClient();
        this.dataClient = this.bigTableClient.getDataClient();
        this.csvReader = new CsvFileReader();
        this.xmlReader = new XmlFileReader();
        this.jsonFileReader = new JsonFileReader();
    }

    public static void main(String[] args) throws IOException {
        DataLoader loader = new DataLoader();
//        loader.writePersonsToTable();
//        loader.writeFeedBacksToTable();
//        loader.writeInvoicesToTable();
        loader.writeOrdersToTable();
        loader.writePostsToTable();
        loader.writeKnownPersonToTable();
        loader.writeProductsToTable();
        loader.writePersonTagsToTable();
        loader.writePostTagsToTable();
    }

    public void writePersonsToTable() throws IOException {
        System.out.println("Début chargement de la data Person");
        ArrayList<Person> persons = this.csvReader.getPersonsDataListFromCsvFile();
        try {
            if(!this.bigTableClient.isColumnFamilyExisting(this.CUSTOMER_COLUMN_FAMILY)) {
                this.bigTableClient.addColumnFamily(this.CUSTOMER_COLUMN_FAMILY);
            }
            for(int i = 0; i < persons.size(); i++) {
                System.out.println((i+1)+"/"+persons.size());
                this.addPersonDataRow(persons.get(i),i);
            }
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void writeFeedBacksToTable() throws IOException {
        System.out.println("Début chargement de la data Feedback");
        ArrayList<FeedBack> feedBacks = this.csvReader.getFeedBacksDataListFromCsvFile();
        try {
            if(!this.bigTableClient.isColumnFamilyExisting(this.FEEDBACK_COLUMN_FAMILY)) {
                this.bigTableClient.addColumnFamily(this.FEEDBACK_COLUMN_FAMILY);
            }
            for(int i = 0; i < feedBacks.size(); i++) {
                this.addFeedBackDataRow(feedBacks.get(i),i);
                System.out.println((i+1)+"/"+ feedBacks.size());
            }
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void writeProductsToTable() throws IOException {
        System.out.println("Début chargement de la data Product");
        ArrayList<Product> products = this.csvReader.getProductsFromCsvFile();
        try {
            if(!this.bigTableClient.isColumnFamilyExisting(this.PRODUCT_COLUMN_FAMILY)) {
                this.bigTableClient.addColumnFamily(this.PRODUCT_COLUMN_FAMILY);
            }
            for(int i = 0; i < products.size(); i++) {
                System.out.println((i+1)+"/"+ products.size());
                this.addProductDataRow(products.get(i),i);
            }
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void writeKnownPersonToTable() throws IOException {
        System.out.println("Début chargement de la data KnownPerson");
        ArrayList<PersonLink> knownPersons = this.csvReader.getLinkByPersonFromCsvFile();
        try {
            if(!this.bigTableClient.isColumnFamilyExisting(this.KNOWN_PERSON_COLUMN_FAMILY)) {
                this.bigTableClient.addColumnFamily(this.KNOWN_PERSON_COLUMN_FAMILY);
            }
            for(int i = 0; i < knownPersons.size(); i++) {
                System.out.println((i+1)+"/"+knownPersons.size());
                this.addKnownPersonDataRow(knownPersons.get(i),i);
            }
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void writePersonTagsToTable() throws IOException {
        System.out.println("Début chargement de la data PersonTags");
        ArrayList<TagsByPerson> PersonTags = this.csvReader.getTagsByPersonFromCsvFile();
        try {
            if(!this.bigTableClient.isColumnFamilyExisting(this.PERSON_TAGS_COLUMN_FAMILY)) {
                this.bigTableClient.addColumnFamily(this.PERSON_TAGS_COLUMN_FAMILY);
            }
            for(int i = 0; i < PersonTags.size(); i++) {
                System.out.println((i+1)+"/"+ PersonTags.size());
                this.addPersonTagsDataRow(PersonTags.get(i),i);
            }
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void writePostTagsToTable() throws IOException {
        System.out.println("Début chargement de la data PostTags");
        ArrayList<TagsByPost> postTags = this.csvReader.getTagsByPostFromCsvFile();
        try {
            if(!this.bigTableClient.isColumnFamilyExisting(this.POST_TAGS_COLUMN_FAMILY)) {
                this.bigTableClient.addColumnFamily(this.POST_TAGS_COLUMN_FAMILY);
            }
            for(int i = 0; i < postTags.size(); i++) {
                System.out.println((i+1)+"/"+ postTags.size());
                this.addPostTagsDataRow(postTags.get(i),i);
            }
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void writeInvoicesToTable() throws  IOException {
        ArrayList<Invoice> invoices = this.xmlReader.getInvoicesFromXml();
        ArrayList<OrderLine> orderLines = this.xmlReader.getOrderLines();
        try {
            // INVOICES
            if(!this.bigTableClient.isColumnFamilyExisting(this.INVOICE_COLUMN_FAMILY)) {
                this.bigTableClient.addColumnFamily(this.INVOICE_COLUMN_FAMILY);
            }
            System.out.println("Début chargement de la data Invoices");
            for(int i = 0; i < invoices.size(); i++) {
                System.out.println((i+1)+"/"+ invoices.size());
                this.addInvoiceDataRow(invoices.get(i),i);
            }
            // INVOICE ORDERLINES
            if(!this.bigTableClient.isColumnFamilyExisting(this.INVOICE_ORDERLINE_COLUMN_FAMILY)) {
                this.bigTableClient.addColumnFamily(this.INVOICE_ORDERLINE_COLUMN_FAMILY);
            }
            System.out.println("Début chargement de la data Orderlines Invoices");
            for(int i = 0; i < orderLines.size(); i++) {
                System.out.println((i+1)+"/"+ orderLines.size());
                this.addOrderLineInvoiceDataRow(orderLines.get(i),i);
            }
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void writeOrdersToTable() throws IOException {
        ArrayList<Order> orders = this.jsonFileReader.getOrdersFromJson();
        int productIndex = 0;
        ArrayList<Product> orderlineProducts;
        try {
            if(!this.bigTableClient.isColumnFamilyExisting(this.ORDER_COLUMN_FAMILY)) {
                this.bigTableClient.addColumnFamily(this.ORDER_COLUMN_FAMILY);
            }
            if(!this.bigTableClient.isColumnFamilyExisting(this.ORDER_ORDERLINE_COLUMN_FAMILY)) {
                this.bigTableClient.addColumnFamily(this.ORDER_ORDERLINE_COLUMN_FAMILY);
            }
            System.out.println("Début chargement de la data Orders");
            for(int i = 0; i < orders.size(); i++) {
                System.out.println((i+1)+"/"+ orders.size());
                this.addOrderDataRow(orders.get(i),i);
                orderlineProducts = orders.get(i).getOrderline();
                for(Product product : orderlineProducts) {
                    this.addOrderLineOrderDataRow(product,productIndex);
                    productIndex++;
                }
            }
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void writePostsToTable() throws IOException {
        ArrayList<Post> posts = this.csvReader.getPostsFromCsvFile();
        try {
            if(!this.bigTableClient.isColumnFamilyExisting(this.POST_COLUMN_FAMILY)) {
                this.bigTableClient.addColumnFamily(this.POST_COLUMN_FAMILY);
            }
            System.out.println("Début chargement de la data Posts");
            for(int i = 0; i < posts.size(); i++) {
                System.out.println((i+1)+"/"+ posts.size());
                this.addPostDataRow(posts.get(i),i);
            }
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void addPersonDataRow(Person person, Integer rowkeyIndex) {
        try {
            RowMutation rowMutation =
                    RowMutation.create(this.tableId, this.ROW_KEY_PREFIX + rowkeyIndex)
                            .setCell(this.CUSTOMER_COLUMN_FAMILY, "id", person.getId())
                            .setCell(this.CUSTOMER_COLUMN_FAMILY, "firstName", person.getFirstName())
                            .setCell(this.CUSTOMER_COLUMN_FAMILY, "lastName", person.getLastName())
                            .setCell(this.CUSTOMER_COLUMN_FAMILY, "gender", person.getGender())
                            .setCell(this.CUSTOMER_COLUMN_FAMILY, "birthday", person.getBirthday())
                            .setCell(this.CUSTOMER_COLUMN_FAMILY, "creationDate", person.getCreationDate())
                            .setCell(this.CUSTOMER_COLUMN_FAMILY, "locationIP", person.getLocationIP())
                            .setCell(this.CUSTOMER_COLUMN_FAMILY, "browserUsed", person.getBrowserUsed())
                            .setCell(this.CUSTOMER_COLUMN_FAMILY, "place", person.getPlace());
            this.dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void addFeedBackDataRow(FeedBack feedback, Integer rowkeyIndex) {
        try {
            RowMutation rowMutation =
                    RowMutation.create(this.tableId, this.ROW_KEY_PREFIX + rowkeyIndex)
                            .setCell(this.FEEDBACK_COLUMN_FAMILY, "asin", feedback.getAsin())
                            .setCell(this.FEEDBACK_COLUMN_FAMILY, "personID", feedback.getPersonId())
                            .setCell(this.FEEDBACK_COLUMN_FAMILY, "comment", feedback.getComment());
            this.dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void addInvoiceDataRow(Invoice invoice, Integer rowkeyIndex) {
        try {
            RowMutation rowMutation =
                    RowMutation.create(this.tableId, this.ROW_KEY_PREFIX + rowkeyIndex)
                            .setCell(this.INVOICE_COLUMN_FAMILY, "orderID", invoice.getOrderId())
                            .setCell(this.INVOICE_COLUMN_FAMILY, "personID", invoice.getPersonId())
                            .setCell(this.INVOICE_COLUMN_FAMILY, "orderDate", invoice.getOrderDate())
                            .setCell(this.INVOICE_COLUMN_FAMILY, "totalPrice", invoice.getTotalPrice());
            this.dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void addOrderLineInvoiceDataRow(OrderLine orderLine, Integer rowkeyIndex) {
        try {
            RowMutation rowMutation =
                    RowMutation.create(this.tableId, this.ROW_KEY_PREFIX + rowkeyIndex)
                            .setCell(this.INVOICE_ORDERLINE_COLUMN_FAMILY, "productId", orderLine.getOrderId())
                            .setCell(this.INVOICE_ORDERLINE_COLUMN_FAMILY, "asin", orderLine.getAsin())
                            .setCell(this.INVOICE_ORDERLINE_COLUMN_FAMILY, "title", orderLine.getTitle())
                            .setCell(this.INVOICE_ORDERLINE_COLUMN_FAMILY, "price", orderLine.getPrice())
                            .setCell(this.INVOICE_ORDERLINE_COLUMN_FAMILY, "brand", orderLine.getBrand())
                            .setCell(this.INVOICE_ORDERLINE_COLUMN_FAMILY, "orderId", orderLine.getOrderId());
            this.dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void addOrderDataRow(Order order, Integer rowkeyIndex) {
        try {
            RowMutation rowMutation =
                    RowMutation.create(this.tableId, this.ROW_KEY_PREFIX + rowkeyIndex)
                            .setCell(this.ORDER_COLUMN_FAMILY, "orderId", order.getOrderId())
                            .setCell(this.ORDER_COLUMN_FAMILY, "personId", order.getPersonId())
                            .setCell(this.ORDER_COLUMN_FAMILY, "orderDate", order.getOrderDate())
                            .setCell(this.ORDER_COLUMN_FAMILY, "totalPrice", order.getTotalPrice());
            this.dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void addOrderLineOrderDataRow(Product product, Integer rowkeyIndex) {
        try {
            RowMutation rowMutation =
                    RowMutation.create(this.tableId, this.ROW_KEY_PREFIX + rowkeyIndex)
                            .setCell(this.ORDER_ORDERLINE_COLUMN_FAMILY, "productId", product.getProductId())
                            .setCell(this.ORDER_ORDERLINE_COLUMN_FAMILY, "asin", product.getAsin())
                            .setCell(this.ORDER_ORDERLINE_COLUMN_FAMILY, "title", product.getTitle())
                            .setCell(this.ORDER_ORDERLINE_COLUMN_FAMILY, "price", product.getPrice())
                            .setCell(this.ORDER_ORDERLINE_COLUMN_FAMILY, "imgUrl", product.getImgUrl())
                            .setCell(this.ORDER_ORDERLINE_COLUMN_FAMILY, "brand", product.getBrand())
                            .setCell(this.ORDER_ORDERLINE_COLUMN_FAMILY, "orderId", product.getOrderId());
            this.dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void addProductDataRow(Product product, Integer rowkeyIndex) {
        try {
            RowMutation rowMutation =
                    RowMutation.create(this.tableId, this.ROW_KEY_PREFIX + rowkeyIndex)
                            .setCell(this.ORDER_ORDERLINE_COLUMN_FAMILY, "productId", product.getProductId())
                            .setCell(this.ORDER_ORDERLINE_COLUMN_FAMILY, "asin", product.getAsin())
                            .setCell(this.ORDER_ORDERLINE_COLUMN_FAMILY, "title", product.getTitle())
                            .setCell(this.ORDER_ORDERLINE_COLUMN_FAMILY, "price", product.getPrice())
                            .setCell(this.ORDER_ORDERLINE_COLUMN_FAMILY, "imgUrl", product.getImgUrl())
                            .setCell(this.ORDER_ORDERLINE_COLUMN_FAMILY, "brand", product.getBrand());
            this.dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void addPostDataRow(Post post, Integer rowkeyIndex) {
        try {
            RowMutation rowMutation =
                    RowMutation.create(this.tableId, this.ROW_KEY_PREFIX + rowkeyIndex)
                            .setCell(this.POST_COLUMN_FAMILY, "id", post.getId())
                            .setCell(this.POST_COLUMN_FAMILY, "imageFile", post.getImageFile())
                            .setCell(this.POST_COLUMN_FAMILY, "creationDate", post.getCreationDate())
                            .setCell(this.POST_COLUMN_FAMILY, "locationIP", post.getLocationIP())
                            .setCell(this.POST_COLUMN_FAMILY, "browserUsed", post.getBrowserUsed())
                            .setCell(this.POST_COLUMN_FAMILY, "language", post.getLanguage())
                            .setCell(this.POST_COLUMN_FAMILY, "content", post.getContent())
                            .setCell(this.POST_COLUMN_FAMILY, "length", post.getLength())
                            .setCell(this.POST_COLUMN_FAMILY, "authorId", post.getAuthorId());
            this.dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void addKnownPersonDataRow(PersonLink knowPerson, Integer rowkeyIndex) {
        try {
            RowMutation rowMutation =
                    RowMutation.create(this.tableId, this.ROW_KEY_PREFIX + rowkeyIndex)
                            .setCell(this.KNOWN_PERSON_COLUMN_FAMILY, "personId",knowPerson.getPersonId())
                            .setCell(this.KNOWN_PERSON_COLUMN_FAMILY, "friendId", knowPerson.getFriendId())
                            .setCell(this.KNOWN_PERSON_COLUMN_FAMILY, "creationDate", knowPerson.getCreationDate());
            this.dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void addPersonTagsDataRow(TagsByPerson personTags, Integer rowkeyIndex) {
        try {
            RowMutation rowMutation =
                    RowMutation.create(this.tableId, this.ROW_KEY_PREFIX + rowkeyIndex)
                            .setCell(this.PERSON_TAGS_COLUMN_FAMILY, "personId", personTags.getPersonId())
                            .setCell(this.PERSON_TAGS_COLUMN_FAMILY, "tagId", personTags.getTagId());
            this.dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    public void addPostTagsDataRow(TagsByPost postTags, Integer rowkeyIndex) {
        try {
            RowMutation rowMutation =
                    RowMutation.create(this.tableId, this.ROW_KEY_PREFIX + rowkeyIndex)
                            .setCell(this.POST_TAGS_COLUMN_FAMILY, "postId", postTags.getPostId())
                            .setCell(this.POST_TAGS_COLUMN_FAMILY, "tagId", postTags.getTagId());
            this.dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
