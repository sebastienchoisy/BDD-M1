package dataManager;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Row;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

public class DataRequests {
    private final BigTableClient bigTableClient;
    private final String CUSTOMER_COLUMN_FAMILY = "Customer";
    private final String FEEDBACK_COLUMN_FAMILY = "Feedback";
    private final String ORDER_COLUMN_FAMILY = "Order";
    private final String ORDER_ORDERLINE_COLUMN_FAMILY = "Order_orderlines";
    private final String PRODUCT_COLUMN_FAMILY = "Product";
    private final String POST_COLUMN_FAMILY = "Post";
    private final String PERSON_TAGS_COLUMN_FAMILY = "Person_tags";


    public DataRequests() throws IOException {
        this.bigTableClient = new BigTableClient();
    }

    public static void main(String[] args) throws Exception {
        DataRequests requests = new DataRequests();
        requests.query1("Bill","Brown");
        requests.query2("Armasight Spark CORE Multi-Purpose Night Vision Monocular","2022-06-05","2022-06-25");
        requests.query3("B007SYGLZO","2000-06-06","2030-06-25");
        requests.query4();
        requests.query5("4445","Elfin_Sports_Cars");
          requests.query7("TRYMAX");
    }

    /*  Query 1.
        For a given customer, find his/her all related data including profile, orders, invoices,
        feedback, comments, and posts in the last month, return the category in which he/she has
        bought the largest number of products, and return the tag which he/she has engaged the
        greatest times in the posts
    */

    public void query1(String customerFirstName, String customerLastName) {
        Filters.Filter filter = FILTERS.family().exactMatch("Customer");
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        ArrayList<String> boughtBrandsOccurence = new ArrayList<>();
        ArrayList<Row> results = new ArrayList<>();
        String customerId = "";
        // Recuperation du profil en cherchant une ligne avec le nom et le prenom du customer
        for(Row row : rows) {
                if(checkIfRowAsValueForQualifier(row,"Customer","firstName", customerFirstName)
                && checkIfRowAsValueForQualifier(row,"Customer","lastName", customerLastName)) {
                    customerId = getValueForQualifier(row,"Customer","id");
                    this.bigTableClient.printRow(row);
                }
        }
        // Recuperation des commandes relatives à l'id du client récupéré plus haut
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
        // Recuperation des feedbacks relatives à l'id du client
        System.out.println("Feedbacks relative to "+customerFirstName+" "+customerLastName);
        dataRows = getFamilyRowsForSpecificCustomer("Feedback",customerId,"personId");
        if(dataRows.size() > 0) {
            dataRows.forEach(this.bigTableClient::printRow);
        } else {
            System.out.println("No feedback registered for this customer");
        }
        // Recuperation des invoices relatives à l'id du client
        System.out.println("Invoices relative to "+customerFirstName+" "+customerLastName);
        dataRows = getFamilyRowsForSpecificCustomer("Invoice",customerId,"personId");
        if(dataRows.size() > 0) {
            dataRows.forEach(this.bigTableClient::printRow);
        } else {
            System.out.println("No invoice registered for this customer");
        }
        // Recuperation des posts relatives à l'id du client
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
            results.forEach(this.bigTableClient::printRow);
        } else {
            System.out.println("No post registered for this customer");
        }
        // Affichage de la marque la plus achetée par le client
        System.out.println("Most Bought Brand/Category for "+customerFirstName+" "+customerLastName);
        System.out.println("**** "+this.getMostOccurencesInList(boughtBrandsOccurence)+" ****");
        // Affichage du tag le plus utilisé par le client
        System.out.println("Most used tag for "+customerFirstName+" "+customerLastName);
        System.out.println("**** "+this.getMostUsedTagForCustomer("4145")+" ****");
    }


    /*  Query 2. For a given product during a given period, find the people who commented or
        posted on it, and had bought it
    */

    public void query2(String productName, String fromDate, String toDate) {
        ArrayList<Row> peopleWhoBoughtIt = new ArrayList<>();
        String customerId;
        // Récupération des personnes qui ont posté sur le produit pendant une certaine période de temps
        ArrayList<Row> peopleWhoPosted = this.getPeopleWhoPostedOnProduct(productName, fromDate,toDate);
        if(peopleWhoPosted.size() > 0) {
            // Récupération des commandes des clients qui ont posté et recherche du produit dans leur commande
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
        // Affichage des personnes qui ont parlé du produit et l'ont acheté
        if(peopleWhoBoughtIt.size() > 0 ) {
            System.out.println("\nPeople who talked about our product and bought it: " + productName.toUpperCase());
            for(Row row : peopleWhoBoughtIt) {
                this.bigTableClient.printRow(row);
            }
        }
    }


    /* Query 3. For a given product during a given period, find people who have undertaken
    activities related to it, e.g., posts, comments, and review, and return sentences from these texts
    that contain negative sentiments.
    */

    public void query3(String asin, String fromDate, String toDate) {
        String productName = getNameforProduct(asin);
        // Récupération des clients qui ont parlé du produit
        ArrayList<Row> peopleWhoPosted = getPeopleWhoPostedOnProduct(productName,fromDate,toDate);
        ArrayList<String> negativeFeedbacks;
        for(Row row : peopleWhoPosted) {
            // Récupération des feedbacks sur ce produit par les clients qui ont en parlé
            ArrayList<Row> feedBacks = getFeedbacksForSpecificCustomerAndSpecificProduct(getValueForQualifier(row,CUSTOMER_COLUMN_FAMILY,"id"),asin);
            if(feedBacks.size() > 0) {
                // Filtrage pour récupérer seulement les feedbacks négatifs si il y a
                String firstName = getValueForQualifier(row,CUSTOMER_COLUMN_FAMILY,"firstName");
                String lastName = getValueForQualifier(row,CUSTOMER_COLUMN_FAMILY,"lastName");
                System.out.println(firstName+" "+lastName+" reviewed this product");
                negativeFeedbacks = getNegativeFeedbacks(feedBacks);
                // Affichage des feedbacks négatifs
                if(negativeFeedbacks.size() > 0) {
                    negativeFeedbacks.forEach(System.out::println);
                }
            }
        }
    }


    /* Query 4. Find the top-2 persons who spend the highest amount of money in orders. Then for
       each person, traverse her knows-graph with 3-hop to find the friends, and finally return the
       common friends of these two persons.
    */

    public void query4() {
        System.out.println("The top 2 persons who spent the highest amount of money in orders");
        ArrayList<Row> topPeople = new ArrayList<>();
        // Récupération des deux clients qui ont dépensé le plus en commande avec la méthode getTop2PeopleAtSpending()
        ArrayList<String> topPeopleId = this.getTop2PeopleAtSpending();
        ArrayList<Row> mutualFriends = new ArrayList<>();
        // Récupération de la ligne des plus gros acheteurs et des amis
        for(int i = 0; i < 2; i++){
            topPeople.add(this.getCustomerRowWithId(topPeopleId.get(i)));
            if(i == 0) {
                mutualFriends.addAll(this.getCustomerFriends(topPeopleId.get(i),3));
            } else {
                // Pour le deuxième acheteur, conservation des amis en commun et suppresion des autres
                ArrayList<Row> friends = this.getCustomerFriends(topPeopleId.get(i),3);
                mutualFriends.forEach(friend -> {
                    if(!friends.contains(friend)){
                        mutualFriends.remove(friend);
                    }
                });
            }
        }
        // Suppression des doublons
        ArrayList<Row> mutualFriendsWithoutDuplicates = new ArrayList<>(
                new HashSet<>(mutualFriends));
        // On affiche les amis mutuels et les deux plus gros acheters
        System.out.println("Mutual Friends :");
        mutualFriendsWithoutDuplicates.forEach(friend -> System.out.println(this.extractCustomerInfoFromRow(friend)));
        topPeople.forEach(peopleRow -> System.out.println(this.extractCustomerInfoFromRow(peopleRow)));
    }


    /* Query 5. Given a start customer and a product category, find persons who are this customer's
    friends within 3-hop friendships in Knows graph, besides, they have bought products in the
    given category. Finally, return feedback with the 5-rating review of those bought products.
    */

    public void query5(String customerId, String brandName) {
        // Récupération des amis du customer avec un hop de 3
        ArrayList<Row> friends = this.getCustomerFriends(customerId,3);
        ArrayList<Row> friendsWhoBought = new ArrayList<>();
        ArrayList<Row> productsOrder;
        ArrayList<Row> orderRows;
        ArrayList<Row> positiveFeedbacks = new ArrayList<>();
        for(Row row : friends) {
            // Récupération des commandes pour chaque ami
            String id = this.getValueForQualifier(row,CUSTOMER_COLUMN_FAMILY,"id");
            orderRows = this.getFamilyRowsForSpecificCustomer(ORDER_COLUMN_FAMILY, id, "personId");

            // Check si les amis ont acheté le produit de la marque ou non, si oui ajout à la liste et récupération des feedbacks positifs
            for(Row orderRow: orderRows) {
                productsOrder = this.getProductsBoughtByOrder(this.getValueForQualifier(orderRow,ORDER_COLUMN_FAMILY,"orderId"));
                productsOrder.forEach(productRow -> {
                    if(this.isProductFromThisBrand(productRow,brandName)) {
                        friendsWhoBought.add(row);
                        positiveFeedbacks.addAll(this.getPositiveFeedbacksForSpecificProduct(this.getValueForQualifier(productRow,PRODUCT_COLUMN_FAMILY,"asin")));
                    }
                });
            }
            // Affichage des amis qui ont acheté le produit
            if(friendsWhoBought.size() > 0) {
                System.out.println("Friends who bought");
                friendsWhoBought.forEach(System.out::println);
            }
            // Affichage des feedbacks positif des produits achetés
            if(positiveFeedbacks.size() > 0) {
                positiveFeedbacks.forEach(feedback -> System.out.println(this.getValueForQualifier(feedback,FEEDBACK_COLUMN_FAMILY,"comment")));
            }
        }
    }

    /* Query 7. For the products of a given vendor with declining sales compare to the former
        quarter, analyze the reviews for these items to see if there are any negative sentiments.
    */
    public void query7(String vendor) {
        // Récupération des lignes des produits
        Filters.Filter filter = FILTERS.family().exactMatch(PRODUCT_COLUMN_FAMILY);
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        String startDatePreviousQuarter = "2022-01-01";
        String endDatePreviousQuarter = "2022-03-31";
        String startDateActualQuarter = "2022-04-01";
        String endDateActualQuarter = "2022-06-30";
        ArrayList<Row> negativeFeedbacks = new ArrayList<>();
        String asin;
        // On cherche les produits qui appartiennent au vendeur passé en parametre
        for(Row row : rows) {
            if(this.getValueForQualifier(row,PRODUCT_COLUMN_FAMILY,"brand").equals(vendor)) {
                asin = getValueForQualifier(row,PRODUCT_COLUMN_FAMILY,"asin");
                // On compare les ventes du produit entre le quarter précèdent et le quarter actuel
                if(this.getSellsForSpecificProductForSpecificPeriod(asin,startDatePreviousQuarter,endDatePreviousQuarter)
                > this.getSellsForSpecificProductForSpecificPeriod(asin,startDateActualQuarter,endDateActualQuarter)) {
                    // On récupère les feedbacks négatifs si les ventes sont en baisse
                   negativeFeedbacks.addAll(this.getNegativeFeedbacksForSpecificProduct(asin));
                }
            }
        }
        // Affichage des feedbacks négatifs si il y a
        if(negativeFeedbacks.size() > 0) {
            System.out.println("Negative feedbacks for bad products");
            negativeFeedbacks.forEach(System.out::println);
        }
    }

    // Methode qui renvoit la liste des lignes des gens qui ont posté sur un produit avec le nom de celui ci
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

    // Méthode pour extraire des informations d'un client à partir de la ligne associée
    public String extractCustomerInfoFromRow(Row row) {
        return this.getValueForQualifier(row,CUSTOMER_COLUMN_FAMILY,"id")+" "+
                this.getValueForQualifier(row,CUSTOMER_COLUMN_FAMILY,"firstName")+" "+
                this.getValueForQualifier(row,CUSTOMER_COLUMN_FAMILY,"lastName");
    }

    // Méthode pour récupérer les 2 plus gros clients en fonction des dépenses
    public ArrayList<String> getTop2PeopleAtSpending() {
        HashMap<String,Double> spentAmounts = new HashMap<>();
        String id;
        Double amount;
        Filters.Filter filter = FILTERS.family().exactMatch(CUSTOMER_COLUMN_FAMILY);
        ServerStream<Row> customers = this.bigTableClient.getRowsWithFilter(filter);
        ArrayList<Double> amounts = new ArrayList<>();
        for(Row row : customers) {
            id = getValueForQualifier(row, CUSTOMER_COLUMN_FAMILY, "id");
            amount = this.getSpentAmountForSpecificCustomer(id);
            spentAmounts.put(id, amount);
            amounts.add(amount);
        }
        Collections.sort(amounts, Collections.reverseOrder());
        ArrayList<String> topPeopleId = new ArrayList<>();
        for (Map.Entry<String,Double> entry : spentAmounts.entrySet()) {
            if (topPeopleId.size()< 2 && (entry.getValue().equals(amounts.get(0)) || entry.getValue().equals(amounts.get(1)))) {
                topPeopleId.add(entry.getKey());
            } else if(topPeopleId.size() >= 2) {
                return topPeopleId;
            }
        }
        return topPeopleId;
    }

    //Méthode pour récupérer l'argent dépensé total pour un client spécifique
    public Double getSpentAmountForSpecificCustomer(String customerId) {
        Double amount = 0.0;
        Filters.Filter filter = FILTERS.family().exactMatch(ORDER_COLUMN_FAMILY);
        ServerStream<Row> orders = this.bigTableClient.getRowsWithFilter(filter);
        for(Row row : orders) {
            if(getValueForQualifier(row, ORDER_COLUMN_FAMILY, "personId").equals(customerId)) {
                amount += Float.parseFloat(getValueForQualifier(row, ORDER_COLUMN_FAMILY, "totalPrice"));
            }
        }
        return amount;
    }

    // Méthode qui renvoit la liste des lignes des amis d'une client spécifique avec le nombre de hop en paramètre
    public ArrayList<Row> getCustomerFriends(String id, Integer hop) {
        String KNOWN_PERSON_COLUMN_FAMILY = "Person_Known";
        Filters.Filter filter = FILTERS.family().exactMatch(KNOWN_PERSON_COLUMN_FAMILY);
        ArrayList<String> lookingForFriends = new ArrayList<>();
        ArrayList<String> newFriends = new ArrayList<>();
        lookingForFriends.add(id);
        for(int i = 0; i < hop; i++) {
            ServerStream<Row> friends = this.bigTableClient.getRowsWithFilter(filter);
            for (Row row : friends) {
                String friendId = getValueForQualifier(row, KNOWN_PERSON_COLUMN_FAMILY, "friendId");
                if (lookingForFriends.contains(getValueForQualifier(row, KNOWN_PERSON_COLUMN_FAMILY, "personId"))
                        && !lookingForFriends.contains(friendId)) {
                    newFriends.add(friendId);
                }
            }
            lookingForFriends.addAll(newFriends);
        }
        lookingForFriends.remove(id);
        ArrayList<Row> friendsRow = new ArrayList<>();
        lookingForFriends.forEach(friendId -> {
            friendsRow.add(getCustomerRowWithId(friendId));});
        return friendsRow;
    }

    // Méthode pour récupérer la ligne d'un client avec son Id
    public Row getCustomerRowWithId(String id) {
        Filters.Filter filter = FILTERS.family().exactMatch(CUSTOMER_COLUMN_FAMILY);
        ServerStream<Row> customers = this.bigTableClient.getRowsWithFilter(filter);
        for(Row row : customers) {
            if(getValueForQualifier(row,CUSTOMER_COLUMN_FAMILY,"id").equals(id)) {
                return row;
            }
        }
        return null;
    }

    // Méthode pour récupérer les feedbacks d'un client spécifique à propos d'un produit spécifique
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

    // Méthode pour récupérer les feedbacks positifs pour un produit spécifique
    public ArrayList<Row> getPositiveFeedbacksForSpecificProduct(String productAsin) {
        ArrayList<Row> results = new ArrayList<>();
        Filters.Filter filter = FILTERS.family().exactMatch(FEEDBACK_COLUMN_FAMILY);
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        for(Row row : rows){
            if(getValueForQualifier(row,FEEDBACK_COLUMN_FAMILY,"asin").equals(productAsin) &&
            isFeedBackPositive(row)) {
                results.add(row);
            }
        }
        return results;
    }

    // Méthode pour récupérer les feedbacks negatifs pour un produit spécifique
    public ArrayList<Row> getNegativeFeedbacksForSpecificProduct(String productAsin) {
        ArrayList<Row> results = new ArrayList<>();
        Filters.Filter filter = FILTERS.family().exactMatch(FEEDBACK_COLUMN_FAMILY);
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        for(Row row : rows){
            if(getValueForQualifier(row,FEEDBACK_COLUMN_FAMILY,"asin").equals(productAsin) &&
                    isFeedBackNegative(row)) {
                results.add(row);
            }
        }
        return results;
    }

    //Méthode pour filter une liste de lignes de feedbacks et renvoyer seulement la partie "comment" si negatif
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

    // Méthode qui renvoit true si le feedback de la ligne est positif
    public boolean isFeedBackPositive(Row feedBackRow) {
        String feedback = getValueForQualifier(feedBackRow,FEEDBACK_COLUMN_FAMILY,"comment");
        if(Integer.parseInt(feedback.substring(1,2)) == 5) {
            return true;
        }
        return false;
    }

    // Méthode qui renvoit true si le feedback de la ligne est negatif
    public boolean isFeedBackNegative(Row feedBackRow) {
        String feedback = getValueForQualifier(feedBackRow,FEEDBACK_COLUMN_FAMILY,"comment");
        if(Integer.parseInt(feedback.substring(1,2)) < 2) {
            return true;
        }
        return false;
    }

    // Méthode pour récupérer le nom d'un produit à partir de son asin
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

    // Méthode qui renvoit l'élèment de la liste qui a le plus d'occurences
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

    // Méthode qui vérifie si un qualifier de la ligne a cette valeur
    public boolean checkIfRowAsValueForQualifier(Row row, String family, String qualifier, String value) {
        return row.getCells(family, qualifier).get(0).getValue().toStringUtf8().equals(value);
    }

    // Méthode qui récupère la valeur d'un qualifier d'une ligne
    public String getValueForQualifier(Row row, String family, String qualifier) {
        return row.getCells(family, qualifier).get(0).getValue().toStringUtf8();
    }

    // Méthode qui renvoit true si la date à vérifier et entre les deux dates passées en paramètres
    public boolean checkIfDateInRange(String dateToChecked, String dateFrom, String dateTo) {
        return getDateFromString(dateToChecked.substring(0,10)).after(getDateFromString(dateFrom))
                && getDateFromString(dateToChecked.substring(0,10)).before(getDateFromString(dateTo));
    }

    // Méthode pour créer une date à partir d'un String
    public Date getDateFromString(String date) {
        return Date.from(LocalDate.parse(date).atStartOfDay(ZoneId.systemDefault()).toInstant());
    }

    // Méthode pour récupérer la liste des marques des produits achetés dans une commande spécifique
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

    // Méthode pour récupérer la liste des lignes des produits associés à une commande
    public ArrayList<Row> getProductsBoughtByOrder(String id) {
        ArrayList<Row> productsRow = new ArrayList<>();
        Filters.Filter filter = FILTERS.family().exactMatch(ORDER_ORDERLINE_COLUMN_FAMILY);
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        rows.forEach(row -> {
            if(getValueForQualifier(row,ORDER_ORDERLINE_COLUMN_FAMILY,"orderId").equals(id)){
                productsRow.add(row);
            }
        });
        return productsRow;
    }

    // Méthode qui renvoit true si le produit est de la marque passée en paramètre
    public boolean isProductFromThisBrand(Row row, String brandName) {
        return this.getValueForQualifier(row,ORDER_ORDERLINE_COLUMN_FAMILY,"brand").equals(brandName);
    }

    // Méthode pour récupérer la liste des noms des produits dans une commande
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

    /* Méthode pour récupérer un object lié à un customer
     ex : getFamilyRowsForSpecificCustomer("Feedback",customerId,"personId") pour récupérer les lignes des feedbacks
     données par un client spécifique */
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

    // Méthode pour récupérer le nombre de ventes pour un produit spécifique et une période donnée
    public Integer getSellsForSpecificProductForSpecificPeriod(String asin, String fromDate, String toDate) {
        Integer sells = 0;
        Filters.Filter filter = FILTERS.family().exactMatch(ORDER_COLUMN_FAMILY);
        ServerStream<Row> rows = this.bigTableClient.getRowsWithFilter(filter);
        for(Row row: rows) {
            if(this.checkIfDateInRange(this.getValueForQualifier(row,ORDER_COLUMN_FAMILY,"orderDate"),fromDate,toDate)) {
                ArrayList<Row> products = this.getProductsBoughtByOrder(this.getValueForQualifier(row, ORDER_COLUMN_FAMILY, "orderId"));
                for (Row productRow : products) {
                    System.out.println(productRow);
                    if (getValueForQualifier(productRow, ORDER_ORDERLINE_COLUMN_FAMILY, "asin").equals(asin)) {
                        sells++;
                    }
                }
            }
        }
        return sells;
    }

    //Méthode pour récupérer le tag le plus utilisé pour un client spécifique
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
