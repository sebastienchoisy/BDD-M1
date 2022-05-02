package order;

public class Order {
    String orderID;
    String personID;
    String orderDate;
    String totalPrice;
    OrderLine orderLine;

    public Order(String orderID, String personID, String orderDate, String totalPrice, OrderLine orderLine) {
        this.orderID = orderID;
        this.personID = personID;
        this.orderDate = orderDate;
        this.totalPrice = totalPrice;
        this.orderLine = orderLine;
    }
}
