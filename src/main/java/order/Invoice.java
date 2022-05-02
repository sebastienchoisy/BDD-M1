package order;

import lombok.Data;

@Data
public class Invoice {
    String orderID;
    String personID;
    String orderDate;
    String totalPrice;
    OrderLine orderLine;

    public Invoice(String orderID, String personID, String orderDate, String totalPrice, OrderLine orderLine) {
        this.orderID = orderID;
        this.personID = personID;
        this.orderDate = orderDate;
        this.totalPrice = totalPrice;
        this.orderLine = orderLine;
    }
}
