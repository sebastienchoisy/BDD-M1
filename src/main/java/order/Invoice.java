package order;

import lombok.Data;

@Data
public class Invoice {
    String orderId;
    String personId;
    String orderDate;
    String totalPrice;

    public Invoice(String orderId, String personId, String orderDate, String totalPrice) {
        this.orderId = orderId;
        this.personId = personId;
        this.orderDate = orderDate;
        this.totalPrice = totalPrice;
    }

    public Invoice() {}
}
