package order;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;
import product.Product;

import java.util.ArrayList;

@Data
public class Order {
    @JsonProperty("OrderId")
    String orderID;
    @JsonProperty("PersonId")
    String personID;
    @JsonProperty("OrderDate")
    String orderDate;
    @JsonProperty("TotalPrice")
    String totalPrice;
    @JsonProperty("Orderline")
    ArrayList<Product> orderline;

    public Order(String orderID, String personID, String orderDate, String totalPrice, ArrayList<Product> orderLine) {
        this.orderID = orderID;
        this.personID = personID;
        this.orderDate = orderDate;
        this.totalPrice = totalPrice;
        this.orderline = orderLine;
    }

    public Order() {
        super();
    }
}
