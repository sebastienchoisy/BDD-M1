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
    String orderId;
    @JsonProperty("PersonId")
    String personId;
    @JsonProperty("OrderDate")
    String orderDate;
    @JsonProperty("TotalPrice")
    String totalPrice;
    @JsonProperty("Orderline")
    ArrayList<Product> orderline;

    public Order(String orderId, String personId, String orderDate, String totalPrice, ArrayList<Product> orderLine) {
        this.orderId = orderId;
        this.personId = personId;
        this.orderDate = orderDate;
        this.totalPrice = totalPrice;
        this.orderline = orderLine;
    }

    public void setOrderIdForOrderLine() {
        this.orderline.forEach(product -> {
            product.setOrderId(this.orderId);
            if(product.getImgUrl() == null) {
                product.setImgUrl("");
            }
        });
    }

    public Order() {
        super();
    }
}
