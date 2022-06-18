package order;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;


@Data
@JsonPropertyOrder(value={"productId","asin","title","price","brand"})
public class OrderLine {
    String productId;
    String asin;
    String title;
    String price;
    String brand;
    String orderId;

    public OrderLine(String productId, String asin, String title, String price, String brand, String orderId) {
        this.productId = productId;
        this.asin = asin;
        this.title = title;
        this.price = price;
        this.brand = brand;
        this.orderId = orderId;
    }

    public OrderLine() {}
}
