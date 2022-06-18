package order;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;

import java.util.ArrayList;

@Data
@JsonPropertyOrder(value={"productId","asin","title","price","brand"})
public class OrderLine {
    String productID;
    String asin;
    String title;
    String price;
    String brand;

    public OrderLine(String productID, String asin, String title, String price, String brand) {
        this.productID = productID;
        this.asin = asin;
        this.title = title;
        this.price = price;
        this.brand = brand;
    }

    public OrderLine() {}
}
