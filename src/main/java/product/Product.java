package product;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;

@Data
@JsonPropertyOrder(value={"asin","title","price","imgUrl"})
public class Product {
    @JsonProperty("productId")
    String productId;
    String asin;
    String title;
    String price;
    String imgUrl;
    String brand;
    String orderId;

    public Product(String asin, String title, String price, String imgUrl) {
        this.asin = asin;
        this.title = title;
        this.price = price;
        this.imgUrl = imgUrl;
    }

    public Product() {}
}
