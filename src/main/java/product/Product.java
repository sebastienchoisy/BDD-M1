package product;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;

@Data
@JsonPropertyOrder({"asin","title","price","imgUrl"})
public class Product {
    String asin;
    String title;
    String price;
    String imgUrl;
    @JsonIgnore
    String brand;

    public Product(String asin, String title, String price, String imgUrl) {
        this.asin = asin;
        this.title = title;
        this.price = price;
        this.imgUrl = imgUrl;
    }

    public Product() {}
}
