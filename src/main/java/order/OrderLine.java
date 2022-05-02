package order;

import lombok.Data;

@Data
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
}
