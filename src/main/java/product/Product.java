package product;

public class Product {
    String asin;
    String title;
    String price;
    String imgUrl;

    public Product(String asin, String title, String price, String imgUrl) {
        this.asin = asin;
        this.title = title;
        this.price = price;
        this.imgUrl = imgUrl;
    }
}
