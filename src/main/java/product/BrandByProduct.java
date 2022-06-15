package product;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class BrandByProduct {
    @JsonProperty("brandName")
    String brandName;
    @JsonProperty("productId")
    String productId;

    public BrandByProduct(){}
}
