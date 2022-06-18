package vendor;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Vendor {
    @JsonProperty("Vendor")
    String vendor;
    @JsonProperty("Country")
    String country;
    @JsonProperty("Industry")
    String industry;

    public Vendor() {}
}
