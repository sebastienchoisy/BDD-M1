package vendor;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Vendor {
    @JsonProperty("Vendor")
    String vendor;
    @JsonProperty("country")
    String country;
    @JsonProperty("industry")
    String industry;

    public Vendor() {}
}
