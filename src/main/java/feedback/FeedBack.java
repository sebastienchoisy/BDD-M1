package feedback;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;

@Data
@JsonPropertyOrder(value={"asin","personId","comment"})
public class FeedBack {
    String asin;
    String personId;
    String comment;

    public FeedBack(String asin, String personID, String comment) {
        this.asin = asin;
        this.personId = personID;
        this.comment = comment;
    }

    public FeedBack() {}
}
