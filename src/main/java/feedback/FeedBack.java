package feedback;

import lombok.Data;

@Data
public class FeedBack {
    String asin;
    String personID;
    String comment;

    public FeedBack(String asin, String personID, String comment) {
        this.asin = asin;
        this.personID = personID;
        this.comment = comment;
    }
}
