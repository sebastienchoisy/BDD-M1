package socialNetwork;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;

@Data
@JsonPropertyOrder(value={"personId","friendId","creationDate"})
public class PersonLink {
    String personId;
    String friendId;
    String creationDate;

    PersonLink() {}
}
