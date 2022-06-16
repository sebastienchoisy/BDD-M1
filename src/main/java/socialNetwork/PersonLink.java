package socialNetwork;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class PersonLink {
    @JsonProperty("Person.id")
    String personId;
    @JsonProperty("friendId")
    String friendId;
    @JsonProperty("creationDate")
    String creationDate;

    PersonLink() {}
}
