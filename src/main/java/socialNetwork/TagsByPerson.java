package socialNetwork;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class TagsByPerson {
    @JsonProperty("Person.Id")
    String personId;
    @JsonProperty("Tag.Id")
    String tagId;
}
