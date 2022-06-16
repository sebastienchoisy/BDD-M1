package socialNetwork;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class AuthorByPost {
    @JsonProperty("Person.id")
    String postId;
    @JsonProperty("post.id")
    String personId;

    public AuthorByPost() {}
}
