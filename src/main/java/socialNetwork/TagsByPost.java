package socialNetwork;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class TagsByPost {
    @JsonProperty("Post.Id")
    String postId;
    @JsonProperty("Tag.Id")
    String tagId;
}
