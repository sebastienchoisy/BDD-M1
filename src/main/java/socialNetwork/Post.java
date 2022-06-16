package socialNetwork;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;

@Data
@JsonPropertyOrder({"id","imageFile","creationDate","locationIP","browserUsed", "language","content","length"})
public class Post {
    String id;
    String imageFile;
    String creationDate;
    String locationIP;
    String browserUsed;
    String language;
    String content;
    String length;
    String authorId;

    public Post(String id, String imageFile, String creationDate,
                String locationIP, String browserUsed, String language, String content, String length, String authorId) {
        this.id = id;
        this.imageFile = imageFile;
        this.creationDate = creationDate;
        this.locationIP = locationIP;
        this.browserUsed = browserUsed;
        this.language = language;
        this.content = content;
        this.length = length;
        this.authorId = authorId;
    }

    public Post() {}
}
