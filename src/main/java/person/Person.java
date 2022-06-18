package person;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;

@Data
@JsonPropertyOrder(value={"id","firstName","lastName","gender","birthday","creationDate","locationIP","browserUsed","place"})
public class Person {
    private String id;
    private String firstName;
    private String lastName;
    private String gender;
    private String birthday;
    private String creationDate;
    private String locationIP;
    private String browserUsed;
    private String place;

    public Person(String id, String firstName, String lastName, String gender, String birthday, String creationDate,
                  String locationIP, String browserUsed, String place) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.gender = gender;
        this.birthday = birthday;
        this.creationDate = creationDate;
        this.locationIP = locationIP;
        this.browserUsed = browserUsed;
        this.place = place;
    }
    public Person(){}
}
