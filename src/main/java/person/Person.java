package person;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;
import java.util.ArrayList;

@Data
@JsonPropertyOrder({"id","firstName","lastName","gender","birthday","creationDate","locationIP","browserUsed","place"})
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
    @JsonIgnore
    private ArrayList<Person> knownPersons;
    @JsonIgnore
    private ArrayList<String> interestTags;

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
        this.knownPersons = new ArrayList<>();
        this.interestTags = new ArrayList<>();
    }

    public Person(){}

    public void addKnownPerson(Person person) {
        this.knownPersons.add(person);
    }

    public void addInterestTag(String tag) {
        this.interestTags.add(tag);
    }
}
