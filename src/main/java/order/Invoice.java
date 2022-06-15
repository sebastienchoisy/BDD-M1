package order;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.Data;

import java.util.ArrayList;

@Data
public class Invoice {
    String orderID;
    String personID;
    String orderDate;
    String totalPrice;
    ArrayList<OrderLine> orderLine;

    public Invoice(String orderID, String personID, String orderDate, String totalPrice, ArrayList<OrderLine> orderLine) {
        this.orderID = orderID;
        this.personID = personID;
        this.orderDate = orderDate;
        this.totalPrice = totalPrice;
        this.orderLine = orderLine;
    }

    public Invoice() {}
}
