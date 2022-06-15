package filereader;

import order.Invoice;
import order.OrderLine;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.ArrayList;

public class XmlFileReader {

    public ArrayList<Invoice> getInvoicesFromXml() {
        ArrayList<Invoice> Invoices = new ArrayList<>();
        try {
            File XmlFile = new File("././data/Invoice/Invoice.xml");
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(XmlFile);
            doc.getDocumentElement().normalize();

            NodeList nList = doc.getElementsByTagName("Invoice");
            for(int i = 0; i < nList.getLength(); i++) {
                Invoices.add(this.getInvoiceFromXml(nList.item(i)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Invoices;
    }

    private OrderLine getOrderlineFromXml(Node orderLine) {
        Element elt = (Element) orderLine;
        return new OrderLine(elt.getElementsByTagName("productId").item(0).getTextContent(),
                elt.getElementsByTagName("asin").item(0).getTextContent(),
                elt.getElementsByTagName("title").item(0).getTextContent(),
                elt.getElementsByTagName("price").item(0).getTextContent(),
                elt.getElementsByTagName("brand").item(0).getTextContent());
    }

    private Invoice getInvoiceFromXml(Node invoice) {
        Element elt = (Element) invoice;
        ArrayList<OrderLine> orderLineList = new ArrayList<>();
        NodeList nList = elt.getElementsByTagName("Orderline");
        for(int i = 0; i < nList.getLength(); i++){
            orderLineList.add(this.getOrderlineFromXml(nList.item(i)));
        }
        return new Invoice(elt.getElementsByTagName("OrderId").item(0).getTextContent(),
                elt.getElementsByTagName("PersonId").item(0).getTextContent(),
                elt.getElementsByTagName("OrderDate").item(0).getTextContent(),
                elt.getElementsByTagName("TotalPrice").item(0).getTextContent(),
                orderLineList);
    }
}
