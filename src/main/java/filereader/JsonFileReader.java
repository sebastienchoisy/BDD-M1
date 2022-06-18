package filereader;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import order.Order;
import java.io.*;
import java.util.ArrayList;

public class JsonFileReader {
    private File file;
    private ObjectMapper mapper;

    public JsonFileReader() {
        mapper = new ObjectMapper();
    }

    public static void main(String[] args) throws IOException {
        JsonFileReader reader = new JsonFileReader();
        reader.getOrdersFromJson();
    }

    public ArrayList<Order> getOrdersFromJson() throws IOException {
        this.fixOrdersJson();
        this.file = new File("././data/Order/OrderFixed.json");
        ArrayList<Order> ordersList = this.mapper.readValue(this.file, new TypeReference<>() {});
        ordersList.forEach(order -> order.setOrderIdForOrderLine());
        return ordersList;
    }

    private void fixOrdersJson() throws IOException {
        this.file = new File("././data/Order/Order.json");
        System.out.println("Traitement Json car mauvais format...");
        ArrayList<String> OrdersJson = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(this.file));
        String line = reader.readLine();

        while (line != null)
        {
            OrdersJson.add(line);
            line = reader.readLine();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter("././data/Order/OrderFixed.json",true));
        writer.append("[");
        for(int i = 0; i < OrdersJson.size(); i++) {
            try {
                if(i == OrdersJson.size() - 1 ){
                    writer.append(OrdersJson.get(i));
                } else {
                    writer.append(OrdersJson.get(i) + ",");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        writer.append("]");
        writer.close();
    }
}
