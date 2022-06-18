package dataManager;

import java.io.IOException;


public class DataLoader {
    public void loadAllData() throws IOException {
        CustomerDataLoader customerLoader = new CustomerDataLoader();
        customerLoader.writeToTable();
    }
}
