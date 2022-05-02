package filereader;

import feedback.FeedBack;
import person.Person;

import java.io.*;
import java.util.ArrayList;

public class CsvReader {
    File file;
    FileReader reader;
    BufferedReader bReader;

    public ArrayList<Person> getPersonsDataListFromCsvFile(){
        ArrayList<Person> list = new ArrayList();
        String delimiter = "\\|";
        try {
            this.file = new File("././data/Customer/person_0_0.csv");
            this.reader = new FileReader(file);
            this.bReader = new BufferedReader(this.reader);
            String line;
            this.bReader.readLine();
            String[] tempArr;
            while((line = this.bReader.readLine()) != null) {
                tempArr = line.split(delimiter);
                list.add(new Person(tempArr[0],tempArr[1],tempArr[2],tempArr[3],tempArr[4],tempArr[5],
                        tempArr[6],tempArr[7],tempArr[8]));
            }
            this.bReader.close();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        }
       return list;
    }

    public ArrayList<FeedBack> getFeedBacksDataListFromCsvFile(){
        ArrayList<FeedBack> list = new ArrayList();
        String delimiter = "\\|";
        try {
            this.file = new File("././data/Feedback/Feedback.csv");
            this.reader = new FileReader(file);
            this.bReader = new BufferedReader(this.reader);
            String line;
            this.bReader.readLine();
            String[] tempArr;
            while((line = this.bReader.readLine()) != null) {
                tempArr = line.split(delimiter);
                list.add(new FeedBack(tempArr[0],tempArr[1],tempArr[2]));
            }
            this.bReader.close();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        }
        return list;
    }


}
