package filereader;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import feedback.FeedBack;
import person.Person;
import product.BrandByProduct;
import product.Product;
import socialNetwork.TagsByPerson;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class CsvFileReader {
    private File file;
    private final CsvMapper csvMapper = new CsvMapper();
    private final char COLUMN_SEPARATOR = '|';

    public static void main(String[] args) throws IOException {
        CsvFileReader reader = new CsvFileReader();
        reader.getPersonsDataListFromCsvFile();
    }

    // On récupère les clients du fichier CSV et on retourne une liste d'objets Person
    public ArrayList<Person> getPersonsDataListFromCsvFile() throws IOException {
        ArrayList<Person> persons = new ArrayList<>();
        CsvSchema schema = csvMapper.schemaFor(Person.class).withHeader().withColumnSeparator(COLUMN_SEPARATOR);
        this.file = new File("././data/Customer/person_0_0.csv");
        MappingIterator<Person> it = csvMapper
                .readerFor(Person.class)
                .with(schema)
                .readValues(this.file);
        persons = (ArrayList<Person>) it.readAll();
        persons.forEach(person -> {
            try {
                person.setInterestTags(this.getTagsByPersonFromCsvFile().get(person.getId()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return persons;
    }

    // On récupère les feedbacks du fichier CSV et on retourne une liste d'object Feedback
    public ArrayList<FeedBack> getFeedBacksDataListFromCsvFile() throws IOException {
        CsvSchema schema = csvMapper.schemaFor(FeedBack.class).withHeader().withColumnSeparator(COLUMN_SEPARATOR);
        this.file = new File("././data/Feedback/Feedback.csv");
        MappingIterator<FeedBack> it = csvMapper
                .readerFor(FeedBack.class)
                .with(schema)
                .readValues(this.file);
        return (ArrayList<FeedBack>) it.readAll();
    }

    // On récupère Les products du fichier CSV et on retourne une liste de products (On récupère la marque de l'autre fichier CSV)
    public ArrayList<Product> getProductsFromCsvFile() throws IOException {
        HashMap<String,String> productMap = new HashMap<>();
        CsvSchema schema = csvMapper.schemaFor(Product.class).withHeader().withColumnSeparator(',');
        this.file = new File("././data/Product/Product.csv");
        MappingIterator<Product> it = csvMapper
                .readerFor(Product.class)
                .with(schema)
                .readValues(this.file);
        ArrayList<BrandByProduct> brands = this.getBrandByProductFromCsvFile();
        brands.forEach(brandByProduct -> {
            productMap.put(brandByProduct.getProductId(),brandByProduct.getBrandName());
        });
        ArrayList<Product> products = (ArrayList<Product>) it.readAll();
        products.forEach(product -> {
            product.setBrand(productMap.get(product.getAsin()));
        });
        return products;
    }

    // On récupère la marque de chaque produit et on renvoit une liste d'objets BrandByProduct
    public ArrayList<BrandByProduct> getBrandByProductFromCsvFile() throws IOException {
        CsvSchema schema = csvMapper.schemaFor(BrandByProduct.class).withoutHeader().withColumnSeparator(',');
        this.file = new File("././data/Product/BrandByProduct.csv");
        MappingIterator<BrandByProduct> it = csvMapper
                .readerFor(BrandByProduct.class)
                .with(schema)
                .readValues(this.file);
        return (ArrayList<BrandByProduct>) it.readAll();
    }

    public HashMap<String,ArrayList<String>> getTagsByPersonFromCsvFile() throws IOException {
        HashMap<String,ArrayList<String>> tagsByPersonMap = new HashMap<>();
        CsvSchema schema = csvMapper.schemaFor(TagsByPerson.class).withHeader().withColumnSeparator(COLUMN_SEPARATOR);
        this.file = new File("././data/SocialNetwork/person_hasInterest_tag_0_0.csv");
        MappingIterator<TagsByPerson> it = csvMapper
                .readerFor(TagsByPerson.class)
                .with(schema)
                .readValues(this.file);
        ArrayList<TagsByPerson> list = (ArrayList<TagsByPerson>) it.readAll();
        list.forEach(tagByPerson -> {
            String personId = tagByPerson.getPersonId();
            String tagId = tagByPerson.getTagId();
            if(tagsByPersonMap.containsKey(personId)){
                tagsByPersonMap.get(personId).add(tagId);
            } else {
                ArrayList<String> tagList = new ArrayList<>();
                tagList.add(tagId);
                tagsByPersonMap.put(personId, tagList);
            }
        });
        return tagsByPersonMap;
    }


}
