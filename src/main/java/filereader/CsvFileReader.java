package filereader;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import feedback.FeedBack;
import person.Person;
import socialNetwork.*;
import product.BrandByProduct;
import product.Product;
import vendor.Vendor;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class CsvFileReader {
    private File file;
    private final CsvMapper csvMapper;
    private final char COLUMN_SEPARATOR = '|';

    public CsvFileReader() {
        this.csvMapper = new CsvMapper();
    }

    // Récupération des clients du fichier CSV et on retourne une liste d'objets Person
    public ArrayList<Person> getPersonsDataListFromCsvFile() throws IOException {
        ArrayList<Person> persons;
        CsvSchema schema = csvMapper.schemaFor(Person.class).withHeader().withColumnSeparator(COLUMN_SEPARATOR);
        this.file = new File("././data/Customer/person_0_0.csv");
        MappingIterator<Person> it = csvMapper
                .readerFor(Person.class)
                .with(schema)
                .readValues(this.file);
        persons = (ArrayList<Person>) it.readAll();
        return persons;
    }

    // Récupération des feedbacks du fichier CSV et on retourne une liste d'object Feedback
    public ArrayList<FeedBack> getFeedBacksDataListFromCsvFile() throws IOException {
        CsvSchema schema = csvMapper.schemaFor(FeedBack.class).withoutHeader().withColumnSeparator(COLUMN_SEPARATOR).withEscapeChar('\\');
        this.file = new File("././data/Feedback/Feedback.csv");
        MappingIterator<FeedBack> it = csvMapper
                .readerFor(FeedBack.class)
                .with(schema)
                .readValues(this.file);
        return (ArrayList<FeedBack>) it.readAll();
    }

    // Récupération des products du fichier CSV et on retourne une liste de products (On récupère la marque de l'autre fichier CSV)
    public ArrayList<Product> getProductsFromCsvFile() throws IOException {
        HashMap<String,String> productMap = new HashMap<>();
        CsvSchema schema = csvMapper.schemaFor(Product.class).withHeader().withColumnSeparator(',');
        this.file = new File("././data/Product/Product.csv");
        MappingIterator<Product> it = csvMapper
                .readerFor(Product.class)
                .with(schema)
                .readValues(this.file);
        ArrayList<BrandByProduct> brands = this.getBrandByProductFromCsvFile();
        brands.forEach(brandByProduct -> productMap.put(brandByProduct.getProductId(),brandByProduct.getBrandName()));
        ArrayList<Product> products = (ArrayList<Product>) it.readAll();
        products.forEach(product -> product.setBrand(productMap.get(product.getAsin())));
        return products;
    }

    // Récupération de la marque de chaque produit et on renvoit une liste d'objets BrandByProduct
    public ArrayList<BrandByProduct> getBrandByProductFromCsvFile() throws IOException {
        CsvSchema schema = csvMapper.schemaFor(BrandByProduct.class).withoutHeader().withColumnSeparator(',');
        this.file = new File("././data/Product/BrandByProduct.csv");
        MappingIterator<BrandByProduct> it = csvMapper
                .readerFor(BrandByProduct.class)
                .with(schema)
                .readValues(this.file);
        return (ArrayList<BrandByProduct>) it.readAll();
    }

    // Récupération des tags pour les personnes depuis le csv
    public ArrayList<TagsByPerson> getTagsByPersonFromCsvFile() throws IOException {
        CsvSchema schema = csvMapper.schemaFor(TagsByPerson.class).withHeader().withColumnSeparator(COLUMN_SEPARATOR);
        this.file = new File("././data/SocialNetwork/person_hasInterest_tag_0_0.csv");
        MappingIterator<TagsByPerson> it = csvMapper
                .readerFor(TagsByPerson.class)
                .with(schema)
                .readValues(this.file);
        return (ArrayList<TagsByPerson>) it.readAll();
    }

    // Récupération des liens entre les personnes depuis le csv
    public ArrayList<PersonLink> getLinkByPersonFromCsvFile() throws IOException {
        CsvSchema schema = csvMapper.schemaFor(PersonLink.class).withoutHeader().withColumnSeparator(COLUMN_SEPARATOR);
        this.file = new File("././data/SocialNetwork/person_knows_person_0_0.csv");
        MappingIterator<PersonLink> it = csvMapper
                .readerFor(PersonLink.class)
                .with(schema)
                .readValues(this.file);
        ArrayList<PersonLink> list = (ArrayList<PersonLink>) it.readAll();
        // On supprime le première élèment avec les headers (compliqué à traiter avec des headers nommés identiquement)
        list.remove(0);
        return list;
    }

    // Récupération des posts du file CSV
    public ArrayList<Post> getPostsFromCsvFile() throws IOException {
        CsvSchema schema = csvMapper.schemaFor(Post.class).withHeader().withColumnSeparator(COLUMN_SEPARATOR);
        this.file = new File("././data/SocialNetwork/post_0_0.csv");
        MappingIterator<Post> it = csvMapper
                .readerFor(Post.class)
                .with(schema)
                .readValues(this.file);
        ArrayList<Post> list =  (ArrayList<Post>) it.readAll();
        ArrayList<AuthorByPost> authorByPost = this.getAuthorByPost();
        HashMap<String,String> hashMap = new HashMap<>();
        // On ajoute aussi l'auteur directement pour faciliter la structure
        authorByPost.forEach(obj -> hashMap.put(obj.getPostId(), obj.getPersonId()));
        list.forEach(post -> post.setAuthorId(hashMap.get(post.getId())));
        return list;
    }

    // Récupération des auteurs par post depuis le file CSV
    public ArrayList<AuthorByPost> getAuthorByPost() throws IOException {
        CsvSchema schema = csvMapper.schemaFor(AuthorByPost.class).withHeader().withColumnSeparator(COLUMN_SEPARATOR);
        this.file = new File("././data/SocialNetwork/post_hasCreator_person_0_0.csv");
        MappingIterator<AuthorByPost> it = csvMapper
                .readerFor(AuthorByPost.class)
                .with(schema)
                .readValues(this.file);
        return (ArrayList<AuthorByPost>) it.readAll();
    }

    // Récupération des tags par post depuis le file CSV
    public ArrayList<TagsByPost> getTagsByPostFromCsvFile() throws IOException {
        CsvSchema schema = csvMapper.schemaFor(TagsByPost.class).withHeader().withColumnSeparator(COLUMN_SEPARATOR);
        this.file = new File("././data/SocialNetwork/post_hasTag_tag_0_0.csv");
        MappingIterator<TagsByPost> it = csvMapper
                .readerFor(TagsByPost.class)
                .with(schema)
                .readValues(this.file);
        return (ArrayList<TagsByPost>) it.readAll();
    }

    // Récupération des vendeurs depuis le file CSV
    public ArrayList<Vendor> getVendorsFromCsvFile() throws IOException {
        CsvSchema schema = csvMapper.schemaFor(Vendor.class).withHeader().withColumnReordering(true).withColumnSeparator(',');
        this.file = new File("././data/Vendor/Vendor.csv");
        MappingIterator<Vendor> it = csvMapper
                .readerFor(Vendor.class)
                .with(schema)
                .readValues(this.file);
        return (ArrayList<Vendor>) it.readAll();
    }
}
