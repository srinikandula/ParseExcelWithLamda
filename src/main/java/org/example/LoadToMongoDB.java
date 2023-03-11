package org.example;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import org.bson.Document;

public class LoadToMongoDB {
    public static String COLLECTION_NAME = "clientData";
    public void loadToMongo(){
        // Creating a Mongo client
        MongoClient mongo = new MongoClient( "localhost" , 27017 );

        // Creating Credentials
        MongoCredential credential = MongoCredential.createCredential("sampleUser", "myDb", "password".toCharArray());
        System.out.println("Connected to the database successfully");

        //Accessing the database
        MongoDatabase database = mongo.getDatabase("myDb");

        //Creating a collection
        MongoCollection collection = database.getCollection(COLLECTION_NAME);
        try{
            database.createCollection(COLLECTION_NAME);
        }catch (Exception e) {
           // e.printStackTrace();
        }
        Document document = new Document();
        document.append("name", "Ram");
        document.append("age", 26);
        document.append("city", "Hyderabad");
        database.getCollection(COLLECTION_NAME).insertOne(document);
        System.out.println("DONE");
    }

}
