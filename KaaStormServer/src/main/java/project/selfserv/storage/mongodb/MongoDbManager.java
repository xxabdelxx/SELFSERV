package project.selfserv.storage.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import com.mongodb.MongoClient; 
import com.mongodb.MongoCredential;  
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import org.apache.logging.log4j.LogManager;


public class MongoDbManager {

	// Creating a Mongo client 
    private MongoClient mongo = new MongoClient( "192.168.100.210" , 27017 ); 
	// Creating Credentials 
    private MongoCredential credential;
    private MongoDatabase database;
    private static final Logger logger = LogManager.getLogger(MongoDbManager.class);
    
    public static MongoDbManager MongoDbManagerInstance = new MongoDbManager();
    
    public MongoClient getMongo() {
		return mongo;
	}

	public MongoCredential getCredential() {
		return credential;
	}

	public MongoDatabase getDatabase() {
		return database;
	}

	public MongoDbManager() {
		// TODO Auto-generated constructor stub    
	}
	
	public boolean startMongoDb()
	{
		credential = MongoCredential.createCredential("selfservteam", "selfservdb","selfserv123".toCharArray()); 
		// Accessing the database 
		database = mongo.getDatabase("selfservdb");
		if(database.getName() ==  null) {
			logger.error("##############################  CONNECT TO DATABASE : ERROR  #####################");
			return false;
		}
		return true;
	}
	
	public void createCollection(String CollectionName)
	{
		  //Creating a collection 
	      database.createCollection(CollectionName); 
	      logger.info("Collection "+CollectionName+" Created successfully");
	}
	
	public void insertDocument(String CollectionName,Document document)
	{
		// Retrieving a collection
	      MongoCollection<Document> collection = database.getCollection(CollectionName); 
	      logger.info("Collection "+CollectionName+" selected successfully");
	      collection.insertOne(document); 
	      logger.info("###############   INSERT DOCUMENT : OK   ##############");   
	}
	
   

}
