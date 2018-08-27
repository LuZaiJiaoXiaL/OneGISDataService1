package DBBasicUtil;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Calendar;


public class MongoDBUtil {
	public static String URL = "192.168.7.131";//192.168.7.128
    private MongoClient mongoClient = null;

	public MongoDBUtil(String URL){
         MongoClient Client = new MongoClient(URL, 27017);
         this.mongoClient=Client;
    }

	public  MongoClient getClient() {
		return mongoClient;
	}

	public  MongoDatabase getDatabase(String Database) {
		MongoDatabase mongoDatabase = mongoClient.getDatabase(Database);
		return mongoDatabase;
	}

	public  MongoCollection<Document> getCollection(String Database,String Collection) {
		MongoDatabase mongoDatabase = mongoClient.getDatabase(Database);
		MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(Collection);
		return mongoCollection;
	}
	
	public  MongoCollection<DBObject> getDBObjectCollection(String Database,String Collection) {
		MongoDatabase mongoDatabase = mongoClient.getDatabase(Database);
		MongoCollection<DBObject> mongoCollection = mongoDatabase.getCollection(Collection,DBObject.class);
		return mongoCollection;
	}

	public void DeleteCollection(String Database,String Collection) {
		MongoDatabase mongoDatabase = mongoClient.getDatabase(Database);
		MongoCollection<DBObject> collection = mongoDatabase.getCollection(Collection, DBObject.class);
		collection.drop();
		System.out.println("数据表删除成功！");
	}
	
	public  void SpatioTemporalQuery(double taskLon, double taskLat, double taskWidth, double taskHeight,
			Calendar calendarMIN, Calendar calendarMAX, Calendar calendar, int scalePara, int TimeSearchRange) {

	}
	
	public void CollectionExist(String Database,String CollectionName) {
		MongoCollection mongoCollection=getCollection(Database,CollectionName);
		if(mongoCollection!=null) {
			mongoCollection.drop();
//			System.out.println("数据表删除成功！");
		}
	}

}
