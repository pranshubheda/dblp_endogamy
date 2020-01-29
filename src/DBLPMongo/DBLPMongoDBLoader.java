package DBLPMongo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;

public class DBLPMongoDBLoader {
	private static Connection sourceDBLPConnection;
	private static MongoDatabase destinationMongoDBLPDB;
	private static MongoClient mongoClient = new MongoClient();

	private static Connection getDBConnection(String url, String username, String password) throws Exception {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(url, username, password);
		} catch (SQLException e) {
			System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
			e.printStackTrace();
		}
		
		if (conn != null) {
			System.out.println("Connected to the database");
		}
		else {
			throw new Exception("Failed to make database connection!");
		}
		
		return conn;
	}
	
	private static void migratePaperRecords() throws SQLException {
		long startTime = System.nanoTime();
		System.out.println("Started fetching paper");
		
		String fetchPaperSql = ""
				+ "select *, sha1(paper_key) as paper_key_id "
				+ "from paper;";
		
		PreparedStatement fetchPaperPs = sourceDBLPConnection.prepareStatement(fetchPaperSql);
		ResultSet fetchPaperRS = fetchPaperPs.executeQuery();
		
		long endTime = System.nanoTime();
		System.out.println("Finished fetching paper in "+ (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");	
		
		startTime = System.nanoTime();
		System.out.println("Started inserting paper");
		List<InsertOneModel<Document>> paperDocuments = new ArrayList<>();
		while(fetchPaperRS.next()) {
			Document paperDocument = new Document();
			paperDocument.append("_id", fetchPaperRS.getString("paper_key_id"));
			paperDocument.append("paper_key", fetchPaperRS.getString("paper_key"));
			paperDocument.append("conference", fetchPaperRS.getString("conference"));
			paperDocument.append("title", fetchPaperRS.getString("title"));
			paperDocument.append("year", fetchPaperRS.getInt("year"));
			paperDocuments.add(new InsertOneModel<>(paperDocument));
		}
		fetchPaperPs.close();
		MongoCollection<Document> personCollection = destinationMongoDBLPDB.getCollection("Paper");
		personCollection.bulkWrite(paperDocuments, new BulkWriteOptions().ordered(false));
		endTime = System.nanoTime();
		System.out.println("Finished inserting paper in "+ (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");
	}

	private static void migratePersonRecords() throws SQLException {
		long startTime = System.nanoTime();
		System.out.println("Started fetching person");
		
		String fetchPersonSql = ""
				+ "select sha1(name) as person_name_hash_id, primary_alias "
				+ "from person;";
		
		PreparedStatement fetchPersonPs = sourceDBLPConnection.prepareStatement(fetchPersonSql);
		ResultSet fetchPersonRS = fetchPersonPs.executeQuery();
		
		long endTime = System.nanoTime();
		System.out.println("Finished fetching person in "+ (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");	
		
		startTime = System.nanoTime();
		System.out.println("Started inserting person");
		List<InsertOneModel<Document>> personDocuments = new ArrayList<>();
		while(fetchPersonRS.next()) {
			Document personDocument = new Document();
			personDocument.append("_id", fetchPersonRS.getString("person_name_hash_id"));
			personDocument.append("primary_alias", fetchPersonRS.getInt("primary_alias"));
			personDocuments.add(new InsertOneModel<>(personDocument));
		}
		fetchPersonPs.close();
		MongoCollection<Document> personCollection = destinationMongoDBLPDB.getCollection("Person");
		personCollection.bulkWrite(personDocuments, new BulkWriteOptions().ordered(false));
		endTime = System.nanoTime();
		System.out.println("Finished inserting person in "+ (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");
	}
	
	private static void updatePersonPaperRecords() throws SQLException {
		long startTime = System.nanoTime();
		System.out.println("Started fetching person-paper info from Author");
		
		long updated = 0;
		long max = 8017609;
		long pageSize = 1000000;
		
		while (updated < max) {
			String fetchPersonPaperSql = ""
					+ "select sha1(name) as person_name_hash_id, paper_id, author_rank "
					+ "from author "
					+ "limit ?, ?;";
			
			PreparedStatement fetchPersonPaperPs = sourceDBLPConnection.prepareStatement(fetchPersonPaperSql);
			fetchPersonPaperPs.setLong(1, updated);
			fetchPersonPaperPs.setLong(2, pageSize);
			ResultSet fetchPersonPaperRS = fetchPersonPaperPs.executeQuery();
			
			long endTime = System.nanoTime();
			System.out.println("Finished fetching person-paper in "+ (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");	
			
			startTime = System.nanoTime();
			System.out.println("Started inserting person-paper");
			List<UpdateOneModel<Document>> personPaperDocuments = new ArrayList<>();
			while(fetchPersonPaperRS.next()) {
				Document filterDocument = new Document();
				filterDocument.append("_id", fetchPersonPaperRS.getString("person_name_hash_id"));
				
				
				Document authoredPaperDocument = new Document("paper_id", fetchPersonPaperRS.getInt("paper_id"));
				authoredPaperDocument.put("author_rank", fetchPersonPaperRS.getInt("author_rank"));
				
				BasicDBObject updateDocument = new BasicDBObject(); 
				updateDocument.put("$push", new BasicDBObject("authored_papers", authoredPaperDocument));
				
				personPaperDocuments.add(new UpdateOneModel<Document>(filterDocument, updateDocument));
			}
			fetchPersonPaperPs.close();
			MongoCollection<Document> personCollection = destinationMongoDBLPDB.getCollection("Person");
			personCollection.bulkWrite(personPaperDocuments, new BulkWriteOptions().ordered(false));
			updated+=pageSize;
		}
		
		long endTime = System.nanoTime();
		System.out.println("Finished inserting person-paper in "+ (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");

	}
	
	public static void main(String[] args) throws SQLException {
		
		try {
			String sourceDBLPUrl = "jdbc:mysql://127.0.0.1:3306/dblp";
			String sourceDBLPUsername = "root";
			String sourceDBLPPassword = "root";
			sourceDBLPConnection = getDBConnection(sourceDBLPUrl, sourceDBLPUsername, sourceDBLPPassword);
			destinationMongoDBLPDB =  mongoClient.getDatabase("DBLP");
			migratePersonRecords();
			updatePersonPaperRecords();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			if(sourceDBLPConnection!=null)
				sourceDBLPConnection.close();
		}

	}

}
