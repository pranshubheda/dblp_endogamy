package dataLoading;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.bson.Document;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;

public class LoadPublicationNetwork {
	private static MongoClient mongoClient = new MongoClient();
	private static MongoDatabase sourceMongoDb;
	private static String destinationDbPath;
	private static BatchInserter inserter;
	private static HashMap<String, Long> movieGenreNodeIdMap = new HashMap<String, Long>();
	private static int personCounterSeed = 25000000;
	private static GraphDatabaseService db;
	
	private static void migratePapersToNeo4j() throws SQLException {
		long startTime = System.nanoTime();
		System.out.println("Started fetching paper");

		Connection conn = DBConnection.getConn();
		String fetchPaperSql = ""
				+ "select * "
				+ "from paper;";
		PreparedStatement fetchPapersPS = conn.prepareStatement(fetchPaperSql);
		ResultSet fetchPaperRS = fetchPapersPS.executeQuery();
		
		long endTime = System.nanoTime();
		System.out.println("Finished fetching paper in "+ (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");	
		
		startTime = System.nanoTime();
		System.out.println("Started inserting paper");
		while(fetchPaperRS.next()) {
			HashMap<String, Object> paperAttributes = new HashMap<>();
			int paperId = fetchPaperRS.getInt("id");
			int year = fetchPaperRS.getInt("year");
			paperAttributes.put("paper_key", fetchPaperRS.getString("paper_key"));
			paperAttributes.put("year", fetchPaperRS.getInt("year"));
			inserter.createNode(paperId, paperAttributes, Label.label("Paper"));
		}

		fetchPapersPS.close();
		endTime = System.nanoTime();
		System.out.println("Finished inserting paper in "+ (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");

	}

	private static void migratePersonToNeo4j() throws SQLException {
		long startTime = System.nanoTime();
		System.out.println("Started fetching person");

		Connection conn = DBConnection.getConn();
		String fetchPersonSql = ""
				+ "select distinct primary_alias  "
				+ "from person;";
		PreparedStatement fetchPersonPS = conn.prepareStatement(fetchPersonSql);
		ResultSet fetchPersonRS = fetchPersonPS.executeQuery();
		
		long endTime = System.nanoTime();
		System.out.println("Finished fetching person in "+ (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");	
		
		startTime = System.nanoTime();
		System.out.println("Started inserting person");
		while(fetchPersonRS.next()) {
			HashMap<String, Object> personAttributes = new HashMap<>();
			int personId = personCounterSeed + fetchPersonRS.getInt("primary_alias");
			inserter.createNode(personId, personAttributes, Label.label("Person"));
		}

		fetchPersonPS.close();
		endTime = System.nanoTime();
		System.out.println("Finished inserting person in "+ (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");

	}

	private static void createAuthorPaperRealtionship() {
		// fetch data from mongodb
		MongoCollection<Document> personCollection = sourceMongoDb.getCollection("Person");
		FindIterable<Document> personDocuments = personCollection.find();
		int count = 0;

		for (Document person : personDocuments) {
			HashMap<String, Object> attributesPerson = new HashMap<>();
			int personNodeId = personCounterSeed + person.getInteger("primary_alias");

			ArrayList<Document> authoredPapers = (ArrayList<Document>) person.get("authored_papers");
			if (authoredPapers != null) {
				for (Document authoredPaper : authoredPapers) {
					int paperNodeId = authoredPaper.getInteger("paper_id");
					int rank = authoredPaper.getInteger("author_rank");
					HashMap<String, Object> authoredRelationAttr = new HashMap<>();
					authoredRelationAttr.put("rank", rank);
					inserter.createRelationship(personNodeId, paperNodeId, RelationshipType.withName("HasAuthored"), authoredRelationAttr);
//					System.out.printf("Creating relation %d HasAuthored %d with rank %d \n", personNodeId, paperNodeId, rank);
				}
			}
		}		
	}

	private static void updatePersonNodes() throws SQLException {
		long startTime = System.nanoTime();
		System.out.println("Started fetching person");

		Connection conn = DBConnection.getConn();
		
		String fetchPersonSql = ""
				+ "select p1.id as primary_alias, p1.name as primary_alias_name "
				+ "from person p1 "
				+ "where p1.id = p1.primary_alias;";
		
		PreparedStatement fetchPersonPS = conn.prepareStatement(fetchPersonSql);
		ResultSet fetchPersonRS = fetchPersonPS.executeQuery();
		
		long endTime = System.nanoTime();
		System.out.println("Finished fetching person in "+ (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");	
		
		startTime = System.nanoTime();
		System.out.println("Started updating person");
		while(fetchPersonRS.next()) {
			HashMap<String, Object> personAttributes = new HashMap<>();
			int personId = personCounterSeed + fetchPersonRS.getInt("primary_alias");
			inserter.setNodeProperty(personId, "primary_alias_name", fetchPersonRS.getString("primary_alias_name"));
		}

		fetchPersonPS.close();
		endTime = System.nanoTime();
		System.out.println("Finished updating person in "+ (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");
	}

	public static void queryNeo4j() {
		long startTime = System.nanoTime();
		try {
			db = new GraphDatabaseFactory().newEmbeddedDatabase( new File(destinationDbPath) );
			try ( Transaction tx = db.beginTx()){
				File queryFile = new File("C:\\Users\\prans\\Documents\\capstone\\capstone_workspace\\dblpParser\\src\\endogamy\\test_query.cql");
				String query = new String(new FileInputStream(queryFile).readAllBytes());
				System.out.println(query);
				Result result = db.execute(query);
				while(result.hasNext()) {
					System.out.println(result.resultAsString());
				}
			}			
		} catch (Exception e) {	
			e.printStackTrace();
		} finally {
			if (db != null) {
				db.shutdown();
			}
		}
		long endTime = System.nanoTime();
		System.out.println("Finished execution in "+ (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");
	}
	
	public static void main(String[] args) {
		try {
			String sourceDbName = "DBLP";
			sourceMongoDb = mongoClient.getDatabase(sourceDbName);
			destinationDbPath = "C:\\Users\\prans\\Documents\\capstone\\dblp_dataset\\neo4j\\dblp\\";
			long startTime = System.nanoTime();
			inserter = BatchInserters.inserter(new File(destinationDbPath));
			migratePapersToNeo4j();
			migratePersonToNeo4j();
			createAuthorPaperRealtionship();
			updatePersonNodes();
//			queryNeo4j();
			long endTime = System.nanoTime();
			System.out.println("Finished migration in "+ (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (inserter != null) {
				inserter.shutdown();
			}
			if (mongoClient != null) {
				mongoClient.close();
			}
		}
	}

}
