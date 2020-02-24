package DBLPNeo4j;

import java.io.File;
import java.io.FileInputStream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class QueryNeo4jDBLP {

	private static String destinationDbPath = "C:\\Users\\prans\\Documents\\capstone\\dblp_dataset\\neo4j\\dblp\\";
	private static GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( new File(destinationDbPath) );

	public static void queryNeo4j() {
		long startTime = System.nanoTime();
		try {
			try ( Transaction tx = db.beginTx()){
				File queryFile = new File("C:\\Users\\prans\\Documents\\capstone\\capstone_workspace\\dblpParser\\src\\DBLPNeo4j\\test_query.cql");
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
		queryNeo4j();
	}
}
