package DBLPNeo4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

public class CoAuthorDump {
	
	private static String destinationDbPath = "C:\\Users\\prans\\Documents\\capstone\\dblp_dataset\\neo4j\\dblp\\";
	private static GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(destinationDbPath))
			.setConfig(GraphDatabaseSettings.pagecache_memory, "2048M" )
			.setConfig(GraphDatabaseSettings.string_block_size, "60" )
			.setConfig(GraphDatabaseSettings.array_block_size, "300" )
			.newGraphDatabase();
	
	public static void generateUnweightedNetwork() throws FileNotFoundException {
		File outputFile = new File("C:/Users/prans/Documents/capstone/co_author_network/unweighted_network.tsv");
		PrintWriter writer = new PrintWriter(outputFile);
		long startTime = System.nanoTime();
		long total = 2490000;
		long batchSize = 10000;
		long done = 2429998;
		long limit = done+batchSize;
		try ( Transaction tx = db.beginTx()){
			while(done < total) {
			String query = 
					"MATCH (p:Person)-[q:HasAuthored]-(r:Paper)-[q1:HasAuthored]-(p1:Person) " + 
					"WITH id(p)-25000000 as author_1, id(p1)-25000000 as author_2, r " + 
					"WHERE author_1 > $done and author_1 <= $limit " + 
					"RETURN author_1, author_2, size((r)-[:HasAuthored]-()) as author_count " + 
					"ORDER BY author_1;"; 
			HashMap<String, Object> params = new HashMap();
			params.put("limit", limit);
			params.put("done", done);
			Result result = db.execute(query, params);
			while(result.hasNext()) {
				Map<String, Object> row = result.next();
				Long author_1 = (Long)row.get("author_1");
				Long author_2 = (Long)row.get("author_2");
				Long author_count = (Long)row.get("author_count");
				writer.write(author_1+"\t"+author_2+"\t"+author_count+"\n");
			}
			done+=batchSize;
			limit = done+batchSize;
			writer.flush();
			System.out.println("Loaded "+done);
		}			
		} catch (Exception e) {		
			e.printStackTrace();
		} finally {
			if (db != null) {
				db.shutdown();
			}
		}
		writer.close();
	}
	
	public static void main(String[] args) throws FileNotFoundException {
		long startTime = System.nanoTime();
		generateUnweightedNetwork();
		long endTime = System.nanoTime();
		System.out.println("Finished execution in " + (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");
	}

}
