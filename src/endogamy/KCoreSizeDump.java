package endogamy;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

public class KCoreSizeDump {

	private static String destinationDbPath = "C:/Users/prans/Documents/capstone/co_author_network/neo4j_filtered/filtered_co_author_network/";
	private static File outputFile = new File("C:/Users/prans/Documents/capstone/co_author_network/filtered_kcore_size_dump.tsv");

//	private static String destinationDbPath = "C:/Users/prans/Documents/capstone/co_author_network/neo4j/co_author_network/";
//	private static File outputFile = new File("C:/Users/prans/Documents/capstone/co_author_network/unweighted_kcore_size_dump.tsv");

	private static GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(destinationDbPath))
			.setConfig(GraphDatabaseSettings.pagecache_memory, "2048M" )
			.setConfig(GraphDatabaseSettings.string_block_size, "60" )
			.setConfig(GraphDatabaseSettings.array_block_size, "300" )
			.newGraphDatabase();
	
	public static void queryNeo4j() {
		long startTime = System.nanoTime();
		try {
			try ( Transaction tx = db.beginTx()){
				String query = 
						"MATCH (p:author) " + 
						"WITH p.kClass as kClass, count(*) as size " + 
						"RETURN kClass, size " + 
						"order by kClass desc;";
				Result result = db.execute(query);
				PrintWriter writer = new PrintWriter(outputFile);
				
				long size = 0;
				while(result.hasNext()) {
					Map<String, Object> row = result.next();
					size = size + (Long) row.get("size");
					long kClass = (Long) row.get("kClass");
					writer.write(kClass+"\t"+size+"\n");
				}

				writer.close();
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
