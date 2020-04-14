package DBLPNeo4j;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

public class KCore {

	private static String destinationDbPath = "C:/Users/prans/Documents/capstone/co_author_network/neo4j/co_author_network/";
	private static GraphDatabaseService mainDb;
	private static String auxillaryDbPath = "C:/Users/prans/Documents/capstone/co_author_network/neo4j_auxillary/auxillary_db/";
	private static GraphDatabaseService auxDb;

	private static void initailizeDb() {
		mainDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(destinationDbPath))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "2048M" )
				.setConfig(GraphDatabaseSettings.string_block_size, "60" )
				.setConfig(GraphDatabaseSettings.array_block_size, "300" )
				.newGraphDatabase();
		
		auxDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(auxillaryDbPath))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "2048M" )
				.setConfig(GraphDatabaseSettings.string_block_size, "60" )
				.setConfig(GraphDatabaseSettings.array_block_size, "300" )
				.newGraphDatabase();
	}
	
	public static void updateNodesWithGivenKClass(ArrayList<Long> deletedNodes, Long kClass) {
		try(Transaction tx = mainDb.beginTx()) {
			for(Long node : deletedNodes) {
				mainDb.getNodeById(node).setProperty("kClass", kClass);
			}
			tx.success();
			tx.close();
		}
	}
	
	public static ArrayList<Long> deleteNodesWithGivenKClass(Long kClass) {
		String query = 
				"match (p:author) " + 
				"with p, size((p)-[:co_authored]-()) as degree " + 
				"where degree <= $kClass " + 
				"detach delete p " +
				"return id(p) as deleted_node_id;";
		HashMap<String, Object> params = new HashMap();
		params.put("kClass", kClass);
		ArrayList<Long> deletedNodes = new ArrayList<>();
		Result result = auxDb.execute(query, params);
		while(result.hasNext()) {
			Map<String, Object> row = result.next();
			long deletedNodeId = (Long)row.get("deleted_node_id");
			deletedNodes.add(deletedNodeId);
		}
		return deletedNodes;
	}
	
	public static long fetchNodeCount() {
		String query = 
					"match (p:author) " + 
					"return count(p) as node_count;";
		Result result = auxDb.execute(query);
		Map<String, Object> row = result.next();
		Long nodesCount = (Long)row.get("node_count");
		return nodesCount;
	}
	
	public static void main(String[] args) {
		try {
			initailizeDb();
			//fetch nodes with degree < given from aux db
			long kClass = 0;
			long nodesCount = fetchNodeCount();
			while(nodesCount > 0) {
				//remove the nodes from aux db
				ArrayList<Long> deletedNodes = deleteNodesWithGivenKClass(kClass);
				//update the property values for these nodes in main db or write in file
				updateNodesWithGivenKClass(deletedNodes, kClass);
				nodesCount = fetchNodeCount();
				System.out.println(deletedNodes.size()+"\t"+kClass+"\t"+nodesCount);
				kClass++;
			}
		}catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(mainDb!=null)
				mainDb.shutdown();
			if(auxDb!=null)
				auxDb.shutdown();
		}
	}

}
