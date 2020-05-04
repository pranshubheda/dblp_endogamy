package endogamy;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import scala.collection.immutable.RedBlackTree.EntriesIterator;

public class DmpDump {

	private static String destinationDbPath = "C:/Users/prans/Documents/capstone/co_author_network/neo4j_filtered/filtered_co_author_network/";
	private static File outputFile = new File("C:/Users/prans/Documents/capstone/co_author_network/filtered_dmp_dump.tsv");
	
//	private static String destinationDbPath = "C:/Users/prans/Documents/capstone/co_author_network/neo4j/co_author_network/";
//	private static File outputFile = new File("C:/Users/prans/Documents/capstone/co_author_network/unweighted_dmp_dump.tsv");

	
	private static GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(destinationDbPath))
			.setConfig(GraphDatabaseSettings.pagecache_memory, "2048M" )
			.setConfig(GraphDatabaseSettings.string_block_size, "60" )
			.setConfig(GraphDatabaseSettings.array_block_size, "300" )
			.newGraphDatabase();
	
	public static void queryNeo4j() {
		long startTime = System.nanoTime();
		try {
			try ( Transaction tx = db.beginTx()){
				File queryFile = new File("C:\\Users\\prans\\Documents\\capstone\\capstone_workspace\\dblpParser\\src\\endogamy\\dmp_degree_rank");
				String query = new String(new FileInputStream(queryFile).readAllBytes());
				System.out.println(query);
				Result result = db.execute(query);
				PrintWriter writer = new PrintWriter(outputFile);
				
				HashMap<Long, String> output = new HashMap();
				
				//node rank based on degree
				long degreeRankCounter = 1;
				long size = 0;
				while(result.hasNext()) {
					Map<String, Object> row = result.next();
					long id = (Long) row.get("id");
					long degree = (Long) row.get("degree");
					degreeRankCounter++;
					String outputString = id+"\t"+degree+"\t"+degreeRankCounter;
					output.put(id, outputString);
				}
				
				//node rank based on kcore
				long kCoreRankCounter = 1;
				queryFile = new File("C:\\Users\\prans\\Documents\\capstone\\capstone_workspace\\dblpParser\\src\\endogamy\\dmp_kcore_rank");
				query = new String(new FileInputStream(queryFile).readAllBytes());
				System.out.println(query);
				result = db.execute(query);
				
				while(result.hasNext()) {
					Map<String, Object> row = result.next();
					long id = (Long) row.get("id");
					long kClass = (Long) row.get("kClass");
					degreeRankCounter++;
					String outputString = output.get(id); 
					outputString = outputString+"\t"+kClass+"\t"+kCoreRankCounter;
					output.put(id, outputString);
					kCoreRankCounter++;
				}
				
				HashMap<Long, Double> dmpScores = new HashMap();
				
				Iterator<String> outputIterator = output.values().iterator();
				while(outputIterator.hasNext()) {
					String outputLine = outputIterator.next();
					List<Long> outputData = Arrays.stream(outputLine.split("\t")).map(Long::valueOf).collect(Collectors.toList());
					double dmp = Math.log(outputData.get(2)) - Math.log(outputData.get(4));
					dmp = Math.abs(dmp);
					outputLine = outputLine+"\t"+dmp;
					System.out.println(outputLine);
					dmpScores.put(outputData.get(0), dmp);
					output.put(outputData.get(0), outputLine);
				}
				
				ArrayList<Map.Entry<Long, Double>> sortedDmpScores = sort(dmpScores);
				
				for(Map.Entry<Long, Double> entry : sortedDmpScores) {
					String outputLine = output.get(entry.getKey());
					writer.write(outputLine+"\n");
					System.out.println(outputLine);
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
	
	
	public static ArrayList sort(HashMap<Long, Double> map) {				
		ArrayList arr = new ArrayList<>();
	    for(Map.Entry<Long, Double> e: map.entrySet()) {
			arr.add(e);
		}

		Comparator<Map.Entry<Long, Double>> valueComparator = new Comparator<Map.Entry<Long, Double>>() {
            
            @Override
            public int compare(Map.Entry<Long, Double> e1, Map.Entry<Long, Double> e2) {
                Double v1 = e1.getValue();
                Double v2 = e2.getValue();
                return v1.compareTo(v2) * -1;
            }
	    };

		Collections.sort(arr, valueComparator);

		return arr;
	}
	
	public static void main(String[] args) {
		queryNeo4j();
	}
}
