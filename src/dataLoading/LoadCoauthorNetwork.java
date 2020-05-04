package dataLoading;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class LoadCoauthorNetwork {
	
//	private static String destinationDbPath = "C:/Users/prans/Documents/capstone/co_author_network/neo4j_filtered/filtered_co_author_network/";
//	private static String destinationDbPath = "C:/Users/prans/Documents/capstone/co_author_network/neo4j/co_author_network/";
	private static String destinationDbPath = "C:/Users/prans/Documents/capstone/co_author_network/neo4j_auxillary/auxillary_db/";
	private static BatchInserter inserter;
	private static GraphDatabaseService db;
	
	public static void main(String[] args) throws IOException {
		try {
			long startTime = System.nanoTime();
			long previouslyInsertedNodeCount = 0;
			//create dataLoading
			inserter = BatchInserters.inserter(new File(destinationDbPath));
			//get files from folder
			File coAuthorNetwork = new File("C:/Users/prans/Documents/capstone/co_author_network/unweighted_network_new.tsv");
			FileInputStream fis = new FileInputStream(coAuthorNetwork);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line = br.readLine();
			//create nodes
			for (int i=1; i<=2487238; i++) {
				Map<String, Object> nodeAttributes = new HashMap<>();
				Label labels[] = new Label[1];
				labels[0] = Label.label("author");
				inserter.createNode(i, nodeAttributes, labels);
			}

			//create relationships
			long previousAuthor1 = -1;
			HashSet<Long> duplicateCheck = null;
			while (line != null) {
				String[] inputLine = line.split("\t");
				Long author1 = Long.parseLong(inputLine[0]);
				Long author2 = Long.parseLong(inputLine[1]);
				Long authorCount = Long.parseLong(inputLine[2]);
				if(previousAuthor1 != author1) {
					duplicateCheck = new HashSet<Long>();
				}
				if(author1 < author2 && authorCount >= 2 && authorCount <=15 && !duplicateCheck.contains(author2)) {
					inserter.createRelationship(author1, author2, RelationshipType.withName("co_authored"), null);
					duplicateCheck.add(author2);
				}
				line = br.readLine();
				previousAuthor1 = author1;
			}
			long endTime = System.nanoTime();
			System.out.println("Finished loading in "+ (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			if (inserter != null) {
				inserter.shutdown();
			}
		}

	}
}
