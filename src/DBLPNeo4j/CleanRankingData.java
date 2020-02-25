package DBLPNeo4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class CleanRankingData {
	private static String destinationDbPath = "C:\\Users\\prans\\Documents\\capstone\\dblp_dataset\\neo4j\\dblp\\";
	private static GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( new File(destinationDbPath) );
	
	public static Boolean checkIfConferenceAcronymMatches(String conferenceAcronym) {
		Boolean conferenceAcronymMatches = null;

		String checkIfAcronymExistsQuery = 
				"MATCH (r:Paper) " + 
				"WHERE r.paper_key contains $conferenceAcronym " + 
				"RETURN count(r) > 0 as acronym_exists";
		HashMap<String, Object> params = new HashMap();
		conferenceAcronym = "/"+conferenceAcronym+"/";
		params.put("conferenceAcronym", conferenceAcronym);
		Result result = db.execute(checkIfAcronymExistsQuery, params);

		if(result.hasNext())
			while(result.hasNext()) {
				Map<String, Object> row = result.next();
				conferenceAcronymMatches = (Boolean) row.get("acronym_exists");
			}
		
		return conferenceAcronymMatches;
	}
	
	public static void main(String[] args) {
		try {
			String coreRankingsFile = "C:/Users/prans/Documents/capstone/ground_truth_rankings/CORE/core_rankings_cs.csv";
			BufferedReader br = new BufferedReader(new FileReader(coreRankingsFile));
			String rankingEntry = br.readLine();
			ArrayList<String> conferenceAcronymsNotFound = new ArrayList();
			int conferenceRankGenerated = 0;
			
			String matchingCoreRankingsFile = "C:/Users/prans/Documents/capstone/ground_truth_rankings/CORE/cleaned_core_rankings_cs.csv"; 
			PrintWriter writer = new PrintWriter(new File(matchingCoreRankingsFile));
			int ctr = 0;
			
			while(rankingEntry != null) {
				String[] ranking = rankingEntry.split(",");
				int conferenceRank = Integer.parseInt(ranking[0]);
				String conferenceAcronym = ranking[1];
				String conferenceGrade = ranking[2];
				conferenceAcronym = conferenceAcronym.toLowerCase();
				boolean acronymExists = checkIfConferenceAcronymMatches(conferenceAcronym);
				if(acronymExists) {
					//write to a new file
					writer.write(++conferenceRankGenerated+","+conferenceRank+","+conferenceAcronym+","+conferenceGrade+"\n");
					System.out.println(conferenceRankGenerated+","+conferenceRank+","+conferenceAcronym+","+conferenceGrade);
					if(++ctr >= 100) {
						writer.flush();
						ctr =0;
					}
				}
				else {
					conferenceAcronymsNotFound.add(conferenceAcronym);
				}
				rankingEntry = br.readLine();
			}
			writer.close();
			System.out.println("Not Found Count: "+conferenceAcronymsNotFound.size());
			
		} catch (Exception e) {		
			e.printStackTrace();
		} finally {
			if (db != null) {
				db.shutdown();
			}
		}
		
		
	}

}
