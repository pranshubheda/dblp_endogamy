package DBLPNeo4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
	
	public static boolean validateConferenceGrade(String grade) {
		String[] validGrades = {"A*", "A", "B", "C"};
		HashSet<String> setOfValidGrades = new HashSet<>(Arrays.asList(validGrades));
		boolean valid = setOfValidGrades.contains(grade.strip());
		return valid; 
	}
	
	public static void validateMSARConferenceAcronyms() throws IOException {
		String msarRankingsFile = "C:/Users/prans/Documents/capstone/ground_truth_rankings/MSAR/msar_dm.csv";
		BufferedReader br = new BufferedReader(new FileReader(msarRankingsFile));
		String rankingEntry = br.readLine();
		while (rankingEntry!=null) {
			String[] rankingData = rankingEntry.split(",");
			String acronym = rankingData[2].strip().toLowerCase();
			System.out.printf(acronym+" "+checkIfConferenceAcronymMatches(acronym)+"\n");
			rankingEntry = br.readLine();
		}

	}
	
	public static void main(String[] args) {
		try {
			validateMSARConferenceAcronyms();
			
//			String coreRankingsFile = "C:/Users/prans/Documents/capstone/ground_truth_rankings/CORE/core_rankings_0801.csv";
//			BufferedReader br = new BufferedReader(new FileReader(coreRankingsFile));
//			String rankingEntry = br.readLine();
//			ArrayList<String> conferenceAcronymsNotFound = new ArrayList();
//			int conferenceRankGenerated = 0;
//			HashMap<String, Integer> numericGrade = new HashMap();
//			numericGrade.put("A*", 1001);
//			numericGrade.put("A", 1002);
//			numericGrade.put("B", 1003);
//			numericGrade.put("C", 1004);
// 			
//			String matchingCoreRankingsFile = "C:/Users/prans/Documents/capstone/ground_truth_rankings/CORE/cleaned_core_rankings_0801.csv"; 
//			PrintWriter writer = new PrintWriter(new File(matchingCoreRankingsFile));
//			int ctr = 0;
//			
//			while(rankingEntry != null) {
//				String[] ranking = rankingEntry.split(",");
//				int conferenceRank = Integer.parseInt(ranking[0]);
//				String conferenceAcronym = ranking[2];
//				String conferenceGrade = ranking[4];
//				System.out.println(conferenceRank);
//				if (validateConferenceGrade(conferenceGrade)) {
//					conferenceAcronym = conferenceAcronym.toLowerCase();
//					boolean acronymExists = checkIfConferenceAcronymMatches(conferenceAcronym);
//					if(acronymExists) {
//						//write to a new file
//						writer.write(++conferenceRankGenerated+","+conferenceRank+","+conferenceAcronym+","+numericGrade.get(conferenceGrade)+"\n");
////						System.out.println(conferenceRankGenerated+","+conferenceRank+","+conferenceAcronym+","+numericGrade.get(conferenceGrade));
//						if(++ctr >= 100) {
//							writer.flush();
//							ctr =0;
//						}
//					}
//					else {
//						conferenceAcronymsNotFound.add(conferenceAcronym);
//					}
//				}
//				rankingEntry = br.readLine();
//			}
//			writer.close();
//			System.out.println("Not Found Count: "+conferenceAcronymsNotFound.size());
			
		} catch (Exception e) {		
			e.printStackTrace();
		} finally {
			if (db != null) {
				db.shutdown();
			}
		}
		
		
	}

}
