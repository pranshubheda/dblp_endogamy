package DBLPNeo4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.bouncycastle.asn1.eac.BidirectionalMap;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import com.google.common.collect.Sets;

public class EndogamyScorer {
	private static String destinationDbPath = "C:\\Users\\prans\\Documents\\capstone\\dblp_dataset\\neo4j\\dblp\\";
	private static GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(destinationDbPath))
			.setConfig(GraphDatabaseSettings.pagecache_memory, "2048M" )
			.setConfig(GraphDatabaseSettings.string_block_size, "60" )
			.setConfig(GraphDatabaseSettings.array_block_size, "300" )
			.newGraphDatabase();

	public static void queryNeo4j() {
		long startTime = System.nanoTime();
		try {
			try ( Transaction tx = db.beginTx()){
//				File queryFile = new File("C:\\Users\\prans\\Documents\\capstone\\capstone_workspace\\dblpParser\\src\\DBLPNeo4j\\find_papers_with_multiple_authors.cql");
				File queryFile = new File("C:\\Users\\prans\\Documents\\capstone\\capstone_workspace\\dblpParser\\src\\DBLPNeo4j\\test_query.cql");
//				File queryFile = new File("C:\\Users\\prans\\Documents\\capstone\\capstone_workspace\\dblpParser\\src\\DBLPNeo4j\\intersection.cql");

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
	
	private static HashSet<Long> findAuthors(long paperId) {
		HashSet<Long> authors = null;
		try {
			try ( Transaction tx = db.beginTx()){
				String query = 
						"MATCH (author:Person)-[q:HasAuthored]-(paper:Paper) " + 
						"WHERE ID(paper) = $paperId " + 
						"RETURN ID(author) as author_id;";
				HashMap<String, Object> params = new HashMap();
				params.put("paperId", paperId);
				Result result = db.execute(query, params);
				authors = new HashSet<Long>();
				while(result.hasNext()) {
					Map<String, Object> row = result.next();
					Long authorId = (Long)row.get("author_id");
					authors.add(authorId);
				}
			}			
		} catch (Exception e) {		
			e.printStackTrace();
		}
		return authors;
	}

	public static Long calculateUnionCount(Set<Long> authorSet) {
		String unionCountQuery = 
				"MATCH (p:Person)-[q:HasAuthored]-(r:Paper) " + 
				"where Id(p) in $authorSet " + 
				"RETURN count(DISTINCT r) as unionCount;";
		HashMap<String, Object> params = new HashMap();
		params.put("authorSet", authorSet);
		Result result = db.execute(unionCountQuery, params);
		Long unionCount = null;
		while(result.hasNext()) {
			Map<String, Object> row = result.next();
			unionCount = (Long)row.get("unionCount");
		}
		return unionCount;
	}

	public static Long calculateUnionCount(Set<Long> authorSet, long year) {
		String unionCountQuery = 
				"MATCH (p:Person)-[q:HasAuthored]-(r:Paper) " + 
				"where Id(p) in $authorSet and r.year <= $year " + 
				"RETURN count(DISTINCT r) as unionCount;";
		HashMap<String, Object> params = new HashMap();
		params.put("authorSet", authorSet);
		params.put("year", year);
		Result result = db.execute(unionCountQuery, params);
		Long unionCount = null;
		while(result.hasNext()) {
			Map<String, Object> row = result.next();
			unionCount = (Long)row.get("unionCount");
		}
		return unionCount;
	}
	
	public static Long calculateIntersectionCount(Set<Long> authorSet) {
		String intersectionCountQuery = 
				"MATCH (p:Person)-[q:HasAuthored]-(r:Paper) " + 
				"where Id(p) in $authorSet " + 
				"with r, $authorSetCount as author_count, count(DISTINCT p) as count " + 
				"WHERE author_count = count " + 
				"RETURN count(r) as intersectionCount;";
		HashMap<String, Object> params = new HashMap();
		params.put("authorSet", authorSet);
		params.put("authorSetCount", authorSet.size());

		Result result = db.execute(intersectionCountQuery, params);
		Long intersectionCount = null;
		while(result.hasNext()) {
			Map<String, Object> row = result.next();
			intersectionCount = (Long)row.get("intersectionCount");
		}
		return intersectionCount;
	}
	
	public static Long calculateIntersectionCount(Set<Long> authorSet, long year) {
		String intersectionCountQuery = 
				"MATCH (p:Person)-[q:HasAuthored]-(r:Paper) " + 
				"where Id(p) in $authorSet and r.year <= $year " + 
				"with r, $authorSetCount as author_count, count(DISTINCT p) as count " + 
				"WHERE author_count = count " + 
				"RETURN count(r) as intersectionCount;";
		HashMap<String, Object> params = new HashMap();
		params.put("authorSet", authorSet);
		params.put("authorSetCount", authorSet.size());
		params.put("year", year);

		Result result = db.execute(intersectionCountQuery, params);
		Long intersectionCount = null;
		while(result.hasNext()) {
			Map<String, Object> row = result.next();
			intersectionCount = (Long)row.get("intersectionCount");
		}
		return intersectionCount;
	}
	
	public static double calculateEndogamyScore(Set<Long> authorSet) {
		//calculate cardinality of union
		Double unionCount = calculateUnionCount(authorSet).doubleValue();
		//calculate the cardinality of intersection
		Double intersectionCount = calculateIntersectionCount(authorSet).doubleValue();
		//score
		double endogamyScore =  intersectionCount/unionCount;
		return endogamyScore;
	}

	public static double calculateEndogamyScore(Set<Long> authorSet, long year) {
		//calculate cardinality of union
		Double unionCount = calculateUnionCount(authorSet, year).doubleValue();
		//calculate the cardinality of intersection
		Double intersectionCount = calculateIntersectionCount(authorSet, year).doubleValue();
		//score
		double endogamyScore =  intersectionCount/unionCount;
		return endogamyScore;
	}
	
	public static double findEndogamyScoreOfPaper(long paperId, HashSet<Long> authors) {
		Set<Set<Long>> powerSetOfAuthors = Sets.powerSet(authors);
		double maxEndogamyScore = 0;
		for (Set<Long> authorSet : powerSetOfAuthors) {
			//find the endogamy score of each authorSet
			if (authorSet.size() > 1) {
				double endogamyScore = calculateEndogamyScore(authorSet);
				if(endogamyScore > maxEndogamyScore)
					maxEndogamyScore = endogamyScore;
			}
		}
		return maxEndogamyScore;
	}
	
	public static double findEndogamyScoreOfPaper(long paperId, long year) {
		HashSet<Long> authors = findAuthors(paperId);
		Set<Set<Long>> powerSetOfAuthors = Sets.powerSet(authors);
		double maxEndogamyScore = 0;
		double totalEndogamyScore = 0;
		long counter = 0;
		for (Set<Long> authorSet : powerSetOfAuthors) {
			//find the endogamy score of each authorSet
			if (authorSet.size() > 1) {
				counter++;
				double endogamyScore = calculateEndogamyScore(authorSet, year);
				totalEndogamyScore+=endogamyScore;
				if(endogamyScore > maxEndogamyScore)
					maxEndogamyScore = endogamyScore;
				System.out.println(authorSet + "-> "+Math.round(endogamyScore * 10000.0)/10000.0);
			}
		}
		double averageEndogamyScore = totalEndogamyScore/counter;
		return averageEndogamyScore;
//		return maxEndogamyScore;
	}
	
	public static HashSet<Long> findPapersOfConference(String conferenceAcronym) {
		HashSet<Long> paperIds = null;
		String findPapersOfConferenceQuery = 
				"MATCH (p:Person)-[q:HasAuthored]-(r:Paper) " + 
				"where r.paper_key CONTAINS $conferenceAcronym " + 
				"RETURN ID(r) as paperId;";
		HashMap<String, Object> params = new HashMap();
		params.put("conferenceAcronym", "/"+conferenceAcronym+"/");
		Result result = db.execute(findPapersOfConferenceQuery, params);
		Long intersectionCount = null;
		if(result.hasNext())
			paperIds = new HashSet();
		while(result.hasNext()) {
			Map<String, Object> row = result.next();
			long paperId = (Long)row.get("paperId");
			paperIds.add(paperId);
		}
		return paperIds;
	}
	
	public static double findEndogamyScoreOfConference(String conferenceAcronym) {
		double sumOfEndoScoreOfPapersPublished = 0;
		//find papers in this conference
		HashSet<Long> paperIds = findPapersOfConference(conferenceAcronym);
		//for each paper find endogamy score
		ArrayList<Long> ignoredPaperIds = new ArrayList();
		for (Long paperId : paperIds) {
			HashSet<Long> authors = findAuthors(paperId);
			if (authors.size() <= 10) {
				double endogamyScoreOfPaper = findEndogamyScoreOfPaper(paperId, authors);
				sumOfEndoScoreOfPapersPublished += endogamyScoreOfPaper;				
			}
			else {
				ignoredPaperIds.add(paperId);
			}
		}
		//find the average of all papers as the endogamy score.
		double endogamyScoreOfConference = sumOfEndoScoreOfPapersPublished/paperIds.size();
//		System.out.printf("Ignored Papers Count: %s Paper Ids: %s\n", ignoredPaperIds.size(), ignoredPaperIds);
		return endogamyScoreOfConference;
	}
	
	public static List<Entry<String, Double>> sortMap(HashMap<String, Double> conferenceRanks) {
	    List<Entry<String, Double>> elements = new LinkedList(conferenceRanks.entrySet());
	    Collections.sort(elements, new Comparator<Entry<String, Double>>() {

	        public int compare(Entry<String, Double> o1, Entry<String, Double> o2 ) {
	            return o1.getValue().compareTo(o2.getValue());
	        }

	    });
	    
	    return elements;
	}
	
	public static HashMap<String, Integer> getGroundTruthRankings() throws IOException{
		HashMap<String, Integer> groundTruthConferenceRanks = new HashMap();
		String cleanedCoreRankingsFile = "C:/Users/prans/Documents/capstone/ground_truth_rankings/CORE/cleaned_core_rankings.csv";
        BufferedReader br = new BufferedReader(new FileReader(cleanedCoreRankingsFile));
        String rankingEntry = br.readLine();
        while(rankingEntry != null) {
        	String[] ranking = rankingEntry.split(",");
        	int conferenceRank = Integer.parseInt(ranking[0]);
        	String conferenceAcronym = ranking[2];
        	groundTruthConferenceRanks.put(conferenceAcronym, conferenceRank);
        }
        return groundTruthConferenceRanks;
	}

//	public static Double getConformanceScoreOfGeneratedRankings(BidirectionalMap groundTruthConferenceRanks, HashMap<String, Double> generatedConferenceRanks) {
//		int p = 0, f = 0;
//		//for each combination check conditions and remember count
//		for(int i=0; i<groundTruthConferenceRanks.size(); i++) {
//			int conference1Rank = i+1;
//			String conference1Acronym = (String) groundTruthConferenceRanks.get(conference1Rank);
//			for(int j=i+1; j<groundTruthConferenceRanks.size()+1; j++) {
//				int conference2Rank = j+1;
//				String conference2Acronym = (String) groundTruthConferenceRanks.get(conference2Rank);
//				if(generatedConferenceRanks.get(conference1Acronym) > generatedConferenceRanks.get(conference2Acronym))
//					p++;
//				else
//					f++;
//			}
//		}
//		
//		double conformanceScore = 100 * (p/p+f);
//		return conformanceScore;
//	}
	
	public static Double getConformanceScoreOfGeneratedRankings(HashMap<String, Integer> groundTruthConferenceRanks, List<Entry<String, Double>> sortedConferenceScores) {
		int p = 0, f = 0;
		//for each combination check conditions and remember count
		for(int i=0; i<sortedConferenceScores.size(); i++) {
			Entry<String, Double> conference1Entry = sortedConferenceScores.get(i);
			String conference1Acronym = conference1Entry.getKey();
			double conference1Score = conference1Entry.getValue();
			int conference1Ranking = groundTruthConferenceRanks.get(conference1Acronym);
			
			for(int j=i+1; j<sortedConferenceScores.size()+1; j++) {
				Entry<String, Double> conference2Entry = sortedConferenceScores.get(j);
				String conference2Acronym = conference2Entry.getKey();
				double conference2Score = conference2Entry.getValue();
				int conference2Ranking = groundTruthConferenceRanks.get(conference2Acronym);
				System.out.printf("Comparing %s and %s \n", conference1Acronym, conference2Acronym);
				System.out.printf("Score %s and %s \n", conference1Score, conference2Score);
				System.out.printf("Ranking %s and %s \n", conference1Ranking, conference2Ranking);
				
				if(conference1Ranking > conference2Ranking)
					p++;
				else
					f++;
			}
		}
		
		double conformanceScore = 100 * (p/p+f);
		return conformanceScore;
	}
	
	public static void main(String[] args) throws IOException {
		long startTime = System.nanoTime();
//		String[] conferencesToCheck = {"pods", "isit", "edbt", "icde", "pkdd", "sigmod", "mdm", "asiacrypt", "dasfaa", "pakdd"};
		String[] conferencesToCheck = {"pods", "isit", "edbt"};

		HashMap<String, Integer> groundTruthConferenceRanks = getGroundTruthRankings();
		
		HashMap<String, Double> generatedConferenceScores = new HashMap();
		for (int i = 0; i < conferencesToCheck.length; i++) {
			String conferenceAcronym = conferencesToCheck[i];
			double endogamyScoreOfConference = findEndogamyScoreOfConference(conferenceAcronym);
			generatedConferenceScores.put(conferenceAcronym, endogamyScoreOfConference);	
		}
		
		List<Entry<String, Double>> sortedConferenceScores = sortMap(generatedConferenceScores);
		for(Entry<String, Double> entry: sortedConferenceScores) {
			System.out.printf("Conference: %s Score: %s \n", entry.getKey(), entry.getValue().toString());
		}
		
		long endTime = System.nanoTime();
		System.out.println("Finished execution in " + (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");
		
		double conformanceScore = getConformanceScoreOfGeneratedRankings(groundTruthConferenceRanks, sortedConferenceScores);
		System.out.println("Conformance Score: "+conformanceScore);
	}

}
