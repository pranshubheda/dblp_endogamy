package endogamy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.DecimalFormat;
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
import java.util.stream.Collectors;

import org.bouncycastle.asn1.eac.BidirectionalMap;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import com.google.common.collect.Sets;

public class EndogamyScorer {
	private static String destinationDbPath = "C:\\Users\\prans\\Documents\\capstone\\dblp_dataset\\neo4j\\dblp\\";

	private static String destinationDbPathFilteredCoAuthor = "C:/Users/prans/Documents/capstone/co_author_network/neo4j_filtered/filtered_co_author_network/";

	private static GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(destinationDbPath))
			.setConfig(GraphDatabaseSettings.pagecache_memory, "2048M" )
			.setConfig(GraphDatabaseSettings.string_block_size, "60" )
			.setConfig(GraphDatabaseSettings.array_block_size, "300" )
			.newGraphDatabase();

	private static GraphDatabaseService coAuthorDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(destinationDbPathFilteredCoAuthor))
			.setConfig(GraphDatabaseSettings.pagecache_memory, "2048M" )
			.setConfig(GraphDatabaseSettings.string_block_size, "60" )
			.setConfig(GraphDatabaseSettings.array_block_size, "300" )
			.newGraphDatabase();
	
	public static void queryNeo4j() {
		long startTime = System.nanoTime();
		try {
			try ( Transaction tx = db.beginTx()){
//				File queryFile = new File("C:\\Users\\prans\\Documents\\capstone\\capstone_workspace\\dblpParser\\src\\endogamy\\find_papers_with_multiple_authors.cql");
				File queryFile = new File("C:\\Users\\prans\\Documents\\capstone\\capstone_workspace\\dblpParser\\src\\endogamy\\test_query.cql");
//				File queryFile = new File("C:\\Users\\prans\\Documents\\capstone\\capstone_workspace\\dblpParser\\src\\endogamy\\intersection.cql");

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
	
	public static double findEndogamyScoreOfPaper(long paperId, HashSet<Long> authors, long year) {
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
//				System.out.println(authorSet + "-> "+Math.round(endogamyScore * 10000.0)/10000.0);
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
		conferenceAcronym = "/"+conferenceAcronym+"/";
		params.put("conferenceAcronym", conferenceAcronym);
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
	
	public static HashSet<Long> findPapersOfConferenceInYear(String conferenceAcronym, long year) {
		HashSet<Long> paperIds = null;
		String findPapersOfConferenceQuery = 
				"MATCH (p:Person)-[q:HasAuthored]-(r:Paper) " + 
				"where r.paper_key CONTAINS $conferenceAcronym AND r.year = $year " + 
				"RETURN ID(r) as paperId;";
		HashMap<String, Object> params = new HashMap();
		conferenceAcronym = "/"+conferenceAcronym+"/";
		params.put("conferenceAcronym", conferenceAcronym);
		params.put("year", year);
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
	
	public static HashSet<Long> findPapersOfAuthor(long authorId) {
		HashSet<Long> paperIds = null;
		String findPapersOfAuthorQuery = 
				"MATCH (p:Person)-[q:HasAuthored]-(r:Paper) " + 
				"where id(p) = $authorId " + 
				"RETURN ID(r) as paperId;";
		HashMap<String, Object> params = new HashMap();
		params.put("authorId", authorId);
		Result result = db.execute(findPapersOfAuthorQuery, params);
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
	
	public static HashSet<Long> findSeniorAuthors(long paperId, long skipCount) {
		HashSet<Long> seniorAuthors = null;
		try {
			try ( Transaction tx = db.beginTx()){
				String query = 
						"MATCH (author:Person)-[q:HasAuthored]-(paper:Paper) " + 
						"WHERE ID(paper) = $paperId " + 
						"RETURN ID(author) as author_id " +
						"ORDER BY q.rank;";
				HashMap<String, Object> params = new HashMap();
				params.put("paperId", paperId);
				Result result = db.execute(query, params);
				seniorAuthors = new HashSet<Long>();
				while(result.hasNext()) {
					Map<String, Object> row = result.next();
					if(skipCount > 0) {
						skipCount--;
					}
					else {
						Long authorId = (Long)row.get("author_id");
						seniorAuthors.add(authorId);
					}
				}
			}			
		} catch (Exception e) {		
			e.printStackTrace();
		}
		return seniorAuthors; 
	}
	
	public static double findEndogamyScoreOfConference(String conferenceAcronym, HashSet<Long> paperIds, HashSet<Long> newAuthorsForConference) {
		double sumOfEndoScoreOfPapersPublished = 0;
		ArrayList<Long> ignoredPaperIds = new ArrayList();
		for (Long paperId : paperIds) {
			HashSet<Long> authors = findAuthors(paperId);
			if(newAuthorsForConference != null) {
				authors.removeAll(newAuthorsForConference);
			}
			if (authors.size() <= 10) {
//				long skipCount = (long) (authors.size() * 0.2);
//				authors = findSeniorAuthors(paperId, skipCount);
				double endogamyScoreOfPaper = findEndogamyScoreOfPaper(paperId, authors);
				sumOfEndoScoreOfPapersPublished += endogamyScoreOfPaper;				
//				System.out.println("Checked paper id: "+paperId+" Score: "+endogamyScoreOfPaper);
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
//		String cleanedCoreRankingsFile = "C:/Users/prans/Documents/capstone/ground_truth_rankings/CORE/cleaned_core_rankings_0804.csv";
		String cleanedCoreRankingsFile = "C:/Users/prans/Documents/capstone/ground_truth_rankings/MSAR/msar_db.csv";

        BufferedReader br = new BufferedReader(new FileReader(cleanedCoreRankingsFile));
        String rankingEntry = br.readLine();
        while(rankingEntry != null) {
        	String[] ranking = rankingEntry.split(",");
        	String conferenceAcronym = ranking[2];
        	int conferenceRank = Integer.parseInt(ranking[3]);
        	groundTruthConferenceRanks.put(conferenceAcronym, conferenceRank);
        	rankingEntry = br.readLine();
        }
        return groundTruthConferenceRanks;
	}

	public static Double getConformanceScoreOfGeneratedRankings(HashMap<String, Integer> groundTruthConferenceRanks, List<Entry<String, Double>> sortedConferenceScores) {
		
		int p = 0, f = 0;
		//for each combination check conditions and remember count
		for(int i=0; i<sortedConferenceScores.size(); i++) {
			Entry<String, Double> conference1Entry = sortedConferenceScores.get(i);
			String conference1Acronym = conference1Entry.getKey();
			double conference1Score = conference1Entry.getValue();
			int conference1Ranking = groundTruthConferenceRanks.get(conference1Acronym);
			
			for(int j=i+1; j<sortedConferenceScores.size(); j++) {
				Entry<String, Double> conference2Entry = sortedConferenceScores.get(j);
				String conference2Acronym = conference2Entry.getKey();
				double conference2Score = conference2Entry.getValue();
				int conference2Ranking = groundTruthConferenceRanks.get(conference2Acronym);
//				System.out.printf("Comparing %s and %s \n", conference1Acronym, conference2Acronym);
//				System.out.printf("Score %s and %s \n", conference1Score, conference2Score);
//				System.out.printf("Ranking %s and %s \n", conference1Ranking, conference2Ranking);
				
				// rank 1 is actually bigger than rank 2
				if(conference1Ranking <= conference2Ranking)
					p++;
				else
					f++;
			}
		}
		
		double conformanceScore = 100 * ((p*1.0)/(p+f));
		return conformanceScore;
	}
	
	public static List<Entry<String, Double>> loadEndogamyScores() throws IOException {
		List<Entry<String, Double>> loadedEndogamyScores = new ArrayList();
		String cleanedCoreRankingsFile = "C:/Users/prans/Documents/capstone/endogamy_scores/new_authors_msar_db_2019.csv";
        BufferedReader br = new BufferedReader(new FileReader(cleanedCoreRankingsFile));
        String rankingEntry = br.readLine();
        while(rankingEntry != null) {
        	String[] ranking = rankingEntry.split("  ");
        	String conferenceAcronym = ranking[1];
        	double endogamyScore = Double.parseDouble(ranking[2]);
        	loadedEndogamyScores.add(Map.entry(conferenceAcronym, endogamyScore));
        	rankingEntry = br.readLine();
        }
        return loadedEndogamyScores;
	}
	
	

	private static List<Entry<String, Double>> generateEndogamyScores(HashMap<String, Integer> groundTruthConferenceRanks)
			throws FileNotFoundException {
		//		String[] conferencesToCheck = {"pods", "isit", "edbt", "icde", "sigmod", "mdm", "asiacrypt", "dasfaa", "pakdd"};
//				String[] conferencesToCheck = {"pods", "isit", "edbt"};
		//		String[] conferencesToCheck = {"wsdm"};
		
				Set<String> conferencesToCheckSet = groundTruthConferenceRanks.keySet();
				String[] conferencesToCheck = (String[]) Arrays.copyOf(conferencesToCheckSet.toArray(), conferencesToCheckSet.size(), String[].class); 
				int totalConferencesChecked = 0;
				
				HashMap<String, Double> generatedConferenceScores = new HashMap();
				for (int i = 0; i < conferencesToCheck.length; i++) {
					String conferenceAcronym = conferencesToCheck[i];
					HashSet<Long> newAuthors = null;
//					newAuthors = findNewAuthors(conferenceAcronym, 2019);
					//find papers in this conference
					//HashSet<Long> paperIds = findPapersOfConference(conferenceAcronym);
					HashSet<Long> paperIds = findPapersOfConferenceInYear(conferenceAcronym, 2019);
					if (paperIds!=null && paperIds.size() > 100) {
						//for each paper find endogamy score
						System.out.println(conferenceAcronym+" has "+paperIds.size()+" papers to check");
						double endogamyScoreOfConference = findEndogamyScoreOfConference(conferenceAcronym, paperIds, newAuthors);
						generatedConferenceScores.put(conferenceAcronym, endogamyScoreOfConference);	
		//				System.out.println(conferenceAcronym+" : "+endogamyScoreOfConference);
						totalConferencesChecked++;
					}
				}
		
				String outputEndogamyScores = "C:/Users/prans/Documents/capstone/endogamy_scores/msar_ai_2019.csv"; 
				PrintWriter writer = new PrintWriter(new File(outputEndogamyScores));
				
				int flushCounter = 0;
				List<Entry<String, Double>> sortedConferenceScores = sortMap(generatedConferenceScores);
				int i = 1;
				for(Entry<String, Double> entry: sortedConferenceScores) {
					double ratio = findRatioOfNewAuthors(entry.getKey(), 2019);
					System.out.printf("Conference: %s Score: %.5f  Ratio: %.5f \n", entry.getKey(), entry.getValue(), ratio);
					writer.write(i++ +"  "+entry.getKey()+"  "+ new DecimalFormat("#.#####").format(entry.getValue())+"\n");
//					writer.write(i++ +"  "+entry.getKey()+"  "+ new DecimalFormat("#.#####").format(entry.getValue())+"  "+new DecimalFormat("#.#####").format(ratio)+"\n");
					if(flushCounter>= 100)
						writer.flush();
				}
				
				writer.close();
				System.out.println("Total conferences checked "+totalConferencesChecked );
				return sortedConferenceScores;
	}
	
	public static double generateEndogamyScoreOfAuthor(long authorId) {
		double sumOfEndoScoreOfPapersPublished = 0;
		double generatedEndogamyScore = 0.0;
		HashSet<Long> paperIds = findPapersOfAuthor(authorId);
		for (Long paperId : paperIds) {
			HashSet<Long> authors = findAuthors(paperId);
			if (authors.size() <= 10) {
//				long skipCount = (long) (authors.size() * 0.2);
//				authors = findSeniorAuthors(paperId, skipCount);
				double endogamyScoreOfPaper = findEndogamyScoreOfPaper(paperId, authors);
				sumOfEndoScoreOfPapersPublished += endogamyScoreOfPaper;				
//				System.out.println("Checked paper id: "+paperId+" Score: "+endogamyScoreOfPaper);
			}
		}
		generatedEndogamyScore = sumOfEndoScoreOfPapersPublished/paperIds.size();
		System.out.println("Author ID: "+authorId+" Endo: "+generatedEndogamyScore);
		return generatedEndogamyScore;
	}
	
	public static HashSet<Long> findGivenYearAuthors(String conferenceAcronym, long year) {
		HashSet<Long> givenYearAuthors = null;
		try {
			try ( Transaction tx = db.beginTx()){
				String query = 
						"MATCH (author:Person)-[q:HasAuthored]-(paper:Paper) " + 
						"WHERE paper.paper_key CONTAINS $conferenceAcronym AND paper.year = $year " + 
						"RETURN ID(author) as author_id;";
				HashMap<String, Object> params = new HashMap();
				conferenceAcronym = "/"+conferenceAcronym+"/";
				params.put("conferenceAcronym", conferenceAcronym);
				params.put("year", year);
				Result result = db.execute(query, params);
				givenYearAuthors = new HashSet<Long>();
				while(result.hasNext()) {
					Map<String, Object> row = result.next();
					Long authorId = (Long)row.get("author_id");
					givenYearAuthors.add(authorId);
				}
			}			
		} catch (Exception e) {		
			e.printStackTrace();
		}
		return givenYearAuthors;
	}
	
	public static HashSet<Long> findPreviousYearAuthors(String conferenceAcronym, long year) {
		HashSet<Long> previousYearAuthors = null;
		try {
			try ( Transaction tx = db.beginTx()){
				String query = 
						"MATCH (author:Person)-[q:HasAuthored]-(paper:Paper) " + 
						"WHERE paper.paper_key CONTAINS $conferenceAcronym AND paper.year < $year " + 
						"RETURN ID(author) as author_id;";
				HashMap<String, Object> params = new HashMap();
				conferenceAcronym = "/"+conferenceAcronym+"/";
				params.put("conferenceAcronym", conferenceAcronym);
				params.put("year", year);
				Result result = db.execute(query, params);
				previousYearAuthors = new HashSet<Long>();
				while(result.hasNext()) {
					Map<String, Object> row = result.next();
					Long authorId = (Long)row.get("author_id");
					previousYearAuthors.add(authorId);
				}
			}			
		} catch (Exception e) {		
			e.printStackTrace();
		}
		return previousYearAuthors;
	}
	
	public static double findRatioOfNewAuthors(String conferenceAcronym, long year) {
		HashSet<Long> givenYearAuthors = findGivenYearAuthors(conferenceAcronym, year);
		HashSet<Long> previousYearAuthors = findPreviousYearAuthors(conferenceAcronym, year);
		HashSet<Long> givenYearAuthorsNotPublishedPreviously = (HashSet<Long>) givenYearAuthors.stream().map(Long::new).collect(Collectors.toSet());
		
		boolean removed = givenYearAuthorsNotPublishedPreviously.removeAll(previousYearAuthors);
		double ratio = 0;
		if(removed) {
			ratio = (1.0*givenYearAuthorsNotPublishedPreviously.size()/ givenYearAuthors.size());
		}
		
		System.out.println(givenYearAuthors);
		System.out.println(givenYearAuthors.size());
		System.out.println("******");
		System.out.println(givenYearAuthorsNotPublishedPreviously.size());
		System.out.println(givenYearAuthorsNotPublishedPreviously);
		System.out.println(ratio);
		return ratio;
	}

	public static HashSet<Long> findNewAuthors(String conferenceAcronym, long year) {
		HashSet<Long> givenYearAuthors = findGivenYearAuthors(conferenceAcronym, year);
		HashSet<Long> previousYearAuthors = findPreviousYearAuthors(conferenceAcronym, year);
		HashSet<Long> givenYearAuthorsNotPublishedPreviously = (HashSet<Long>) givenYearAuthors.stream().map(Long::new).collect(Collectors.toSet());
		givenYearAuthorsNotPublishedPreviously.removeAll(previousYearAuthors);
		return givenYearAuthorsNotPublishedPreviously;
	}
	
	public static HashSet<Long> findOneHopNeighbors(long authorId) {
		HashSet<Long> oneHopAuthors = null;
		try {
			try ( Transaction tx = coAuthorDb.beginTx()){
				String query = 
						"MATCH (p:author)-[q:co_authored]-(r:author) " + 
						"WHERE id(p) = $authorId " + 
						"RETURN id(r)+25000000 as neighborId " + 
						"order by r.kClass desc;";
				HashMap<String, Object> params = new HashMap();
				params.put("authorId", authorId-25000000);
				Result result = coAuthorDb.execute(query, params);
				oneHopAuthors = new HashSet<Long>();
				while(result.hasNext()) {
					Map<String, Object> row = result.next();
					Long neighborId = (Long)row.get("neighborId");
					oneHopAuthors.add(neighborId);
				}
			}			
		} catch (Exception e) {		
			e.printStackTrace();
		}
		return oneHopAuthors;
	}
	
	public static HashSet<Long> findCommonPapersOfAuthorSet(Set<Long> authorSet) {
		HashSet<Long> commonPapers = null;
		String findPapersOfAuthorQuery = 
				"MATCH (p:Person)-[q:HasAuthored]-(r:Paper) " + 
				"where Id(p) in $authorSet " + 
				"with r, count(DISTINCT p) as count " + 
				"WHERE count = $authorSetSize " + 
				"RETURN id(r) as commonPapers;";
		HashMap<String, Object> params = new HashMap();
		params.put("authorSet", authorSet);
		params.put("authorSetSize", authorSet.size());
		Result result = db.execute(findPapersOfAuthorQuery, params);
		Long intersectionCount = null;
		if(result.hasNext())
			commonPapers = new HashSet();
		while(result.hasNext()) {
			Map<String, Object> row = result.next();
			long paperId = (Long)row.get("commonPapers");
			commonPapers.add(paperId);
		}
		return commonPapers;
	}
	
	public static int findYearOfPaper(Long paper) {
		String findYearOfPaperQuery = 
				"MATCH (r:Paper) " +
				"WHERE ID(r) = $paper " +
				"RETURN r.year as year;";
		HashMap<String, Object> params = new HashMap();
		params.put("paper", paper);
		Result result = db.execute(findYearOfPaperQuery, params);
		Integer year = null;
		while(result.hasNext()) {
			Map<String, Object> row = result.next();
			year = (Integer)row.get("year");
		}
		return year;
	}
	
	public static double findEndoEdgeWeight(Set<Long> authorSet) {
		HashSet<Long> commonPapers = findCommonPapersOfAuthorSet(authorSet);
		double endoEdgeWeight = 0;
		for(Long paper : commonPapers) {
			HashSet<Long> authors = findAuthors(paper);
			if(authors.size() <= 10) {
				long year = findYearOfPaper(paper);
				double endoScore = findEndogamyScoreOfPaper(paper, authors, year);
				endoEdgeWeight+=endoScore;
			}
		}
		return endoEdgeWeight;
	}
	
	public static double avgEndoNodeWeightOneHopFromAuthor(long author) {
		//find all the one hope neighbors of this node.
		HashSet<Long> neighbors = findOneHopNeighbors(author);
		//find the endogamy score of each publication for these 2 authors and sum it. proportional to # of colabs
		double totalEndoNodeWeight = 0;
		for(Long n : neighbors) {
			HashSet<Long> authorSet = new HashSet<Long>();
			authorSet.add(n);
			authorSet.add(author);
			double endoEdgeWeight = findEndoEdgeWeight(authorSet);
			totalEndoNodeWeight += endoEdgeWeight;
		}
		//same for the central node ... avg of endo of all the edge weights
		HashSet<Long> paperIds = findPapersOfAuthor(author);
		long totalPapers = paperIds.size();
		double avgEndoNodeWeight = totalEndoNodeWeight/totalPapers;
		System.out.println(totalEndoNodeWeight+"\t"+totalPapers+"\t"+avgEndoNodeWeight);
		return avgEndoNodeWeight;
	}
	
	public static ArrayList<Double> generateOneHopEndoDump(long author) throws FileNotFoundException {
		HashSet<Long> neighbors = findOneHopNeighbors(author);
		ArrayList<Double> endoDump = new ArrayList<Double>();
		for(Long n : neighbors) {
			double endoScore = generateEndogamyScoreOfAuthor(n);
			endoDump.add(endoScore);
		}
		Collections.sort(endoDump);
		return endoDump;
	}
	
	public static void main(String[] args) throws IOException {
		File outputFile = new File("C:/Users/prans/Documents/capstone/co_author_network/one_hop_endo_dump_correct.tsv");
		PrintWriter writer = new PrintWriter(outputFile);

		//anomalies
//		ArrayList<Double> endoDump = generateOneHopEndoDump(25000000+1361021);
//		writer.print(endoDump+"\n");
//		endoDump = generateOneHopEndoDump(25000000+1974949);
//		writer.print(endoDump+"\n");
//		endoDump = generateOneHopEndoDump(25000000+2417722);
//		writer.print(endoDump+"\n");
		
		//correct
		ArrayList<Double> endoDump = generateOneHopEndoDump(25000000+146514);
		writer.print(endoDump+"\n");
		writer.flush();
		writer.close();
//		long startTime = System.nanoTime();
		
//		HashMap<String, Integer> groundTruthConferenceRanks = getGroundTruthRankings();
		
//		List<Entry<String, Double>> sortedConferenceScores = generateEndogamyScores(groundTruthConferenceRanks);
//		List<Entry<String, Double>> sortedConferenceScores = loadEndogamyScores();
		
//		double conformanceScore = getConformanceScoreOfGeneratedRankings(groundTruthConferenceRanks, sortedConferenceScores);
//		System.out.println("Conformance Score: "+conformanceScore);
				
//		long endTime = System.nanoTime();
//		System.out.println("Finished execution in " + (endTime-startTime)/(60 * Math.pow(10, 9)) +" min");
	}

}
