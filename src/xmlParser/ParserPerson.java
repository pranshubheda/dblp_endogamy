package xmlParser;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

//import javax.xml.parsers.ParserConfigurationException;
//import javax.xml.parsers.SAXParser;
//import javax.xml.parsers.SAXParserFactory;

import javax.xml.parsers.*;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import dataLoading.DBConnection;

public class ParserPerson {
	private int ancestor = -1;
	private int currElement = -1;
	StringBuffer author;
	int overallAuthorCount = 0;
	int authorIdCounter = -1;
	private Connection conn;
	PreparedStatement stmt_person;
	int line = 0;
	
	private class ConfigHandler extends DefaultHandler {
		
		public void startElement(String namespaceURI, String localName,
				String rawName, Attributes atts) throws SAXException {
			if (rawName.equals("www")) {
				String wwwKey = atts.getValue("key");
				if (wwwKey.contains("homepages")) {
					ancestor = 2;
					author = new StringBuffer();
					authorIdCounter = 0;
				}
			}
			
			if(ancestor == 2 && rawName.equals("author")) {
				author = new StringBuffer();
				currElement = 2;
			}
			line++;
		}
		
		public void characters(char[] ch, int start, int length)
				throws SAXException {
			if (ancestor == 2) {
				if(currElement == 2) {
					String str = new String(ch, start, length).trim();
					author.append(str);
				}
			}
		}
		
		public void endElement(String namespaceURI, String localName,
				String rawName) throws SAXException {
			if (ancestor == 2 && rawName.equals("author")) {
//				System.out.printf("AliasID: %s Id: %d Name: %s \n", ++overallAuthorCount, ++authorIdCounter, author.toString().trim());
				try {
					stmt_person.setInt(1, ++overallAuthorCount);
					stmt_person.setInt(2, ++authorIdCounter);
					int primaryAlias = overallAuthorCount; 
					if (authorIdCounter > 1) {
						primaryAlias = overallAuthorCount-authorIdCounter+1;
					}
					stmt_person.setInt(3, primaryAlias);
					stmt_person.setString(4, author.toString());
					stmt_person.addBatch();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			if(rawName.equals("www")) {
				ancestor = -1;
			}
			
			if (line % 10000 == 0) {
				try {
					System.out.printf("Finished loading %d person ...\n", overallAuthorCount);
					stmt_person.executeBatch();
					conn.commit();
				} catch (SQLException e) {
					System.err.println(e.getMessage());
				}
			}
		}
		
	}
	
	public ParserPerson(String filePath) throws ParserConfigurationException, SAXException, IOException, SQLException {
		conn = DBConnection.getConn();
		conn.setAutoCommit(false);
		stmt_person = conn
				.prepareStatement("insert into person(id,priority,primary_alias,name) values (?,?,?,?)");
		
		SAXParserFactory parserFactory = SAXParserFactory.newInstance();
		SAXParser parser = parserFactory.newSAXParser();
		ConfigHandler handler = new ConfigHandler();
		parser.getXMLReader().setFeature(
				"http://xml.org/sax/features/validation", true);
		parser.parse(new File(filePath), handler);
		try {
			stmt_person.executeBatch();
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		conn.close();
	}

//	public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException, SQLException {
//		System.setProperty("entityExpansionLimit", "2000000");
//		Long start = System.currentTimeMillis();
//		System.out.println("Started loading person ...");
//		String filePath = "C:\\Users\\prans\\Documents\\capstone\\dblp_dataset\\dblp.xml";
//		ParserPerson p = new ParserPerson(filePath);
//		Long end = System.currentTimeMillis();
//		System.out.println("Used: " + (end - start) / 1000 + " seconds");
//	}

}
