package xml;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class DblpParserWWW {
	
	private int ancestor = -1;
	private int currElement = -1;
	StringBuffer author;
	int overallAuthorCount = 0;
	int authorIdCounter = -1;
	
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
				System.out.printf("AliasID: %s Id: %d Name: %s \n", ++overallAuthorCount, ++authorIdCounter, author.toString().trim());
			}
			if(rawName.equals("www")) {
				ancestor = -1;
			}
		}
		
	}
	
	public DblpParserWWW(String filePath) throws ParserConfigurationException, SAXException, IOException {
		SAXParserFactory parserFactory = SAXParserFactory.newInstance();
		SAXParser parser = parserFactory.newSAXParser();
		ConfigHandler handler = new ConfigHandler();
		parser.getXMLReader().setFeature(
				"http://xml.org/sax/features/validation", true);
		parser.parse(new File(filePath), handler);
	
	}

	public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException {
		Long start = System.currentTimeMillis();
		String filePath = "C:\\Users\\prans\\Documents\\capstone\\dblp_dataset\\www_test.xml";
		DblpParserWWW p = new DblpParserWWW(filePath);
		Long end = System.currentTimeMillis();
		System.out.println("Used: " + (end - start) / 1000 + " seconds");
		
	}

}
