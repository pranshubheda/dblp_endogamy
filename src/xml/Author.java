package xml;

public class Author {
	StringBuffer name = new StringBuffer();
	Integer rank;
	
	public Author() {
	}
	
	@Override
	public String toString() {
		return "Author name: "+this.name + "Author Rank: " + this.rank;
	}
}
