package xmlEntities;

public class Author {
	public StringBuffer name = new StringBuffer();
	public Integer rank;
	
	public Author() {
	}
	
	@Override
	public String toString() {
		return "Author name: "+this.name + "Author Rank: " + this.rank;
	}
}
