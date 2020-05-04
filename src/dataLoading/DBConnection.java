package dataLoading;

import java.sql.*;

public class DBConnection {
	// Change the parameters accordingly.
	private static String dbUrl = "jdbc:mysql://localhost:3306/dblp_test";

	private static String user = "root";
	private static String password = "root";

	public static Connection getConn() {
		try {
//			Class.forName("org.gjt.mm.mysql.Driver");
			return DriverManager.getConnection(dbUrl, user, password);
		} catch (Exception e) {
			System.out.println("Error while opening a conneciton to database server: "
								+ e.getMessage());
			return null;
		}
	}
}
