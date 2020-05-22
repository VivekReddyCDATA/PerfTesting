import java.sql.DriverManager;

public class ProgressCassandraDriver  extends ConnectDB {
	
	public void connect() {
		try {
	        Class.forName("com.ddtek.jdbc.cassandra.CassandraDriver");
	    }
	    catch (Exception exp) {
	        System.out.println("Sorry!!! Unable to load Cassandra Driver");
	        exp.printStackTrace();
	    }
	    
	    try {
	        conn = DriverManager.getConnection("jdbc:datadirect:cassandra://localhost:9042;");
	        
	        System.out.println("Connection Successful to Cassandra DB !!!!!!!!!!!!!!!!!!");
	    }
	    catch (Exception e) {
	        e.printStackTrace();
	    }
	}
	
}
