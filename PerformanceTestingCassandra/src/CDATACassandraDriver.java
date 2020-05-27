import java.sql.DriverManager;
import java.util.Properties;

public class CDATACassandraDriver extends ConnectDB{

	
	public void connect() {
		try {
	        Class.forName("cdata.jdbc.cassandra.CassandraDriver");
	    }
	    catch (Exception exp) {
	        System.out.println("Sorry!!! Unable to load Cassandra Driver");
	        exp.printStackTrace();
	    }
	    
	    try {
	        conn = DriverManager.getConnection("jdbc:cassandra:server=localhost;port=9042;");
	        
	        System.out.println("Connection Successful to Cassandra DB !!!!!!!!!!!!!!!!!!");
	    }
	    catch (Exception e) {
	        e.printStackTrace();
	    }
	}
}
