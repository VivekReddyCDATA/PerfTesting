import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public class Testing {
	public static void main(String[] args) throws Exception
    {
		String keyspace = "cdata";
		
		DataStaxConnectDB dsDB = new DataStaxConnectDB();
		dsDB.connect("/home/vivekreddy/Desktop/Intern/secure-connect-testservers.zip", "vivekk", "!rssbus2020");
		dsDB.useKeyspace(keyspace);
		
		DataStaxDataLoader DL = new DataStaxDataLoader(dsDB.getSession(), keyspace);
		
		SimpleStatement simpleInsertBalance =
			    SimpleStatement.newInstance("SELECT COUNT(*) as numRows FROM cdata.nyc_payroll");
		
//		DL.createTable();
		
//		SimpleStatement statement =
//				  new SimpleStatement("SELECT release_version FROM system.local");
		
		DL.batchInsertFromJson("/home/vivekreddy/Desktop/Intern/nyc_payroll_900k.json");
//		
//		DL.PerformExp("DataStax Java API", false);

//		Row row = DL.session.execute(simpleInsertBalance).one();
////		
//		long value = row.getLong("numRows");
////
//		System.out.println(value);
		
//		row = DL.session.execute(simpleInsertBalance).one();
//		
//		value = row.getLong("numRows");
//
//		System.out.println(value);
		
		DL.session.close();
			
    }
}
