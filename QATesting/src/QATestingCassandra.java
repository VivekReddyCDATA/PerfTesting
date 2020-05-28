import java.sql.*;
import java.io.*;
import java.util.*; 
import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;

public class QATestingCassandra {
	
	public int TestItr = 1;
	public static ConnectDB SQLObj;
	
	public QATestingCassandra(String driverType)
	{		
		long ldiff; 
		java.util.Date dStart = new java.util.Date();  //get start time
		SQLObj.connect();
		java.util.Date dEnd = new java.util.Date();  //get end time
	    ldiff = dEnd.getTime()-dStart.getTime();
	    System.out.print(MessageFormat.format("Database connection time (ms): {0}, Driver Type: {1}\n", ldiff, driverType));	
	}
	
	public void storedProcedures() throws SQLException{
		DatabaseMetaData table_meta = SQLObj.getMetaData();
		ResultSet rs = table_meta.getProcedures(null, null, "%");
		while(rs.next()){
		  System.out.println(rs.getString("PROCEDURE_NAME"));
		}
	}
	
	public void colInfo(String schema, String tableName) throws SQLException{
		DatabaseMetaData table_meta = SQLObj.getMetaData();
		
		ResultSet rs_new = table_meta.getColumns(null, schema, tableName, null);
	    while (rs_new.next()) {
	          System.out.println(rs_new.getString("COLUMN_NAME") + " "
	                    + rs_new.getString("TYPE_NAME") + " "
	                    + rs_new.getString("COLUMN_SIZE"));
	    }
	    System.out.println("\n");	
	}
	
	public void DBStats() throws SQLException{
		DatabaseMetaData table_meta = SQLObj.getMetaData();
		String table[]={"TABLE"};  
		ResultSet rs = table_meta.getTables(null, null, "%", null);  
		String Query = "SELECT COUNT(*) as {2} FROM {0}.{1}.{2}";
		ArrayList<String> tables =  new ArrayList<String>();
		while(rs.next()){
			String TC = rs.getString("TABLE_CAT");
			String TS = rs.getString("TABLE_SCHEM");
			String TN = rs.getString("TABLE_NAME");
//			try {
//				if (TN == "NodeIdInfo") continue;
//				GenuineQuery(MessageFormat.format(Query, TC, TS, TN));
//			}
//			catch (SQLException e) {
//				 System.out.println("Error: " + TC +"."+TS+"."+ TN);
// 				 e.printStackTrace();
// 				 break;
//			}
//			if (TN == "RestaurantsCustom") break;
		System.out.println("Table Name: " + TC +"."+TS+"."+ TN);
		}
	}
	
	public void BatchInsert(int numRecords) throws SQLException {
		
		Random rand = new Random();
		
		String Query =  "INSERT INTO MyCustomObject__c (Name, CustomString__c, "
						+ "CustomString2__c, CustomString3__c, CustomDateTime1__c, " 
						+ "CustomDateTime2__c, CustomDouble1__c)"
						+ " VALUES (?, ?, ?, ?, ?, ?, ?) ";
		PreparedStatement pstmt = SQLObj.getPreparedStatement(Query);
		
		for (int k = 0; k < numRecords; k++) {
			String i = String.valueOf(k);
			pstmt.setString(1, "Test Object 1 : " + i );
			pstmt.setString(2, "Test String 'Just Some Random Stuffffff' 1 : " + i );
			pstmt.setString(3, "Test String 'Just Similar Ramdom Stufffffffff' 2 : " + i );
			pstmt.setString(4, "Test String 'HIiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii" 
								+ "iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii' 3 : " + i );
			
			pstmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
			pstmt.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
			pstmt.setDouble(7, rand.nextDouble());
			pstmt.addBatch();
		}
		
		
		int[] r = pstmt.executeBatch();
		for(int j: r)
		  System.out.println(j);
			
	}
	
	public void GenuineQuery(String cmd) throws SQLException{
		ResultSet rs = SQLObj.execQuery(cmd);
		int lim = 0;
		while(rs.next()) {
			System.out.println("-------------------New Row ----------");
		    for(int i=1;i<=rs.getMetaData().getColumnCount();i++) {
		    	  System.out.println(rs.getMetaData().getColumnName(i) +" = "+rs.getString(i)); 
		    }
		    lim++;
		    if (lim>=5) break;
		}
	}
	
	public void trailRun(String cmd) throws SQLException
	{
		java.util.Date dStart = new java.util.Date();   //get start time
		this.GenuineQuery(cmd);
		java.util.Date dEnd = new java.util.Date();  //get end time
		System.out.print(MessageFormat.format("Trail Run: Query Time (ms): {0}, .Query \"{1}\"\n", dEnd.getTime()-dStart.getTime(), cmd)); 
	}
	
	public void statistics(String cmd, String driverType, boolean debug) throws SQLException
	{	
		ArrayList<Long> ldiff = new ArrayList<Long>();
		for (int iter = 0; iter < TestItr; iter++) {
	        java.util.Date dStart = new java.util.Date();   //get start time
			ResultSet rs = SQLObj.execQuery(cmd);
			while(rs.next()) {
			    for(int i=1;i<=rs.getMetaData().getColumnCount();i++) {
			      if (iter == 0 && debug) {
			    	  System.out.println(rs.getMetaData().getColumnName(i) +"="+rs.getString(i)); 
			      }
			      else {
			    	  Object obj = rs.getObject(i);
			      }
			    }
			}
			java.util.Date dEnd = new java.util.Date();  //get end time
			ldiff.add(dEnd.getTime()-dStart.getTime());
		}
		Long avg = Long.valueOf(0);
	    for (int i = 0; i<TestItr; i++) { avg += ldiff.get(i);}
	    avg = avg / (TestItr);
	    System.out.print(MessageFormat.format("Query Time (ms): {0}, Driver Type: {1} \n .......Query \"{2}\"\n", avg, driverType, cmd)); 	
	}
	
	
	public void PerformExp(String driverType, boolean debug) {
		
        try {
        	
//        	 To Not to Record outlier
//        	this.trailRun("SELECT COUNT(*) as NumRowsFound FROM cdata.test.sampletable");
    
     	
        	// --------------------- Cdata.test.utmfileanalyze_details --------------------
        	
        	TestItr = 3;
        	
//        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, work_location_borough"
//					+ " FROM test.nyc_payroll LIMIT 10000", driverType, debug);
//        	
//        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, work_location_borough"
//					+ " FROM test.nyc_payroll LIMIT 50000", driverType, debug);
//        	
//        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, work_location_borough"
//					+ " FROM test.nyc_payroll LIMIT 150000", driverType, debug);
//        	
//        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, work_location_borough"
//					+ " FROM test.nyc_payroll", driverType, debug);
        	
        	
        	// CDATA Spec
        	
        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, work_location_borough"
					+ " FROM Cdata.test.nyc_payroll LIMIT 10000", driverType, debug);
        	
        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, work_location_borough"
					+ " FROM Cdata.test.nyc_payroll LIMIT 50000", driverType, debug);
        	
        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, work_location_borough"
					+ " FROM Cdata.test.nyc_payroll LIMIT 150000", driverType, debug);
        	
        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, work_location_borough"
					+ " FROM Cdata.test.nyc_payroll", driverType, debug);
        }
        
        catch (Exception e) {
            System.out.println("DB: Unable to close the statement \n" + e);
        }   
	}
	
	public static void main(String[] args) throws Exception
    {	
		String driverType;
		QATestingCassandra obj;
		
		boolean debug = false;
		
		System.out.println("\n ------------------ CDATA Section -----------------");
		driverType = "Cassandra";
		SQLObj = new CDATACassandraDriver();
		obj = new QATestingCassandra(driverType);
		
//		SQLObj.execCommand(Query);
//		obj.PerformExp(driverType, debug);
//		obj.GenuineQuery("SELECT * FROM test.keyspaces");
//		obj.GenuineQuery("SELECT * FROM sys_procedures");
//		obj.storedProcedures();
//		SQLObj.execCommand("DELETE FROM Cdata.test.nyc_payroll WHERE mid_init = 'L' ");
//		
//		obj.GenuineQuery("SELECT * FROM Cdata.test.restaurants");
//		obj.DBStats();
//		obj.GenuineQuery("SELECT * FROM sys_tablecolumns");
		obj.colInfo("test", "restaurants");
		
		SQLObj.terminate();	

		
    }
}


