import java.sql.*;
import java.io.*;
import java.util.*; 
import java.text.MessageFormat;

public class perfTest {
	
	public ConnectDB SQLObj;
	public int TestItr = 5;
	
	public perfTest(String driverType)
	{
		SQLObj = new ConnectDB();
		
		long ldiff; 
		java.util.Date dStart = new java.util.Date();  //get start time
		SQLObj.connect();
		java.util.Date dEnd = new java.util.Date();  //get end time
	    ldiff = dEnd.getTime()-dStart.getTime();
	    System.out.print(MessageFormat.format("Database connection time (ms): {0}, Driver Type: {1}\n", ldiff, driverType));
	   	
	}

	public void statistics(String cmd, String driverType, boolean debug) throws SQLException
	{	
		int numRows = 0;
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
			    	  Resultset Object = rs.getObject();
			      }
			    }
			}
			numRows = rs.getRow();
			java.util.Date dEnd = new java.util.Date();  //get end time
//			System.out.println(dEnd.getTime()-dStart.getTime());
			ldiff.add(dEnd.getTime()-dStart.getTime());
		}
		Long avg = Long.valueOf(0);
	    for (int i = 1; i<TestItr; i++) { avg += ldiff.get(i);}
	    avg = avg / (TestItr-1);
	    System.out.print(MessageFormat.format("Query Time (ms): {0}, Driver Type: {1} Num Rows {2} \n .......Query \"{3}\"\n", avg, driverType, numRows, cmd)); 	
	}
	
	public void clean()
	{
		SQLObj.terminate();
	}

	public static void main(String[] args) throws Exception
    {
		String driverType = "CData";
		boolean debug = false;
		
        perfTest obj = new perfTest(driverType);
        try {
        	obj.statistics("SELECT LastViewedDate FROM Lead", driverType, debug);
        	obj.statistics("SELECT Website FROM Lead", driverType, debug);
        	obj.statistics("SELECT Street, City, State, Latitude , Longitude FROM Lead", driverType, debug);
        }
        catch (Exception e) {
            System.out.println("DB: Unable to close the statement");
        }
        obj.clean();
    }
}
