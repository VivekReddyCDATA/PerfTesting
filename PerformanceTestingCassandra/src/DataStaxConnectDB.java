import java.net.InetSocketAddress;
import java.nio.file.Paths;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.AlterKeyspaceStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;

public class DataStaxConnectDB {
	
	private CqlSession session;
	 	
    public void connect(String BundleZip, String user, String pass) {
        CqlSessionBuilder builder = CqlSession.builder()
        							.withCloudSecureConnectBundle(Paths.get(BundleZip))
        							.withAuthCredentials(user, pass);
        session = builder.build();
    }
        
    public void createKeyspace(String keyspaceName, int numberOfReplicas) {
    	CreateKeyspace createKeyspace = SchemaBuilder.createKeyspace(keyspaceName).ifNotExists()
    									.withSimpleStrategy(numberOfReplicas)
    									.withDurableWrites(true);
    	
    	session.execute(createKeyspace.build());
    }
    
    public void useKeyspace(String keyspace) {
    	session.execute("USE " + CqlIdentifier.fromCql(keyspace));
    }
    
    public CqlSession getSession() {
        return this.session;
    }
 
    public void close() {
        session.close();
    }    
}