package io.huru.dwmongo;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBBundle implements ConfiguredBundle<MongoConfigurationProvider> {

	private Logger logger = LoggerFactory.getLogger(getClass());
	private MongoClient mongoClient;
	private MongoDBConfiguration conf;

	@Override
	public void run(MongoConfigurationProvider confProvider, final Environment environment) throws Exception {
		
		conf = confProvider.getMongo();
		environment.lifecycle().manage(new Managed() {
			
			@Override
			public void stop() throws Exception {
				mongoClient.close();
			}
			
			@Override
			public void start() throws Exception {
				MongoClientURI mongoURI = conf.getMongoClientUri();
				mongoClient = new MongoClient( mongoURI );
				environment.healthChecks().register("Mongo DB Health Check", new MongoHealthCheck(mongoClient.getDatabase(mongoURI.getDatabase()), conf.getUri()));
				logger.info("Successfully started mongo db client.");
			}

		});
		
	}
	
	public MongoDatabase getMongoDB(){
		return mongoClient.getDatabase(conf.getName());
	}

	public MongoCollection<Document> getMongoCollection(String collectionName) {
		return getMongoDB().getCollection(collectionName);
	}
	
	@SuppressWarnings("deprecation")
	public DB getDB(){
		return mongoClient.getDB(conf.getName());
	}

	@Override
	public void initialize(Bootstrap<?> bootstrap) {
	}

	public DBCollection getCollection(String collectionName) {
		return getDB().getCollection(collectionName);
	}

}
