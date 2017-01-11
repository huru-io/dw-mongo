package io.huru.dwmongo;

import com.codahale.metrics.health.HealthCheck;
import com.mongodb.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.temporal.ChronoField;

public class MongoHealthCheck extends HealthCheck {
	
	private static Logger logger = LoggerFactory.getLogger(MongoHealthCheck.class);

    private MongoDatabase db;
	private String uri;

	public MongoHealthCheck(MongoDatabase mongoDatabase, String uri) {
    	this.db = mongoDatabase;
    	this.uri = uri;
	}

	@Override
    protected Result check() throws Exception {
        try {
        	String collectionName = "ping-" + LocalDateTime.now().getLong(ChronoField.NANO_OF_DAY);
			db.createCollection(collectionName);
			db.getCollection(collectionName).drop();
	        return Result.healthy("MongoDB is running... :) ");
		} catch (Exception e) {
			logger.error("Could not connect to mongodb at " + uri, e);
			return Result.unhealthy("Mongo URI is " + uri, e);
		}
    }

}