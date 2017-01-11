package io.huru.dwmongo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.mongodb.MongoClientURI;
import io.dropwizard.validation.PortRange;
import io.dropwizard.validation.ValidationMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang3.StringUtils.*;


public class MongoDBConfiguration {
	
	private static transient Logger logger = LoggerFactory.getLogger(MongoDBConfiguration.class);
	
	public MongoDBConfiguration() {
	}
	
	@JsonProperty
	private String uri;

	@JsonProperty
	private String host;
	
	@JsonProperty
	@PortRange
	private int port;
	
	@JsonProperty
	private String name;
	
	@JsonProperty
	private String username;

	@JsonProperty
	private String password;

	private MongoClientURI mongoUri;

	public String getHost() {
		return mongoUri != null ? mongoUri.getHosts().get(0) : host;
	}

	public int getPort() {
		return port;
	}

	public String getName() {
		return mongoUri != null ? mongoUri.getDatabase() : name;
	}

	public String getUsername() {
		return mongoUri != null ? mongoUri.getUsername() : username;
	}

	public char[] getPassword() {
		return mongoUri != null ? mongoUri.getPassword() : password.toCharArray();
	}
	
    @ValidationMethod(message = "Must provide full uri or all pieces like server, port, db name, usernam and password")
    public boolean isValid() {
    	if (isNotBlank(uri)) return true;
    	if (isBlank(host)) return false;
    	if (isBlank(name)) return false;
    	return true;
    }
    
	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = isNotBlank(uri) ? uri.trim() : uri;
		this.mongoUri = new MongoClientURI(uri);
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public MongoClientURI getMongoClientUri() {
		StringBuilder b = new StringBuilder();
		if (isNotEmpty(getUri())){
			b.append(getUri());
		} else {
			// Construct URI from split parameters.
			b = new StringBuilder("mongodb://");
			if (isNotEmpty(getUsername())){
				b.append(getUsername()).append(":").append(getPassword()).append("@");
			}
			
			b.append(getHost());
			if (getPort() > 0){
				b.append(":").append(getPort());
			}
			b.append("/").append(getName());
		}
		
		logger.info("Resolved mongo URI " + b.toString());

		return new MongoClientURI(b.toString());
		
	}
    
}
