package io.huru.dwmongo;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;


public interface MongoConfigurationProvider {

	@JsonProperty
	@Valid
	@NotNull
	public MongoDBConfiguration getMongo();
	
}
