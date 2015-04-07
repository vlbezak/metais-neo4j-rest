package sk.gov.finance.metais.neo4j.rest.bo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CypherTransaction {
	
	private String expires;

	public CypherTransaction() {
		
	}
	
	public String getExpires() {
		return expires;
	}

	public void setExpires(String expires) {
		this.expires = expires;
	}

	@Override
	public String toString() {
		return "CypherTransaction [expires=" + expires + "]";
	}
}
