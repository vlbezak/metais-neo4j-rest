package sk.gov.finance.metais.neo4j.rest.bo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CypherError 
{
	private String code;
	private String message;
	
	public CypherError() {
		
	}
	
	public String getCode() {
		return code;
	}
	
	public void setCode(String code) {
		this.code = code;
	}
	
	public String getMessage() {
		return message;
	}
	
	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public String toString() {
		return "CypherError [code=" + code + ", message=" + message + "]";
	}
}
