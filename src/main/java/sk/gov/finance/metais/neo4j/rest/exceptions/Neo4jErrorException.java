package sk.gov.finance.metais.neo4j.rest.exceptions;

import java.util.List;

import sk.gov.finance.metais.neo4j.rest.bo.CypherError;


public class Neo4jErrorException extends RuntimeException {

	private List<CypherError> errors;
	
	public Neo4jErrorException(List<CypherError> errors) {
		
		this.errors = errors;
	}

	public Neo4jErrorException(String message, List<CypherError> errors) {
		super(message);
		
		this.errors = errors;
	}

	
}
