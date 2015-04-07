package sk.gov.finance.metais.neo4j.rest.service;

import java.net.URI;
import java.util.List;




import sk.gov.finance.metais.neo4j.rest.bo.CypherResponse;
import sk.gov.finance.metais.neo4j.rest.bo.Transaction;

import com.fasterxml.jackson.core.JsonProcessingException;

public interface Neo4jTransactionalService 
{
	public CypherResponse runQuery(Transaction transaction, String cypherQuery);
	
	public CypherResponse runQuery(Transaction transaction, List<String> cypherQueries);
	
	public Transaction startTransaction();
	
	public void commitTransaction(Transaction transaction);
	
	public void rollbackTransaction(Transaction transaction);
}
