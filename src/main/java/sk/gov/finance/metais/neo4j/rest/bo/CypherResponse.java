package sk.gov.finance.metais.neo4j.rest.bo;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CypherResponse {

	private String commit;
	private List<CypherResult> results;
	private CypherTransaction transaction;
	private List<CypherError> errors;
	
	public CypherResponse() {
		
	}
	
	public String getCommit() {
		return commit;
	}
	
	public void setCommit(String commit) {
		this.commit = commit;
	}
	
	public List<CypherResult> getResults() {
		return results;
	}
	
	public void setResults(List<CypherResult> results) {
		this.results = results;
	}
	
	public CypherTransaction getTransaction() {
		return transaction;
	}
	
	public void setTransaction(CypherTransaction transaction) {
		this.transaction = transaction;
	}
	
	public List<CypherError> getErrors() {
		return errors;
	}
	
	public void setErrors(List<CypherError> errors) {
		this.errors = errors;
	}

	@Override
	public String toString() {
		return "CypherResponse [commit=" + commit + ", results=" + results
				+ ", transaction=" + transaction + ", errors=" + errors
				+ "]";
	}
}
