package sk.gov.finance.metais.neo4j.rest.bo;

import java.util.ArrayList;
import java.util.List;

public class CypherStatement {
	
	private String statement;
	private List<String> resultDataContents;
	
	public CypherStatement() {
		resultDataContents = new ArrayList<String>();
	}
	
	public CypherStatement(String statement)
	{
		this();
		
		this.statement = statement;
	}
	
	public String getStatement() {
		return statement;
	}
	public void setStatement(String statement) {
		this.statement = statement;
	}
	public List<String> getResultDataContents() {
		return resultDataContents;
	}

	@Override
	public String toString() {
		return "CypherStatement [statement=" + statement
				+ ", resultDataContents=" + resultDataContents + "]";
	}
}
