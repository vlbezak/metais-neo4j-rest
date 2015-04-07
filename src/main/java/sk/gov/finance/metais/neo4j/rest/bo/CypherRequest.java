package sk.gov.finance.metais.neo4j.rest.bo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CypherRequest {
	
	private List<CypherStatement> statements;
	private Map<String,Object> parameters;
	
	public CypherRequest() {
		statements = new ArrayList<CypherStatement>();
		parameters = new HashMap<String, Object>();
	}

	public Map<String,Object> getParameters() {
		return parameters;
	}

	public List<CypherStatement> getStatements() {
		return statements;
	}

	@Override
	public String toString() {
		return "CypherRequest [statements=" + statements + ", parameters="
				+ parameters + "]";
	}
}
