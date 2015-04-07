package sk.gov.finance.metais.neo4j.rest.bo;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CypherData {

	private List<Object> row;
	private List<Object> graph;
	private List<Object> rest;
	
	public CypherData() {
		
	}

	public List<Object> getRow() {
		return row;
	}

	public void setRow(List<Object> row) {
		this.row = row;
	}

	public List<Object> getGraph() {
		return graph;
	}

	public void setGraph(List<Object> graph) {
		this.graph = graph;
	}

	public List<Object> getRest() {
		return rest;
	}

	public void setRest(List<Object> rest) {
		this.rest = rest;
	}

	@Override
	public String toString() {
		return "CypherData [row=" + row + ", graph=" + graph + ", rest=" + rest
				+ "]";
	}
}
