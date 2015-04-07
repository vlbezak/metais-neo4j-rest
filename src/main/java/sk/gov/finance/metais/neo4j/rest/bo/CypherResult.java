package sk.gov.finance.metais.neo4j.rest.bo;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CypherResult {
	
	private List<String> columns;
	private List<CypherData> data;
	
	public CypherResult() {
		
	}
	
	public List<String> getColumns() {
		return columns;
	}
	
	public void setColumns(List<String> columns) {
		this.columns = columns;
	}
	
	public List<CypherData> getData() {
		return data;
	}
	
	public void setData(List<CypherData> data) {
		this.data = data;
	}

	@Override
	public String toString() {
		return "CypherResult [columns=" + columns + ", data=" + data + "]";
	}
}
