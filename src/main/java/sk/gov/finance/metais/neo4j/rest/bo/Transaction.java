package sk.gov.finance.metais.neo4j.rest.bo;

import java.net.URI;

public class Transaction {

	private URI locationUri;
	private String commitUrl;
	
	public Transaction(URI locationUri, String commitUrl) {
		super();
		this.locationUri = locationUri;
		this.commitUrl = commitUrl;
	}

	public URI getLocationUri() {
		return this.locationUri;
	}

	public void setLocationUri(URI locationUri) {
		this.locationUri = locationUri;
	}

	public String getCommitUrl() {
		return commitUrl;
	}

	public void setCommitUrl(String commitUrl) {
		this.commitUrl = commitUrl;
	}

	@Override
	public String toString() {
		return "Transaction [locationUri=" + locationUri + ", commitUrl="
				+ commitUrl + "]";
	}
}
