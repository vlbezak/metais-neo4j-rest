package sk.gov.finance.metais.neo4j.rest.bo;

public class Neo4jTransactionIdentifier {
    private String id;

    public Neo4jTransactionIdentifier(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

}
