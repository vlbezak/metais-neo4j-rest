package sk.gov.finance.metais.neo4j.rest.service.impl;

import java.net.URI;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import sk.gov.finance.metais.neo4j.rest.bo.CypherRequest;
import sk.gov.finance.metais.neo4j.rest.bo.CypherResponse;
import sk.gov.finance.metais.neo4j.rest.bo.CypherStatement;
import sk.gov.finance.metais.neo4j.rest.bo.Transaction;
import sk.gov.finance.metais.neo4j.rest.exceptions.Neo4jErrorException;
import sk.gov.finance.metais.neo4j.rest.service.Neo4jTransactionalService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

@Component
public class Neo4jTransactionalServiceJaxRsImpl implements Neo4jTransactionalService {

    private static final Logger log = LoggerFactory.getLogger(Neo4jTransactionalServiceJaxRsImpl.class);

    private Client client;
    private String dbUrl;
    private String dbTransactionUrl;
    private String userPasswordEncoded;

    public Neo4jTransactionalServiceJaxRsImpl(String dbUrl, String username, String password) {

        client = ClientBuilder.newClient();
        client.register(JacksonJsonProvider.class);

        this.dbUrl = dbUrl;
        this.dbTransactionUrl = dbUrl + "/transaction";
        this.userPasswordEncoded = new String(Base64.encodeBase64((username + ":" + password).getBytes()));

    }

    public CypherResponse runQuery(Transaction transaction, String cypherQuery) throws JsonProcessingException {
        CypherRequest request = new CypherRequest();

        request.getStatements().add(new CypherStatement(cypherQuery));

        WebTarget resourceTarget = client.target(transaction.getLocationUri());

        if (log.isDebugEnabled()) {
            log.debug("Request to neo4j from runQuery - transactionUrl:" + transaction.getLocationUri() + " :\n"
                    + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(request));
        }
        Response response = resourceTarget.request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + userPasswordEncoded)
                .header("Content-Type", "application/json; charset=UTF-8").buildPost(Entity.json(request)).invoke();

        if (log.isDebugEnabled()) {
            log.debug("Response:" + response);
        }

        CypherResponse body = response.readEntity(CypherResponse.class);
        response.close();

        if (log.isDebugEnabled()) {
            log.debug("Response from neo4j from runQuery:\n" + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(body));
        }
        checkResponse(body);

        return body;
    }

    public CypherResponse runQuery(Transaction transaction, List<String> cypherQueries) throws JsonProcessingException {
        CypherRequest request = new CypherRequest();
        for (String cypherQuery : cypherQueries) {
            request.getStatements().add(new CypherStatement(cypherQuery));
        }

        WebTarget resourceTarget = client.target(transaction.getLocationUri());

        if (log.isDebugEnabled()) {
            log.debug("Request to neo4j from runQuery - transactionUrl:" + transaction.getLocationUri() + " :\n"
                    + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(request));
        }
        Response response = resourceTarget.request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + userPasswordEncoded)
                .header("Content-Type", "application/json; charset=UTF-8").buildPost(Entity.json(request)).invoke();

        if (log.isDebugEnabled()) {
            log.debug("Response:" + response);
        }

        CypherResponse body = response.readEntity(CypherResponse.class);
        response.close();

        if (log.isDebugEnabled()) {
            log.debug("Response from neo4j from runQuery:\n" + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(body));
        }
        checkResponse(body);

        return body;
    }

    public Transaction startTransaction() throws JsonProcessingException {
        CypherRequest request = new CypherRequest();

        WebTarget resourceTarget = client.target(dbTransactionUrl);

        Response response = resourceTarget.request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + userPasswordEncoded)
                .header("Content-Type", "application/json; charset=UTF-8").buildPost(Entity.json(request)).invoke();

        if (log.isDebugEnabled()) {
            log.debug("Response:" + response);
        }

        CypherResponse body = response.readEntity(CypherResponse.class);

        String locationUri = (String) response.getHeaders().getFirst("Location");

        response.close();

        if (log.isDebugEnabled()) {
            log.debug("Response from neo4j from startTransaction:\n" + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(body)
                    + " location:" + locationUri);
        }

        return new Transaction(URI.create(locationUri), body.getCommit());

    }

    public void commitTransaction(Transaction transaction) throws JsonProcessingException {
        CypherRequest request = new CypherRequest();

        WebTarget resourceTarget = client.target(transaction.getCommitUrl());
        if (log.isDebugEnabled()) {
            log.debug("Request to neo4j from commitTransaction - transactionUrl: " + dbTransactionUrl + ":\n"
                    + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(request));
        }

        Response response = resourceTarget.request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + userPasswordEncoded)
                .header("Content-Type", "application/json; charset=UTF-8").buildPost(Entity.json(request)).invoke();

        CypherResponse body = response.readEntity(CypherResponse.class);

        response.close();

        if (log.isDebugEnabled()) {
            log.debug("Response to neo4j from commitTransaction:\n" + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(body));
        }

        checkResponse(body);

    }

    public void rollbackTransaction(Transaction transaction) {
        WebTarget resourceTarget = client.target(transaction.getLocationUri());

        Response response = resourceTarget.request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + userPasswordEncoded)
                .header("Content-Type", "application/json; charset=UTF-8").buildDelete().invoke();

        if (log.isDebugEnabled()) {
            log.debug("Response:" + response);
        }
        response.close();

    }

    private void checkResponse(CypherResponse body) {
        if (body.getErrors() != null && !body.getErrors().isEmpty()) {
            throw new Neo4jErrorException("Response has errors", body.getErrors());
        }
    }

}
