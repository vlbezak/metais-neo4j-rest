package sk.gov.finance.metais.neo4j.rest.service.impl;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;

import org.apache.commons.codec.binary.Base64;
import org.apache.wink.client.ClientConfig;
import org.apache.wink.client.ClientResponse;
import org.apache.wink.client.Resource;
import org.apache.wink.client.RestClient;
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
//import org.codehaus.jackson.map.AnnotationIntrospector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

@Component
public class Neo4jTransactionalServiceWinkImpl implements Neo4jTransactionalService {

    private static final Logger log = LoggerFactory.getLogger(Neo4jTransactionalServiceJaxRsImpl.class);

    private RestClient client;
    private String dbUrl;
    private String dbTransactionUrl;
    private String userPasswordEncoded;

    public Neo4jTransactionalServiceWinkImpl(String dbUrl, String username, String password) {

       this.dbUrl = dbUrl;
        this.dbTransactionUrl = dbUrl + "/transaction";
        this.userPasswordEncoded = new String(Base64.encodeBase64((username + ":" + password).getBytes()));
        
        ClientConfig clientConfig = new ClientConfig();
        
        Application app = new Application() {
            public Set<Class<?>> getClasses() {
                Set<Class<?>> classes = new HashSet<Class<?>>();
                classes.add(JacksonJsonProvider.class);
                return classes;
            }
        };
        clientConfig.applications(app);

        client = new RestClient(clientConfig);
    }

    public CypherResponse runQuery(Transaction transaction, String cypherQuery){
        CypherRequest request = new CypherRequest();

        request.getStatements().add(new CypherStatement(cypherQuery));

        Resource resourceTarget = client.resource(transaction.getLocationUri());

        if (log.isDebugEnabled()) {
            try {
                log.debug("Request to neo4j from runQuery - transactionUrl:" + transaction.getLocationUri() + " :\n"
                        + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(request));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        ClientResponse response = resourceTarget.accept(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + userPasswordEncoded)
                .header("Content-Type", MediaType.APPLICATION_JSON).post(request);

        if (log.isDebugEnabled()) {
            log.debug("Response:" + response);
        }

        CypherResponse body = response.getEntity(CypherResponse.class);
       

        if (log.isDebugEnabled()) {
            try {
                log.debug("Response from neo4j from runQuery:\n" + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(body));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        checkResponse(body);

        return body;
    }

    public CypherResponse runQuery(Transaction transaction, List<String> cypherQueries) {
        CypherRequest request = new CypherRequest();
        for (String cypherQuery : cypherQueries) {
            request.getStatements().add(new CypherStatement(cypherQuery));
        }

        Resource resourceTarget = client.resource(transaction.getLocationUri());

        if (log.isDebugEnabled()) {
            try {
                log.debug("Request to neo4j from runQuery - transactionUrl:" + transaction.getLocationUri() + " :\n"
                        + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(request));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        ClientResponse response = resourceTarget.accept(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + userPasswordEncoded)
                .header("Content-Type", MediaType.APPLICATION_JSON).post(request);

        if (log.isDebugEnabled()) {
            log.debug("Response:" + response);
        }

        CypherResponse body = response.getEntity(CypherResponse.class);

        if (log.isDebugEnabled()) {
            try {
                log.debug("Response from neo4j from runQuery:\n" + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(body));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        checkResponse(body);

        return body;
    }

    public Transaction startTransaction(){
        CypherRequest request = new CypherRequest();

        Resource resourceTarget = client.resource(dbTransactionUrl);
        
        if (log.isDebugEnabled()) {
            try {
                log.debug("Request to neo4j from startTransaction - transactionUrl:" + dbTransactionUrl + " :\n"
                        + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(request));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        ClientResponse response = resourceTarget.accept(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + userPasswordEncoded)
                .header("Content-Type", MediaType.APPLICATION_JSON).post(request);

        if (log.isDebugEnabled()) {
            log.debug("Response:" + response);
        }

        CypherResponse body = response.getEntity(CypherResponse.class);

        String locationUri = (String) response.getHeaders().getFirst("Location");

        if (log.isDebugEnabled()) {
            try {
                log.debug("Response from neo4j from startTransaction:\n" + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(body)
                        + " location:" + locationUri);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        return new Transaction(URI.create(locationUri), body.getCommit());

    }

    public void commitTransaction(Transaction transaction){
        CypherRequest request = new CypherRequest();

        Resource resourceTarget = client.resource(transaction.getCommitUrl());
        if (log.isDebugEnabled()) {
            try {
                log.debug("Request to neo4j from commitTransaction - transactionUrl: " + dbTransactionUrl + ":\n"
                        + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(request));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        ClientResponse response = resourceTarget.accept(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + userPasswordEncoded)
                .header("Content-Type", MediaType.APPLICATION_JSON).post(request);

        CypherResponse body = response.getEntity(CypherResponse.class);

        if (log.isDebugEnabled()) {
            try {
                log.debug("Response to neo4j from commitTransaction:\n" + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(body));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        checkResponse(body);

    }

    public void rollbackTransaction(Transaction transaction) {
        Resource resourceTarget = client.resource(transaction.getLocationUri());

        ClientResponse response = resourceTarget.accept(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + userPasswordEncoded)
                .header("Content-Type", MediaType.APPLICATION_JSON).post("");

        if (log.isDebugEnabled()) {
            log.debug("Response:" + response);
        }

    }

    private void checkResponse(CypherResponse body) {
        if (body.getErrors() != null && !body.getErrors().isEmpty()) {
            throw new Neo4jErrorException("Response has errors", body.getErrors());
        }
    }

}
