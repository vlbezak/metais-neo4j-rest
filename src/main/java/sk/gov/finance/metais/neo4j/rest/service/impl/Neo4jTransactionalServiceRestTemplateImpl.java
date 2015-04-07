package sk.gov.finance.metais.neo4j.rest.service.impl;

import java.net.URI;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import sk.gov.finance.metais.neo4j.rest.bo.CypherRequest;
import sk.gov.finance.metais.neo4j.rest.bo.CypherResponse;
import sk.gov.finance.metais.neo4j.rest.bo.CypherStatement;
import sk.gov.finance.metais.neo4j.rest.bo.Transaction;
import sk.gov.finance.metais.neo4j.rest.exceptions.Neo4jErrorException;
import sk.gov.finance.metais.neo4j.rest.service.Neo4jTransactionalService;

public class Neo4jTransactionalServiceRestTemplateImpl implements Neo4jTransactionalService {

    private static final Logger log = LoggerFactory.getLogger(Neo4jTransactionalServiceRestTemplateImpl.class);

    @Autowired
    RestTemplate restTemplate;
    
    private String dbUrl;
    private String dbTransactionUrl;
    private String userPasswordEncoded;

    public Neo4jTransactionalServiceRestTemplateImpl(String dbUrl, String username, String password) {
    
        this.dbUrl = dbUrl;
        this.dbTransactionUrl = dbUrl + "/transaction";
        this.userPasswordEncoded = new String(Base64.encodeBase64((username + ":" + password).getBytes()));
        
        log.debug(userPasswordEncoded);
    }
    
    public CypherResponse runQuery(Transaction transaction, String cypherQuery) {
        CypherRequest request = new CypherRequest();

        request.getStatements().add(new CypherStatement(cypherQuery));

        if (log.isDebugEnabled()) {
            try {
                log.debug("Request to neo4j from runQuery:\n" + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(request));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        ResponseEntity<CypherResponse> response = restTemplate.postForEntity(transaction.getLocationUri(), request, CypherResponse.class);

        if (log.isDebugEnabled()) {
            try {
                log.debug("Response from neo4j from runQuery:\n" + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(response.getBody()));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        checkResponse(response);

        return response.getBody();
    }

    public CypherResponse runQuery(Transaction transaction, List<String> cypherQueries) {
        CypherRequest request = new CypherRequest();

        for(String cypherQuery: cypherQueries)
        {
                request.getStatements().add(new CypherStatement(cypherQuery));
        }
        
        if (log.isDebugEnabled()) {
            try {
                log.debug("Request to neo4j from runQuery:\n" + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(request));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        
        ResponseEntity<CypherResponse> response = restTemplate.postForEntity(transaction.getLocationUri(), request , CypherResponse.class);
        
        if (log.isDebugEnabled()) {
            try {
                log.debug("Response from neo4j from runQuery:\n" + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(response.getBody()));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        
        checkResponse(response);
        
        return response.getBody();
    }

    public Transaction startTransaction() {
        CypherRequest request = new CypherRequest();
        
        if (log.isDebugEnabled()) {
            try {
                log.debug("Request to neo4j from startTransaction:\n" + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(request));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        
        URI locationUri = restTemplate.postForLocation(dbTransactionUrl, request, new HashMap<String, String>());
        
        if (log.isDebugEnabled()) {
            log.debug("Response from neo4j from startTransaction: locationUri='" + locationUri + "'");
        }
        
        ResponseEntity<CypherResponse> response = restTemplate.postForEntity(locationUri, request , CypherResponse.class);
        
        if (log.isDebugEnabled()) {
            try {
                log.debug("Response from neo4j from startTransaction:\n" + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(response.getBody()));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        
        return new Transaction(locationUri, response.getBody().getCommit());
    }
    
    public void commitTransaction(Transaction transaction){
        CypherRequest request = new CypherRequest();
        
        if (log.isDebugEnabled()) {
            try {
                log.debug("Request to neo4j from commitTransaction:\n" + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(request));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        
        ResponseEntity<CypherResponse> response = restTemplate.postForEntity(transaction.getCommitUrl(), request , CypherResponse.class);
        
        if (log.isDebugEnabled()) {
            try {
                log.debug("Request to neo4j from commitTransaction:\n" + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(response.getBody()));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        
        checkResponse(response);

    }

    public void rollbackTransaction(Transaction transaction)  {
        restTemplate.delete(transaction.getLocationUri());

    }


    private void checkResponse(ResponseEntity<CypherResponse> response) {
        if (response.getBody().getErrors() != null && !response.getBody().getErrors().isEmpty()) {
            throw new Neo4jErrorException("Response has errors", response.getBody().getErrors());
        }
    }

}
