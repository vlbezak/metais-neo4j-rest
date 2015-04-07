package sk.gov.finance.metais.neo4j.rest.interceptors;

import java.io.IOException;

import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;



public class LoginInterceptor implements ClientHttpRequestInterceptor{

    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
        System.out.println("Request: " + request + "\nbody: " + body);
        
        return null;
    }


}