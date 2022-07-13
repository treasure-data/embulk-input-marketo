package org.embulk.input.marketo.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpResponseException;
import org.eclipse.jetty.client.api.ContentProvider;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.embulk.config.ConfigException;
import org.embulk.input.marketo.exception.MarketoAPIException;
import org.embulk.input.marketo.model.MarketoAccessTokenResponse;
import org.embulk.input.marketo.model.MarketoError;
import org.embulk.spi.DataException;
import org.embulk.util.retryhelper.jetty92.Jetty92ResponseReader;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.embulk.util.retryhelper.jetty92.Jetty92SingleRequester;
import org.embulk.util.retryhelper.jetty92.StringJetty92ResponseEntityReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static org.embulk.input.marketo.rest.MarketoResponseJetty92EntityReader.jsonResponseInvalid;

/**
 * Marketo base rest client
 * Created by tai.khuu on 9/7/17.
 */
public class MarketoBaseRestClient implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MarketoBaseRestClient.class);

    private static final String APPLICATION_JSON = "application/json";

    private static final String AUTHORIZATION_HEADER = "Authorization";

    private String identityEndPoint;

    private String clientId;

    private String clientSecret;

    private String accountId;

    private String accessToken;

    private int marketoLimitIntervalMillis;

    private Jetty92RetryHelper retryHelper;

    protected long readTimeoutMillis;

    private Optional<String> partnerApiKey;

    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false).configure(ALLOW_UNQUOTED_CONTROL_CHARS, false);

    MarketoBaseRestClient(String identityEndPoint,
                          String clientId,
                          String clientSecret,
                          String accountId,
                          Optional<String> partnerApiKey,
                          int marketoLimitIntervalMillis,
                          long readTimeoutMillis,
                          Jetty92RetryHelper retryHelper)
    {
        this.identityEndPoint = identityEndPoint;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.accountId = accountId;
        this.readTimeoutMillis = readTimeoutMillis;
        this.retryHelper = retryHelper;
        this.marketoLimitIntervalMillis = marketoLimitIntervalMillis;
        this.partnerApiKey = partnerApiKey;
    }

    private void renewAccessToken()
    {
        accessToken = getAccessTokenWithWrappedException();
    }

    @VisibleForTesting
    public String getAccessToken()
    {
        if (accessToken == null) {
            synchronized (this) {
                if (accessToken == null) {
                    accessToken = getAccessTokenWithWrappedException();
                }
            }
        }
        return accessToken;
    }

    private String requestAccessToken()
    {
        final Multimap<String, String> params = ArrayListMultimap.create();
        params.put("client_id", clientId.trim());
        params.put("client_secret", clientSecret.trim());
        params.put("grant_type", "client_credentials");

        // add partner api key to the request
        if (partnerApiKey.isPresent()) {
            LOGGER.info("> Request access_token with partner_id: {}", StringUtils.abbreviate(partnerApiKey.get(), 8));
            params.put("partner_id", partnerApiKey.get());
        }

        String response = retryHelper.requestWithRetry(new StringJetty92ResponseEntityReader(readTimeoutMillis), new Jetty92SingleRequester()
        {
            @Override
            public void requestOnce(HttpClient client, Response.Listener responseListener)
            {
                Request request = client.newRequest(identityEndPoint + MarketoRESTEndpoint.ACCESS_TOKEN.getEndpoint()).method(HttpMethod.GET);
                for (String key : params.keySet()) {
                    for (String value : params.get(key)) {
                        request.param(key, value);
                    }
                }
                request.send(responseListener);
            }

            @Override
            protected boolean isResponseStatusToRetry(Response response)
            {
                return response.getStatus() == 502;
            }

            @Override
            protected boolean isExceptionToRetry(Exception exception)
            {
                if (exception instanceof TimeoutException || exception instanceof SocketTimeoutException || exception instanceof EOFException || super.isExceptionToRetry(exception)) {
                    return true;
                }
                // unwrap
                if (exception instanceof ExecutionException || (exception instanceof IOException && exception.getCause() != null)) {
                    return this.toRetry((Exception) exception.getCause());
                }
                return false;
            }
        });

        MarketoAccessTokenResponse accessTokenResponse;

        try {
            accessTokenResponse = OBJECT_MAPPER.readValue(response, MarketoAccessTokenResponse.class);
        }
        catch (IOException e) {
            LOGGER.error("Exception when parse access token response", e);
            throw new DataException("Can't parse access token response");
        }
        if (accessTokenResponse.hasError()) {
            throw new DataException(accessTokenResponse.getErrorDescription());
        }
        LOGGER.info("Acquired new access token");
        return accessTokenResponse.getAccessToken();
    }

    protected <T> T doGet(final String target, final Map<String, String> headers, final Multimap<String, String> params, Jetty92ResponseReader<T> responseReader)
    {
        return doRequestWithWrappedException(target, HttpMethod.GET, headers, params, null, responseReader);
    }

    protected <T> T doPost(final String target, final Map<String, String> headers, final Multimap<String, String> params, final String content, Jetty92ResponseReader<T> responseReader)
    {
        StringContentProvider contentProvider = null;
        if (content != null) {
            contentProvider = new StringContentProvider(APPLICATION_JSON, content, StandardCharsets.UTF_8);
        }
        return doPost(target, headers, params, responseReader, contentProvider);
    }

    protected <T> T doPost(final String target, final Map<String, String> headers, final Multimap<String, String> params, Jetty92ResponseReader<T> responseReader, final ContentProvider content)
    {
        return doRequestWithWrappedException(target, HttpMethod.POST, headers, params, content, responseReader);
    }

    private String getAccessTokenWithWrappedException()
    {
        try {
            return requestAccessToken();
        }
        catch (Exception e) {
            if (e instanceof HttpResponseException) {
                throw new ConfigException(e.getMessage());
            }
            if (e.getCause() instanceof HttpResponseException) {
                throw new ConfigException(e.getCause().getMessage());
            }
            throw e;
        }
    }

    private <T> T doRequestWithWrappedException(final String target, final HttpMethod method, final Map<String, String> headers, final Multimap<String, String> params, final ContentProvider contentProvider, Jetty92ResponseReader<T> responseReader)
    {
        try {
            return doRequest(target, method, headers, params, contentProvider, responseReader);
        }
        catch (Exception e) {
            if (e instanceof MarketoAPIException || e instanceof HttpResponseException) {
                throw new DataException(e.getMessage());
            }
            if (e.getCause() instanceof MarketoAPIException || e.getCause() instanceof HttpResponseException) {
                throw new DataException(e.getCause().getMessage());
            }
            throw e;
        }
    }

    protected <T> T doRequest(final String target, final HttpMethod method, final Map<String, String> headers, final Multimap<String, String> params, final ContentProvider contentProvider, Jetty92ResponseReader<T> responseReader)
    {
        return retryHelper.requestWithRetry(responseReader, new Jetty92SingleRequester()
        {
            @Override
            public void requestOnce(HttpClient client, Response.Listener responseListener)
            {
                Request request = client.newRequest(target).method(method);
                if (headers != null) {
                    for (String key : headers.keySet()) {
                        request.header(key, headers.get(key));
                    }
                }
                request.header(AUTHORIZATION_HEADER, "Bearer " + getAccessToken());
                if (params != null) {
                    for (String key : params.keySet()) {
                        for (String value : params.get(key)) {
                            request.param(key, value);
                        }
                    }
                }
                LOGGER.info("CALLING Account ID: {} {} -> {} - params: {}", accountId, method, target, params);
                if (contentProvider != null) {
                    request.content(contentProvider);
                }
                request.send(responseListener);
            }

            @Override
            protected boolean isResponseStatusToRetry(Response response)
            {
                //413 failed job
                //414 failed job
                //502 retry
                return response.getStatus() / 4 != 100;
            }

            @Override
            protected boolean isExceptionToRetry(Exception exception)
            {
                if (exception instanceof EOFException || exception instanceof TimeoutException || exception instanceof SocketTimeoutException || super.isExceptionToRetry(exception)) {
                    return true;
                }
                if (exception instanceof ExecutionException || (exception instanceof IOException && exception.getCause() != null)) {
                    return this.toRetry((Exception) exception.getCause());
                }
                if (exception instanceof MarketoAPIException) {
                    //Retry Authenticate Exception
                    MarketoError error = ((MarketoAPIException) exception).getMarketoErrors().get(0);
                    String code = error.getCode();
                    switch (code) {
                        case "602":
                        case "601":
                            LOGGER.info("Access token expired");
                            renewAccessToken();
                            return true;
                        case "606":
                            try {
                                Thread.sleep(marketoLimitIntervalMillis);
                            }
                            catch (InterruptedException e) {
                                LOGGER.error("Encounter exception when waiting for interval limit", e);
                                throw new DataException("Exception when wait for interval limit");
                            }
                            return true;
                        case "604":
                        case "608":
                        case "611":
                        case "615":
                        case "713":
                        case "1029":
                            return true;
                        default:
                            return false;
                    }
                }
                //retry in case request return data but invalid format
                if ((exception instanceof DataException) && exception.getMessage().equals(jsonResponseInvalid)) {
                    return true;
                }
                return false;
            }
        });
    }

    @Override
    public void close()
    {
        if (retryHelper != null) {
            retryHelper.close();
        }
    }
}
