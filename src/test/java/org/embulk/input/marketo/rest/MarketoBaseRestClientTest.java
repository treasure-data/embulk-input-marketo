package org.embulk.input.marketo.rest;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpResponseException;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.input.marketo.exception.MarketoAPIException;
import org.embulk.input.marketo.model.MarketoError;
import org.embulk.input.marketo.model.MarketoResponse;
import org.embulk.spi.DataException;
import org.embulk.util.retryhelper.jetty94.DefaultJetty94ClientCreator;
import org.embulk.util.retryhelper.jetty94.Jetty94ClientCreator;
import org.embulk.util.retryhelper.jetty94.Jetty94RetryHelper;
import org.embulk.util.retryhelper.jetty94.Jetty94SingleRequester;
import org.embulk.util.retryhelper.jetty94.StringJetty94ResponseEntityReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.embulk.input.marketo.rest.MarketoResponseJettyEntityReader.jsonResponseInvalid;

/**
 * Created by tai.khuu on 9/21/17.
 */
public class MarketoBaseRestClientTest
{
    private static final String IDENTITY_END_POINT = "identityEndPoint";

    private static final int MARKETO_LIMIT_INTERVAL_MILIS = 1000;

    private MarketoBaseRestClient marketoBaseRestClient;

    private Jetty94RetryHelper mockJetty94;

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    @Before
    public void prepare()
    {
        mockJetty94 = Mockito.mock(Jetty94RetryHelper.class);
        marketoBaseRestClient = new MarketoBaseRestClient("identityEndPoint", "clientId", "clientSecret", "accountId", Optional.empty(), MARKETO_LIMIT_INTERVAL_MILIS, 60000, mockJetty94);
    }

    @Test
    public void testGetAccessToken()
    {
        Mockito.when(mockJetty94.requestWithRetry(Mockito.any(StringJetty94ResponseEntityReader.class), Mockito.any(Jetty94SingleRequester.class))).thenReturn("{\n" +
                "    \"access_token\": \"access_token\",\n" +
                "    \"token_type\": \"bearer\",\n" +
                "    \"expires_in\": 3599,\n" +
                "    \"scope\": \"tai@treasure-data.com\"\n" +
                "}");
        String accessToken = marketoBaseRestClient.getAccessToken();
        Assert.assertEquals("access_token", accessToken);
    }

    @Test
    public void testGetAccessTokenRequester()
    {
        ArgumentCaptor<Jetty94SingleRequester> jetty94SingleRequesterArgumentCaptor = ArgumentCaptor.forClass(Jetty94SingleRequester.class);
        Mockito.when(mockJetty94.requestWithRetry(Mockito.any(StringJetty94ResponseEntityReader.class), jetty94SingleRequesterArgumentCaptor.capture())).thenReturn("{\"access_token\": \"access_token\"}");
        String accessToken = marketoBaseRestClient.getAccessToken();
        Assert.assertEquals("access_token", accessToken);
        Jetty94SingleRequester jetty94SingleRequester = jetty94SingleRequesterArgumentCaptor.getValue();
        HttpClient client = Mockito.mock(HttpClient.class);
        Response.Listener listener = Mockito.mock(Response.Listener.class);
        Request mockRequest = Mockito.mock(Request.class);
        Mockito.when(client.newRequest(Mockito.eq(IDENTITY_END_POINT + MarketoRESTEndpoint.ACCESS_TOKEN.getEndpoint()))).thenReturn(mockRequest);
        Request request1 = Mockito.mock(Request.class);
        Mockito.when(mockRequest.method(Mockito.eq(HttpMethod.GET))).thenReturn(request1);
        jetty94SingleRequester.requestOnce(client, listener);
        Mockito.verify(request1, Mockito.times(1)).param(Mockito.eq("client_id"), Mockito.eq("clientId"));
        Mockito.verify(request1, Mockito.times(1)).param(Mockito.eq("client_secret"), Mockito.eq("clientSecret"));
        Mockito.verify(request1, Mockito.times(1)).param(Mockito.eq("grant_type"), Mockito.eq("client_credentials"));

        // By default the partner id is not set
        Mockito.verify(request1, Mockito.never()).param(Mockito.eq("partner_id"), Mockito.anyString());

        Assert.assertTrue(jetty94SingleRequester.toRetry(createHttpResponseException(502)));
        Assert.assertTrue(jetty94SingleRequester.toRetry(new ExecutionException(new TimeoutException())));
        Assert.assertTrue(jetty94SingleRequester.toRetry(new ExecutionException(new EOFException())));
        Assert.assertTrue(jetty94SingleRequester.toRetry(new ExecutionException(new SocketTimeoutException())));
        // Retry SocketTimeoutException, TimeoutException and EOFException
        Assert.assertTrue(jetty94SingleRequester.toRetry(new SocketTimeoutException()));
        Assert.assertTrue(jetty94SingleRequester.toRetry(new TimeoutException()));
        Assert.assertTrue(jetty94SingleRequester.toRetry(new EOFException()));
        // When EOFException is wrapped in IOException it should be retried too
        Assert.assertTrue(jetty94SingleRequester.toRetry(new IOException(new EOFException())));
        // Retry TimeoutException when it is wrapped in IOException
        Assert.assertTrue(jetty94SingleRequester.toRetry(new IOException(new TimeoutException())));
    }

    @Test
    public void testGetAccessTokenRequestShouldHavePartnerId()
    {
        final String partnerId = "sample_partner_id";
        mockJetty94 = Mockito.mock(Jetty94RetryHelper.class);
        ArgumentCaptor<Jetty94SingleRequester> jetty94SingleRequesterArgumentCaptor = ArgumentCaptor.forClass(Jetty94SingleRequester.class);
        Mockito.when(mockJetty94.requestWithRetry(Mockito.any(StringJetty94ResponseEntityReader.class), jetty94SingleRequesterArgumentCaptor.capture())).thenReturn("{\"access_token\": \"access_token\"}");

        MarketoBaseRestClient restClient = Mockito.spy(new MarketoBaseRestClient("identityEndPoint",
                "clientId",
                "clientSecret",
                "accountId",
                Optional.of(partnerId),
                MARKETO_LIMIT_INTERVAL_MILIS,
                60000,
                mockJetty94));

        // call method for evaluation
        restClient.getAccessToken();

        Jetty94SingleRequester singleRequester = jetty94SingleRequesterArgumentCaptor.getValue();

        HttpClient client = Mockito.mock(HttpClient.class);
        Request request = Mockito.mock(Request.class);

        Request mockRequest = Mockito.mock(Request.class);
        Mockito.when(client.newRequest(Mockito.eq(IDENTITY_END_POINT + MarketoRESTEndpoint.ACCESS_TOKEN.getEndpoint()))).thenReturn(mockRequest);
        Mockito.when(mockRequest.method(Mockito.eq(HttpMethod.GET))).thenReturn(request);
        singleRequester.requestOnce(client, Mockito.mock(Response.Listener.class));

        Mockito.verify(request, Mockito.times(1)).param(Mockito.eq("client_id"), Mockito.eq("clientId"));
        Mockito.verify(request, Mockito.times(1)).param(Mockito.eq("client_secret"), Mockito.eq("clientSecret"));
        Mockito.verify(request, Mockito.times(1)).param(Mockito.eq("grant_type"), Mockito.eq("client_credentials"));
        Mockito.verify(request, Mockito.times(1)).param(Mockito.eq("partner_id"), Mockito.eq(partnerId));
    }

    @Test
    public void testGetAccessTokenWithError()
    {
        ArgumentCaptor<Jetty94SingleRequester> jetty94SingleRequesterArgumentCaptor = ArgumentCaptor.forClass(Jetty94SingleRequester.class);
        Mockito.when(mockJetty94.requestWithRetry(Mockito.any(StringJetty94ResponseEntityReader.class), jetty94SingleRequesterArgumentCaptor.capture())).thenReturn("{\n" +
                "    \"error\": \"invalid_client\",\n" +
                "    \"error_description\": \"Bad client credentials\"\n" +
                "}");
        try {
            marketoBaseRestClient.getAccessToken();
        }
        catch (DataException ex) {
            Assert.assertEquals("Bad client credentials", ex.getMessage());
            return;
        }
        Assert.fail();
    }

    @Test
    public void testGetAccessTokenThrowHttpResponseException() throws Exception
    {
        HttpClient client = Mockito.mock(HttpClient.class);

        Jetty94ClientCreator clientCreator = Mockito.mock(Jetty94ClientCreator.class);
        Mockito.doReturn(client).when(clientCreator).createAndStart();

        Request request = Mockito.mock(Request.class);
        Mockito.doReturn(request).when(client).newRequest(Mockito.anyString());
        Mockito.doReturn(request).when(request).method(HttpMethod.GET);

        HttpResponseException exception = new HttpResponseException("{\"error\":\"invalid_client\",\"error_description\":\"Bad client credentials\"}", Mockito.mock(Response.class));
        Mockito.doThrow(exception).when(request).send(Mockito.any(Response.Listener.class));

        Jetty94RetryHelper retryHelper = new Jetty94RetryHelper(1, 1, 1, clientCreator);
        final MarketoBaseRestClient restClient = new MarketoBaseRestClient("identityEndPoint", "clientId", "clientSecret", "accountId", Optional.empty(), MARKETO_LIMIT_INTERVAL_MILIS, 1000, retryHelper);

        // calling method should wrap the HttpResponseException by ConfigException
        Assert.assertThrows(ConfigException.class, restClient::getAccessToken);
    }

    @Test
    public void tetDoGetThrowHttpResponseException() throws Exception
    {
        final MarketoBaseRestClient client = doRequestWithWrapper(HttpMethod.GET);
        // calling method should wrap the HttpResponseException by DataException
        Assert.assertThrows(DataException.class, () -> client.doGet("test_target", null, null, new MarketoResponseJettyEntityReader<String>(1000)));
    }

    @Test
    public void tetDoPostThrowHttpResponseException() throws Exception
    {
        final MarketoBaseRestClient client = doRequestWithWrapper(HttpMethod.POST);
        // calling method should wrap the HttpResponseException by DataException
        Assert.assertThrows(DataException.class, () -> client.doPost("test_target", null, null, "{\"any\": \"any\"}", new MarketoResponseJettyEntityReader<String>(1000)));
    }

    private MarketoBaseRestClient doRequestWithWrapper(HttpMethod method) throws Exception
    {
        HttpClient client = Mockito.mock(HttpClient.class);

        Jetty94ClientCreator clientCreator = Mockito.mock(Jetty94ClientCreator.class);
        Mockito.doReturn(client).when(clientCreator).createAndStart();

        Request request = Mockito.mock(Request.class);
        Mockito.doReturn(request).when(client).newRequest(Mockito.anyString());
        Mockito.doReturn(request).when(request).method(method);

        HttpResponseException exception = new HttpResponseException("{\"error\":\"1035\",\"error_description\":\"Unsupported filter type for target subscription: updatedAt\"}", Mockito.mock(Response.class));
        Mockito.doThrow(exception).when(request).send(Mockito.any(Response.Listener.class));

        Jetty94RetryHelper retryHelper = new Jetty94RetryHelper(1, 1, 1, clientCreator);
        final MarketoBaseRestClient restClient = Mockito.spy(new MarketoBaseRestClient("identityEndPoint", "clientId", "clientSecret", "accountId", Optional.empty(), MARKETO_LIMIT_INTERVAL_MILIS, 1000, retryHelper));
        Mockito.doReturn("test_access_token").when(restClient).getAccessToken();

        return restClient;
    }

    @Test
    public void testDoPost()
    {
        MarketoBaseRestClient spy = Mockito.spy(marketoBaseRestClient);
        spy.doPost("target", Maps.newHashMap(), new ImmutableListMultimap.Builder<String, String>().build(), "test_content", new StringJetty94ResponseEntityReader(10));
        Mockito.verify(spy, Mockito.times(1)).doRequest(Mockito.anyString(), Mockito.eq(HttpMethod.POST), Mockito.any(Map.class), Mockito.any(Multimap.class), Mockito.any(StringContentProvider.class), Mockito.any(StringJetty94ResponseEntityReader.class));
    }

    @Test
    public void testDoGet()
    {
        MarketoBaseRestClient spy = Mockito.spy(marketoBaseRestClient);
        spy.doGet("target", Maps.newHashMap(), new ImmutableListMultimap.Builder<String, String>().build(), new StringJetty94ResponseEntityReader(10));
        Mockito.verify(spy, Mockito.times(1)).doRequest(Mockito.anyString(), Mockito.eq(HttpMethod.GET), Mockito.any(Map.class), Mockito.any(Multimap.class), Mockito.isNull(), Mockito.any(StringJetty94ResponseEntityReader.class));
    }

    @Test
    public void testDoRequestRequester()
    {
        MarketoBaseRestClient spy = Mockito.spy(marketoBaseRestClient);
        StringContentProvider contentProvider = new StringContentProvider("Content", StandardCharsets.UTF_8);
        ArgumentCaptor<Jetty94SingleRequester> jetty94SingleRequesterArgumentCaptor = ArgumentCaptor.forClass(Jetty94SingleRequester.class);

        MarketoResponse<Object> expectedMarketoResponse = new MarketoResponse<>();

        Mockito.when(mockJetty94.requestWithRetry(Mockito.any(MarketoResponseJettyEntityReader.class), jetty94SingleRequesterArgumentCaptor.capture())).thenReturn(expectedMarketoResponse);
        Mockito.when(mockJetty94.requestWithRetry(Mockito.any(StringJetty94ResponseEntityReader.class), Mockito.any(Jetty94SingleRequester.class))).thenReturn("{\"access_token\": \"access_token\"}");

        String target = "target";
        HashMap<String, String> headers = Maps.newHashMap();
        headers.put("testHeader1", "testHeaderValue1");

        ImmutableListMultimap<String, String> build = new ImmutableListMultimap.Builder<String, String>().put("param", "param1").build();

        MarketoResponse<Object> marketoResponse = spy.doRequest(target, HttpMethod.POST, headers, build, contentProvider, new MarketoResponseJettyEntityReader<>(10));

        HttpClient client = Mockito.mock(HttpClient.class);
        Response.Listener listener = Mockito.mock(Response.Listener.class);
        Request mockRequest = Mockito.mock(Request.class);
        Mockito.when(client.newRequest(Mockito.eq(target))).thenReturn(mockRequest);

        Mockito.when(mockRequest.method(Mockito.eq(HttpMethod.POST))).thenReturn(mockRequest);
        Jetty94SingleRequester jetty94SingleRequester = jetty94SingleRequesterArgumentCaptor.getValue();
        jetty94SingleRequester.requestOnce(client, listener);

        Assert.assertEquals(expectedMarketoResponse, marketoResponse);

        Mockito.verify(mockRequest, Mockito.times(1)).header(Mockito.eq("testHeader1"), Mockito.eq("testHeaderValue1"));
        Mockito.verify(mockRequest, Mockito.times(1)).header(Mockito.eq("Authorization"), Mockito.eq("Bearer access_token"));
        Mockito.verify(mockRequest, Mockito.times(1)).param(Mockito.eq("param"), Mockito.eq("param1"));
        Mockito.verify(mockRequest, Mockito.times(1)).content(Mockito.eq(contentProvider));
    }

    @Test
    public void testDoRequesterRetry()
    {
        MarketoBaseRestClient spy = Mockito.spy(marketoBaseRestClient);
        ArgumentCaptor<Jetty94SingleRequester> jetty94SingleRequesterArgumentCaptor = ArgumentCaptor.forClass(Jetty94SingleRequester.class);

        Mockito.when(mockJetty94.requestWithRetry(Mockito.any(MarketoResponseJettyEntityReader.class), jetty94SingleRequesterArgumentCaptor.capture())).thenReturn(new MarketoResponse<>());
        Mockito.when(mockJetty94.requestWithRetry(Mockito.any(StringJetty94ResponseEntityReader.class), Mockito.any(Jetty94SingleRequester.class))).thenReturn("{\"access_token\": \"access_token\"}");

        spy.doRequest("", HttpMethod.POST, null, null, null, new MarketoResponseJettyEntityReader<>(10));

        HttpClient client = Mockito.mock(HttpClient.class);
        Response.Listener listener = Mockito.mock(Response.Listener.class);
        Request mockRequest = Mockito.mock(Request.class);
        Mockito.when(client.newRequest(Mockito.anyString())).thenReturn(mockRequest);

        Mockito.when(mockRequest.method(Mockito.eq(HttpMethod.POST))).thenReturn(mockRequest);

        Jetty94SingleRequester jetty94SingleRequester = jetty94SingleRequesterArgumentCaptor.getValue();
        jetty94SingleRequester.requestOnce(client, listener);
        Assert.assertTrue(jetty94SingleRequester.toRetry(createHttpResponseException(502)));

        Assert.assertFalse(jetty94SingleRequester.toRetry(createHttpResponseException(400)));

        Assert.assertFalse(jetty94SingleRequester.toRetry(createMarketoAPIException("ERR", "ERR")));
        Assert.assertTrue(jetty94SingleRequester.toRetry(createMarketoAPIException("606", "")));
        Assert.assertTrue(jetty94SingleRequester.toRetry(createMarketoAPIException("615", "")));
        Assert.assertTrue(jetty94SingleRequester.toRetry(createMarketoAPIException("602", "")));
        // Should retry 601 error too
        Assert.assertTrue(jetty94SingleRequester.toRetry(createMarketoAPIException("601", "")));
        Assert.assertTrue(jetty94SingleRequester.toRetry(createMarketoAPIException("604", "")));
        Assert.assertTrue(jetty94SingleRequester.toRetry(createMarketoAPIException("608", "")));
        Assert.assertTrue(jetty94SingleRequester.toRetry(createMarketoAPIException("611", "")));
        Assert.assertTrue(jetty94SingleRequester.toRetry(createMarketoAPIException("615", "")));
        Assert.assertTrue(jetty94SingleRequester.toRetry(createMarketoAPIException("713", "")));
        Assert.assertTrue(jetty94SingleRequester.toRetry(createMarketoAPIException("1029", "")));
        // Retry wrap SocketTimeoutException, TimeoutException and EOFException
        Assert.assertTrue(jetty94SingleRequester.toRetry(new ExecutionException(new TimeoutException())));
        Assert.assertTrue(jetty94SingleRequester.toRetry(new ExecutionException(new EOFException())));
        Assert.assertTrue(jetty94SingleRequester.toRetry(new ExecutionException(new SocketTimeoutException())));
        // When EOFException is wrapped in IOException it should be retried too
        Assert.assertTrue(jetty94SingleRequester.toRetry(new IOException(new EOFException())));
        // Retry TimeoutException when it is wrapped in IOException
        Assert.assertTrue(jetty94SingleRequester.toRetry(new IOException(new TimeoutException())));

        // Retry SocketTimeoutException, TimeoutException and EOFException
        Assert.assertTrue(jetty94SingleRequester.toRetry(new SocketTimeoutException()));
        Assert.assertTrue(jetty94SingleRequester.toRetry(new TimeoutException()));
        Assert.assertTrue(jetty94SingleRequester.toRetry(new EOFException()));

        Assert.assertTrue(jetty94SingleRequester.toRetry(new DataException(jsonResponseInvalid)));
       // Call 3 times First call then 602 error and  601 error
        Mockito.verify(mockJetty94, Mockito.times(3)).requestWithRetry(Mockito.any(StringJetty94ResponseEntityReader.class), Mockito.any(Jetty94SingleRequester.class));
    }

    @Test(expected = DataException.class)
    public void testResponseInvalidJson() throws Exception
    {
        MarketoResponseJettyEntityReader reader = Mockito.spy(new MarketoResponseJettyEntityReader<>(10000));
        Response.Listener listener = Mockito.mock(Response.Listener.class);
        Mockito.doReturn(listener).when(reader).getListener();
        Response response = Mockito.mock(Response.class);
        Mockito.doReturn(200).when(response).getStatus();
        Mockito.doReturn(response).when(reader).getResponse();
        String ret = "{\n" +
                "    \"requestId\": \"d01f#15d672f8560\",\n" +
                "    \"result\": [\n" +
                "        {\n" +
                "            \"batchId\": 3404,\n" +
                "            \"importId\": \"3404\",\n" +
                "            \"status\": \"Queued\"\n" +
                "        }\n" +
                "    ,\n" +
                "    \"success\": true\n" +
                "}\n";
        Mockito.doReturn(ret).when(reader).readResponseContentInString();
        Jetty94RetryHelper retryHelper = Mockito.spy(new Jetty94RetryHelper(1,
                1000, 12000,
                new DefaultJetty94ClientCreator(10000, 10000)));
        retryHelper.requestWithRetry(reader, new Jetty94SingleRequester()
        {
            @Override
            public void requestOnce(HttpClient client, Response.Listener responseListener)
            {
                // do nothing
            }

            @Override
            protected boolean isResponseStatusToRetry(Response response)
            {
                return false;
            }

            @Override
            protected boolean isExceptionToRetry(Exception exception)
            {
                return false;
            }
        });
    }

    private HttpResponseException createHttpResponseException(int statusCode)
    {
        HttpResponseException exception = Mockito.mock(HttpResponseException.class);
        Response response = Mockito.mock(Response.class);
        Mockito.when(exception.getResponse()).thenReturn(response);
        Mockito.when(response.getStatus()).thenReturn(statusCode);
        return exception;
    }

    private MarketoAPIException createMarketoAPIException(String code, String error)
    {
        MarketoError marketoError = new MarketoError();
        marketoError.setCode(code);
        marketoError.setMessage(error);
        return new MarketoAPIException(Lists.newArrayList(marketoError));
    }
}
