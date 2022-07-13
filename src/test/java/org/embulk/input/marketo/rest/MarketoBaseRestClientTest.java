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
import org.embulk.util.retryhelper.jetty92.DefaultJetty92ClientCreator;
import org.embulk.util.retryhelper.jetty92.Jetty92ClientCreator;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.embulk.util.retryhelper.jetty92.Jetty92SingleRequester;
import org.embulk.util.retryhelper.jetty92.StringJetty92ResponseEntityReader;
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

import static org.embulk.input.marketo.rest.MarketoResponseJetty92EntityReader.jsonResponseInvalid;

/**
 * Created by tai.khuu on 9/21/17.
 */
public class MarketoBaseRestClientTest
{
    private static final String IDENTITY_END_POINT = "identityEndPoint";

    private static final int MARKETO_LIMIT_INTERVAL_MILIS = 1000;

    private MarketoBaseRestClient marketoBaseRestClient;

    private Jetty92RetryHelper mockJetty92;

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    @Before
    public void prepare()
    {
        mockJetty92 = Mockito.mock(Jetty92RetryHelper.class);
        marketoBaseRestClient = new MarketoBaseRestClient("identityEndPoint", "clientId", "clientSecret", "accountId", Optional.empty(), MARKETO_LIMIT_INTERVAL_MILIS, 60000, mockJetty92);
    }

    @Test
    public void testGetAccessToken()
    {
        Mockito.when(mockJetty92.requestWithRetry(Mockito.any(StringJetty92ResponseEntityReader.class), Mockito.any(Jetty92SingleRequester.class))).thenReturn("{\n" +
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
        ArgumentCaptor<Jetty92SingleRequester> jetty92SingleRequesterArgumentCaptor = ArgumentCaptor.forClass(Jetty92SingleRequester.class);
        Mockito.when(mockJetty92.requestWithRetry(Mockito.any(StringJetty92ResponseEntityReader.class), jetty92SingleRequesterArgumentCaptor.capture())).thenReturn("{\"access_token\": \"access_token\"}");
        String accessToken = marketoBaseRestClient.getAccessToken();
        Assert.assertEquals("access_token", accessToken);
        Jetty92SingleRequester jetty92SingleRequester = jetty92SingleRequesterArgumentCaptor.getValue();
        HttpClient client = Mockito.mock(HttpClient.class);
        Response.Listener listener = Mockito.mock(Response.Listener.class);
        Request mockRequest = Mockito.mock(Request.class);
        Mockito.when(client.newRequest(Mockito.eq(IDENTITY_END_POINT + MarketoRESTEndpoint.ACCESS_TOKEN.getEndpoint()))).thenReturn(mockRequest);
        Request request1 = Mockito.mock(Request.class);
        Mockito.when(mockRequest.method(Mockito.eq(HttpMethod.GET))).thenReturn(request1);
        jetty92SingleRequester.requestOnce(client, listener);
        Mockito.verify(request1, Mockito.times(1)).param(Mockito.eq("client_id"), Mockito.eq("clientId"));
        Mockito.verify(request1, Mockito.times(1)).param(Mockito.eq("client_secret"), Mockito.eq("clientSecret"));
        Mockito.verify(request1, Mockito.times(1)).param(Mockito.eq("grant_type"), Mockito.eq("client_credentials"));

        // By default the partner id is not set
        Mockito.verify(request1, Mockito.never()).param(Mockito.eq("partner_id"), Mockito.anyString());

        Assert.assertTrue(jetty92SingleRequester.toRetry(createHttpResponseException(502)));
        Assert.assertTrue(jetty92SingleRequester.toRetry(new ExecutionException(new TimeoutException())));
        Assert.assertTrue(jetty92SingleRequester.toRetry(new ExecutionException(new EOFException())));
        Assert.assertTrue(jetty92SingleRequester.toRetry(new ExecutionException(new SocketTimeoutException())));
        // Retry SocketTimeoutException, TimeoutException and EOFException
        Assert.assertTrue(jetty92SingleRequester.toRetry(new SocketTimeoutException()));
        Assert.assertTrue(jetty92SingleRequester.toRetry(new TimeoutException()));
        Assert.assertTrue(jetty92SingleRequester.toRetry(new EOFException()));
        // When EOFException is wrapped in IOException it should be retried too
        Assert.assertTrue(jetty92SingleRequester.toRetry(new IOException(new EOFException())));
        // Retry TimeoutException when it is wrapped in IOException
        Assert.assertTrue(jetty92SingleRequester.toRetry(new IOException(new TimeoutException())));
    }

    @Test
    public void testGetAccessTokenRequestShouldHavePartnerId()
    {
        final String partnerId = "sample_partner_id";
        mockJetty92 = Mockito.mock(Jetty92RetryHelper.class);
        ArgumentCaptor<Jetty92SingleRequester> jetty92SingleRequesterArgumentCaptor = ArgumentCaptor.forClass(Jetty92SingleRequester.class);
        Mockito.when(mockJetty92.requestWithRetry(Mockito.any(StringJetty92ResponseEntityReader.class), jetty92SingleRequesterArgumentCaptor.capture())).thenReturn("{\"access_token\": \"access_token\"}");

        MarketoBaseRestClient restClient = Mockito.spy(new MarketoBaseRestClient("identityEndPoint",
                "clientId",
                "clientSecret",
                "accountId",
                Optional.of(partnerId),
                MARKETO_LIMIT_INTERVAL_MILIS,
                60000,
                mockJetty92));

        // call method for evaluation
        restClient.getAccessToken();

        Jetty92SingleRequester singleRequester = jetty92SingleRequesterArgumentCaptor.getValue();

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
        ArgumentCaptor<Jetty92SingleRequester> jetty92SingleRequesterArgumentCaptor = ArgumentCaptor.forClass(Jetty92SingleRequester.class);
        Mockito.when(mockJetty92.requestWithRetry(Mockito.any(StringJetty92ResponseEntityReader.class), jetty92SingleRequesterArgumentCaptor.capture())).thenReturn("{\n" +
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

        Jetty92ClientCreator clientCreator = Mockito.mock(Jetty92ClientCreator.class);
        Mockito.doReturn(client).when(clientCreator).createAndStart();

        Request request = Mockito.mock(Request.class);
        Mockito.doReturn(request).when(client).newRequest(Mockito.anyString());
        Mockito.doReturn(request).when(request).method(HttpMethod.GET);

        HttpResponseException exception = new HttpResponseException("{\"error\":\"invalid_client\",\"error_description\":\"Bad client credentials\"}", Mockito.mock(Response.class));
        Mockito.doThrow(exception).when(request).send(Mockito.any(Response.Listener.class));

        Jetty92RetryHelper retryHelper = new Jetty92RetryHelper(1, 1, 1, clientCreator);
        final MarketoBaseRestClient restClient = new MarketoBaseRestClient("identityEndPoint", "clientId", "clientSecret", "accountId", Optional.empty(), MARKETO_LIMIT_INTERVAL_MILIS, 1000, retryHelper);

        // calling method should wrap the HttpResponseException by ConfigException
        Assert.assertThrows(ConfigException.class, restClient::getAccessToken);
    }

    @Test
    public void tetDoGetThrowHttpResponseException() throws Exception
    {
        final MarketoBaseRestClient client = doRequestWithWrapper(HttpMethod.GET);
        // calling method should wrap the HttpResponseException by DataException
        Assert.assertThrows(DataException.class, () -> client.doGet("test_target", null, null, new MarketoResponseJetty92EntityReader<String>(1000)));
    }

    @Test
    public void tetDoPostThrowHttpResponseException() throws Exception
    {
        final MarketoBaseRestClient client = doRequestWithWrapper(HttpMethod.POST);
        // calling method should wrap the HttpResponseException by DataException
        Assert.assertThrows(DataException.class, () -> client.doPost("test_target", null, null, "{\"any\": \"any\"}", new MarketoResponseJetty92EntityReader<String>(1000)));
    }

    private MarketoBaseRestClient doRequestWithWrapper(HttpMethod method) throws Exception
    {
        HttpClient client = Mockito.mock(HttpClient.class);

        Jetty92ClientCreator clientCreator = Mockito.mock(Jetty92ClientCreator.class);
        Mockito.doReturn(client).when(clientCreator).createAndStart();

        Request request = Mockito.mock(Request.class);
        Mockito.doReturn(request).when(client).newRequest(Mockito.anyString());
        Mockito.doReturn(request).when(request).method(method);

        HttpResponseException exception = new HttpResponseException("{\"error\":\"1035\",\"error_description\":\"Unsupported filter type for target subscription: updatedAt\"}", Mockito.mock(Response.class));
        Mockito.doThrow(exception).when(request).send(Mockito.any(Response.Listener.class));

        Jetty92RetryHelper retryHelper = new Jetty92RetryHelper(1, 1, 1, clientCreator);
        final MarketoBaseRestClient restClient = Mockito.spy(new MarketoBaseRestClient("identityEndPoint", "clientId", "clientSecret", "accountId", Optional.empty(), MARKETO_LIMIT_INTERVAL_MILIS, 1000, retryHelper));
        Mockito.doReturn("test_access_token").when(restClient).getAccessToken();

        return restClient;
    }

    @Test
    public void testDoPost()
    {
        MarketoBaseRestClient spy = Mockito.spy(marketoBaseRestClient);
        spy.doPost("target", Maps.newHashMap(), new ImmutableListMultimap.Builder<String, String>().build(), "test_content", new StringJetty92ResponseEntityReader(10));
        Mockito.verify(spy, Mockito.times(1)).doRequest(Mockito.anyString(), Mockito.eq(HttpMethod.POST), Mockito.any(Map.class), Mockito.any(Multimap.class), Mockito.any(StringContentProvider.class), Mockito.any(StringJetty92ResponseEntityReader.class));
    }

    @Test
    public void testDoGet()
    {
        MarketoBaseRestClient spy = Mockito.spy(marketoBaseRestClient);
        spy.doGet("target", Maps.newHashMap(), new ImmutableListMultimap.Builder<String, String>().build(), new StringJetty92ResponseEntityReader(10));
        Mockito.verify(spy, Mockito.times(1)).doRequest(Mockito.anyString(), Mockito.eq(HttpMethod.GET), Mockito.any(Map.class), Mockito.any(Multimap.class), Mockito.isNull(), Mockito.any(StringJetty92ResponseEntityReader.class));
    }

    @Test
    public void testDoRequestRequester()
    {
        MarketoBaseRestClient spy = Mockito.spy(marketoBaseRestClient);
        StringContentProvider contentProvider = new StringContentProvider("Content", StandardCharsets.UTF_8);
        ArgumentCaptor<Jetty92SingleRequester> jetty92SingleRequesterArgumentCaptor = ArgumentCaptor.forClass(Jetty92SingleRequester.class);

        MarketoResponse<Object> expectedMarketoResponse = new MarketoResponse<>();

        Mockito.when(mockJetty92.requestWithRetry(Mockito.any(MarketoResponseJetty92EntityReader.class), jetty92SingleRequesterArgumentCaptor.capture())).thenReturn(expectedMarketoResponse);
        Mockito.when(mockJetty92.requestWithRetry(Mockito.any(StringJetty92ResponseEntityReader.class), Mockito.any(Jetty92SingleRequester.class))).thenReturn("{\"access_token\": \"access_token\"}");

        String target = "target";
        HashMap<String, String> headers = Maps.newHashMap();
        headers.put("testHeader1", "testHeaderValue1");

        ImmutableListMultimap<String, String> build = new ImmutableListMultimap.Builder<String, String>().put("param", "param1").build();

        MarketoResponse<Object> marketoResponse = spy.doRequest(target, HttpMethod.POST, headers, build, contentProvider, new MarketoResponseJetty92EntityReader<>(10));

        HttpClient client = Mockito.mock(HttpClient.class);
        Response.Listener listener = Mockito.mock(Response.Listener.class);
        Request mockRequest = Mockito.mock(Request.class);
        Mockito.when(client.newRequest(Mockito.eq(target))).thenReturn(mockRequest);

        Mockito.when(mockRequest.method(Mockito.eq(HttpMethod.POST))).thenReturn(mockRequest);
        Jetty92SingleRequester jetty92SingleRequester = jetty92SingleRequesterArgumentCaptor.getValue();
        jetty92SingleRequester.requestOnce(client, listener);

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
        ArgumentCaptor<Jetty92SingleRequester> jetty92SingleRequesterArgumentCaptor = ArgumentCaptor.forClass(Jetty92SingleRequester.class);

        Mockito.when(mockJetty92.requestWithRetry(Mockito.any(MarketoResponseJetty92EntityReader.class), jetty92SingleRequesterArgumentCaptor.capture())).thenReturn(new MarketoResponse<>());
        Mockito.when(mockJetty92.requestWithRetry(Mockito.any(StringJetty92ResponseEntityReader.class), Mockito.any(Jetty92SingleRequester.class))).thenReturn("{\"access_token\": \"access_token\"}");

        spy.doRequest("", HttpMethod.POST, null, null, null, new MarketoResponseJetty92EntityReader<>(10));

        HttpClient client = Mockito.mock(HttpClient.class);
        Response.Listener listener = Mockito.mock(Response.Listener.class);
        Request mockRequest = Mockito.mock(Request.class);
        Mockito.when(client.newRequest(Mockito.anyString())).thenReturn(mockRequest);

        Mockito.when(mockRequest.method(Mockito.eq(HttpMethod.POST))).thenReturn(mockRequest);

        Jetty92SingleRequester jetty92SingleRequester = jetty92SingleRequesterArgumentCaptor.getValue();
        jetty92SingleRequester.requestOnce(client, listener);
        Assert.assertTrue(jetty92SingleRequester.toRetry(createHttpResponseException(502)));

        Assert.assertFalse(jetty92SingleRequester.toRetry(createHttpResponseException(400)));

        Assert.assertFalse(jetty92SingleRequester.toRetry(createMarketoAPIException("ERR", "ERR")));
        Assert.assertTrue(jetty92SingleRequester.toRetry(createMarketoAPIException("606", "")));
        Assert.assertTrue(jetty92SingleRequester.toRetry(createMarketoAPIException("615", "")));
        Assert.assertTrue(jetty92SingleRequester.toRetry(createMarketoAPIException("602", "")));
        // Should retry 601 error too
        Assert.assertTrue(jetty92SingleRequester.toRetry(createMarketoAPIException("601", "")));
        Assert.assertTrue(jetty92SingleRequester.toRetry(createMarketoAPIException("604", "")));
        Assert.assertTrue(jetty92SingleRequester.toRetry(createMarketoAPIException("608", "")));
        Assert.assertTrue(jetty92SingleRequester.toRetry(createMarketoAPIException("611", "")));
        Assert.assertTrue(jetty92SingleRequester.toRetry(createMarketoAPIException("615", "")));
        Assert.assertTrue(jetty92SingleRequester.toRetry(createMarketoAPIException("713", "")));
        Assert.assertTrue(jetty92SingleRequester.toRetry(createMarketoAPIException("1029", "")));
        // Retry wrap SocketTimeoutException, TimeoutException and EOFException
        Assert.assertTrue(jetty92SingleRequester.toRetry(new ExecutionException(new TimeoutException())));
        Assert.assertTrue(jetty92SingleRequester.toRetry(new ExecutionException(new EOFException())));
        Assert.assertTrue(jetty92SingleRequester.toRetry(new ExecutionException(new SocketTimeoutException())));
        // When EOFException is wrapped in IOException it should be retried too
        Assert.assertTrue(jetty92SingleRequester.toRetry(new IOException(new EOFException())));
        // Retry TimeoutException when it is wrapped in IOException
        Assert.assertTrue(jetty92SingleRequester.toRetry(new IOException(new TimeoutException())));

        // Retry SocketTimeoutException, TimeoutException and EOFException
        Assert.assertTrue(jetty92SingleRequester.toRetry(new SocketTimeoutException()));
        Assert.assertTrue(jetty92SingleRequester.toRetry(new TimeoutException()));
        Assert.assertTrue(jetty92SingleRequester.toRetry(new EOFException()));

        Assert.assertTrue(jetty92SingleRequester.toRetry(new DataException(jsonResponseInvalid)));
       // Call 3 times First call then 602 error and  601 error
        Mockito.verify(mockJetty92, Mockito.times(3)).requestWithRetry(Mockito.any(StringJetty92ResponseEntityReader.class), Mockito.any(Jetty92SingleRequester.class));
    }

    @Test(expected = DataException.class)
    public void testResponseInvalidJson() throws Exception
    {
        MarketoResponseJetty92EntityReader reader = Mockito.spy(new MarketoResponseJetty92EntityReader<>(10000));
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
        Jetty92RetryHelper retryHelper = Mockito.spy(new Jetty92RetryHelper(1,
                1000, 12000,
                new DefaultJetty92ClientCreator(10000, 10000)));
        retryHelper.requestWithRetry(reader, new Jetty92SingleRequester()
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
