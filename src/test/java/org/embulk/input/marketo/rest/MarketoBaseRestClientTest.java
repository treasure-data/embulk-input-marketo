package org.embulk.input.marketo.rest;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpResponseException;
import org.eclipse.jetty.client.api.ContentProvider;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.embulk.EmbulkTestRuntime;
import org.embulk.input.marketo.exception.MarketoAPIException;
import org.embulk.input.marketo.model.MarketoError;
import org.embulk.input.marketo.model.MarketoResponse;
import org.embulk.spi.DataException;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.embulk.util.retryhelper.jetty92.Jetty92SingleRequester;
import org.embulk.util.retryhelper.jetty92.StringJetty92ResponseEntityReader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

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
        mockJetty92 = mock(Jetty92RetryHelper.class);
        marketoBaseRestClient = new MarketoBaseRestClient("identityEndPoint", "clientId", "clientSecret", MARKETO_LIMIT_INTERVAL_MILIS, mockJetty92);
    }

    @Test
    public void doGet() throws Exception
    {

    }

    @Test
    public void testGetAccessToken()
    {
        when(mockJetty92.requestWithRetry(any(StringJetty92ResponseEntityReader.class), any(Jetty92SingleRequester.class))).thenReturn("{\n" +
                "    \"access_token\": \"access_token\",\n" +
                "    \"token_type\": \"bearer\",\n" +
                "    \"expires_in\": 3599,\n" +
                "    \"scope\": \"tai@treasure-data.com\"\n" +
                "}");
        String accessToken = marketoBaseRestClient.getAccessToken();
        assertEquals("access_token", accessToken);
    }

    @Test
    public void testGetAccessTokenRequester()
    {

        ArgumentCaptor<Jetty92SingleRequester> jetty92SingleRequesterArgumentCaptor = ArgumentCaptor.forClass(Jetty92SingleRequester.class);
        when(mockJetty92.requestWithRetry(any(StringJetty92ResponseEntityReader.class), jetty92SingleRequesterArgumentCaptor.capture())).thenReturn("{\"access_token\": \"access_token\"}");
        String accessToken = marketoBaseRestClient.getAccessToken();
        assertEquals("access_token", accessToken);
        Jetty92SingleRequester value = jetty92SingleRequesterArgumentCaptor.getValue();
        HttpClient client = mock(HttpClient.class);
        Response.Listener listener = mock(Response.Listener.class);
        Request mockRequest = mock(Request.class);
        when(client.newRequest(eq(IDENTITY_END_POINT + MarketoRESTEndpoint.ACCESS_TOKEN.getEndpoint()))).thenReturn(mockRequest);
        Request request1 = mock(Request.class);
        when(mockRequest.method(eq(HttpMethod.GET))).thenReturn(request1);
        value.requestOnce(client, listener);
        verify(request1, times(1)).param(eq("client_id"), eq("clientId"));
        verify(request1, times(1)).param(eq("client_secret"), eq("clientSecret"));
        verify(request1, times(1)).param(eq("grant_type"), eq("client_credentials"));
        assertTrue(value.toRetry(createHttpResponseException(502)));
    }
    @Test
    public void testGetAccessTokenWithError()
    {
        ArgumentCaptor<Jetty92SingleRequester> jetty92SingleRequesterArgumentCaptor = ArgumentCaptor.forClass(Jetty92SingleRequester.class);
        when(mockJetty92.requestWithRetry(any(StringJetty92ResponseEntityReader.class), jetty92SingleRequesterArgumentCaptor.capture())).thenReturn("{\n" +
                "    \"error\": \"invalid_client\",\n" +
                "    \"error_description\": \"Bad client credentials\"\n" +
                "}");
        try {
            marketoBaseRestClient.getAccessToken();
        } catch (DataException ex) {
            assertEquals("Bad client credentials", ex.getMessage());
            return;
        }
        fail();
    }

    @Test
    public void testDoPost() throws Exception
    {
        MarketoBaseRestClient spy = spy(marketoBaseRestClient);
        spy.doPost("target", Maps.<String, String>newHashMap(), new ImmutableListMultimap.Builder<String, String>().build(), "test_content", new StringJetty92ResponseEntityReader(10));
        verify(spy, times(1)).doRequest(anyString(), eq(HttpMethod.POST), any(Map.class), any(Multimap.class), any(StringContentProvider.class), any(StringJetty92ResponseEntityReader.class));
    }

    @Test
    public void testDoGet() throws Exception
    {
        MarketoBaseRestClient spy = spy(marketoBaseRestClient);
        spy.doGet("target", Maps.<String, String>newHashMap(), new ImmutableListMultimap.Builder<String, String>().build(), new StringJetty92ResponseEntityReader(10));
        verify(spy, times(1)).doRequest(anyString(), eq(HttpMethod.GET), any(Map.class), any(Multimap.class), isNull(ContentProvider.class), any(StringJetty92ResponseEntityReader.class));
    }

    @Test
    public void testDoRequestRequester() throws Exception
    {
        MarketoBaseRestClient spy = spy(marketoBaseRestClient);
        StringContentProvider contentProvider = new StringContentProvider("Content", StandardCharsets.UTF_8);
        ArgumentCaptor<Jetty92SingleRequester> jetty92SingleRequesterArgumentCaptor = ArgumentCaptor.forClass(Jetty92SingleRequester.class);

        MarketoResponse<Object> expectedMarketoResponse = new MarketoResponse<>();

        when(mockJetty92.requestWithRetry(any(MarketoResponseJetty92EntityReader.class), jetty92SingleRequesterArgumentCaptor.capture())).thenReturn(expectedMarketoResponse);
        when(mockJetty92.requestWithRetry(any(StringJetty92ResponseEntityReader.class), any(Jetty92SingleRequester.class))).thenReturn("{\"access_token\": \"access_token\"}");

        String target = "target";
        HashMap<String, String> headers = Maps.<String, String>newHashMap();
        headers.put("testHeader1", "testHeaderValue1");

        ImmutableListMultimap<String, String> build = new ImmutableListMultimap.Builder<String, String>().put("param", "param1").build();

        MarketoResponse<Object> marketoResponse = spy.doRequest(target, HttpMethod.POST, headers, build, contentProvider, new MarketoResponseJetty92EntityReader<Object>(10));

        HttpClient client = mock(HttpClient.class);
        Response.Listener listener = mock(Response.Listener.class);
        Request mockRequest = mock(Request.class);
        when(client.newRequest(eq(target))).thenReturn(mockRequest);

        when(mockRequest.method(eq(HttpMethod.POST))).thenReturn(mockRequest);
        Jetty92SingleRequester jetty92SingleRequester = jetty92SingleRequesterArgumentCaptor.getValue();
        jetty92SingleRequester.requestOnce(client, listener);

        assertEquals(expectedMarketoResponse, marketoResponse);

        verify(mockRequest, times(1)).header(eq("testHeader1"), eq("testHeaderValue1"));
        verify(mockRequest, times(1)).header(eq("Authorization"), eq("Bearer access_token"));
        verify(mockRequest, times(1)).param(eq("param"), eq("param1"));
        verify(mockRequest, times(1)).content(eq(contentProvider), eq("application/json"));

    }

    @Test
    public void testDoRequesterRetry() throws Exception
    {
        MarketoBaseRestClient spy = spy(marketoBaseRestClient);
        ArgumentCaptor<Jetty92SingleRequester> jetty92SingleRequesterArgumentCaptor = ArgumentCaptor.forClass(Jetty92SingleRequester.class);

        when(mockJetty92.requestWithRetry(any(MarketoResponseJetty92EntityReader.class), jetty92SingleRequesterArgumentCaptor.capture())).thenReturn(new MarketoResponse<>());
        when(mockJetty92.requestWithRetry(any(StringJetty92ResponseEntityReader.class), any(Jetty92SingleRequester.class))).thenReturn("{\"access_token\": \"access_token\"}");

        spy.doRequest("", HttpMethod.POST, null, null, null, new MarketoResponseJetty92EntityReader<Object>(10));

        HttpClient client = mock(HttpClient.class);
        Response.Listener listener = mock(Response.Listener.class);
        Request mockRequest = mock(Request.class);
        when(client.newRequest(anyString())).thenReturn(mockRequest);

        when(mockRequest.method(eq(HttpMethod.POST))).thenReturn(mockRequest);

        Jetty92SingleRequester jetty92SingleRequester = jetty92SingleRequesterArgumentCaptor.getValue();
        jetty92SingleRequester.requestOnce(client, listener);
        assertTrue(jetty92SingleRequester.toRetry(createHttpResponseException(502)));


        assertFalse(jetty92SingleRequester.toRetry(createHttpResponseException(400)));

        assertFalse(jetty92SingleRequester.toRetry(createMarketoAPIException("ERR", "ERR")));
        assertTrue(jetty92SingleRequester.toRetry(createMarketoAPIException("606", "")));
        assertTrue(jetty92SingleRequester.toRetry(createMarketoAPIException("615", "")));
        assertTrue(jetty92SingleRequester.toRetry(createMarketoAPIException("602", "")));

        verify(mockJetty92, times(2)).requestWithRetry(any(StringJetty92ResponseEntityReader.class), any(Jetty92SingleRequester.class));

    }

    private HttpResponseException createHttpResponseException(int statusCode)
    {
        HttpResponseException exception = mock(HttpResponseException.class);
        Response response = mock(Response.class);
        when(exception.getResponse()).thenReturn(response);
        when(response.getStatus()).thenReturn(statusCode);
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