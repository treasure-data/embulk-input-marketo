package org.embulk.input.marketo.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.io.ByteStreams;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.MarketoError;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.model.MarketoResponse;
import org.embulk.spi.DataException;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by khuutantaitai on 10/3/17.
 */
public class MarketoRestClientTest {

    private static final String TEST_ACCOUNT_ID = "test_account_id";

    private static final String TEST_CLIENT_SECRET = "test_client_secret";

    private static final String TEST_CLIENT_ID = "test_client_id";

    private static String END_POINT = MarketoUtils.getEndPoint(TEST_ACCOUNT_ID);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();

    private MarketoRestClient marketoRestClient;

    private Jetty92RetryHelper mockRetryHelper;

    private static final JavaType RESPONSE_TYPE = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(MarketoResponse.class, MarketoResponse.class, ObjectNode.class);

    @Before
    public void prepare()
    {
        ConfigSource configSource = embulkTestRuntime.getExec().newConfigSource();
        configSource.set("account_id", TEST_ACCOUNT_ID);
        configSource.set("client_secret", TEST_CLIENT_SECRET);
        configSource.set("client_id", TEST_CLIENT_ID);
        MarketoRestClient.PluginTask task = configSource.loadConfig(MarketoRestClient.PluginTask.class);
        mockRetryHelper = mock(Jetty92RetryHelper.class);
        MarketoRestClient realRestClient = new MarketoRestClient(task, mockRetryHelper);
        marketoRestClient = spy(realRestClient);
    }

    @Test
    public void describeLead() throws Exception {
        String leadSchema = new String(ByteStreams.toByteArray(this.getClass().getResourceAsStream("/fixtures/lead_describe.json")));
        MarketoResponse<ObjectNode> marketoResponse = OBJECT_MAPPER.readValue(leadSchema, RESPONSE_TYPE);
        doReturn(marketoResponse).when(marketoRestClient).doGet(eq(END_POINT + MarketoRESTEndpoint.DESCRIBE_LEAD.getEndpoint()), isNull(Map.class), isNull(ImmutableListMultimap.class), any(MarketoResponseJetty92EntityReader.class));
        List<MarketoField> marketoFields = marketoRestClient.describeLead();
        assertEquals(16, marketoFields.size());
        JavaType marketoFieldType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<MarketoField> expectedFields = OBJECT_MAPPER.readValue(new String(ByteStreams.toByteArray(this.getClass().getResourceAsStream("/fixtures/lead_describe_expected.json"))), marketoFieldType);
        assertArrayEquals(expectedFields.toArray(), marketoFields.toArray());
    }

    @Test
    public void createLeadBulkExtract() throws Exception
    {
        MarketoResponse<ObjectNode> marketoResponse = new MarketoResponse<>();
        marketoResponse.setSuccess(true);
        Date startDate = new Date(1506865856000L);
        Date endDate = new Date(1507297856000L);
        ObjectNode bulkExtractResult = OBJECT_MAPPER.createObjectNode();
        bulkExtractResult.set("exportId", new TextNode("bulkExtractId"));
        marketoResponse.setResult(Arrays.asList(bulkExtractResult));
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        doReturn(marketoResponse).when(marketoRestClient).doPost(eq(END_POINT + MarketoRESTEndpoint.CREATE_LEAD_EXTRACT.getEndpoint()), isNull(Map.class), isNull(ImmutableListMultimap.class), argumentCaptor.capture(), any(MarketoResponseJetty92EntityReader.class));
        String filterField = "filterField";
        String bulkExtractId = marketoRestClient.createLeadBulkExtract(startDate, endDate, Arrays.asList("extract_field1", "extract_field2"), filterField);
        assertEquals("bulkExtractId", bulkExtractId);
        String postContent = argumentCaptor.getValue();
        ObjectNode marketoBulkExtractRequest = (ObjectNode) OBJECT_MAPPER.readTree(postContent);
        ObjectNode filter = (ObjectNode) marketoBulkExtractRequest.get("filter");
        ObjectNode dateRangeFilter = (ObjectNode) filter.get(filterField);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(MarketoUtils.MARKETO_DATE_SIMPLE_DATE_FORMAT);
        assertEquals(simpleDateFormat.format(startDate), dateRangeFilter.get("startAt").textValue());
        assertEquals(simpleDateFormat.format(endDate), dateRangeFilter.get("endAt").textValue());
        assertEquals("CSV", marketoBulkExtractRequest.get("format").textValue());
        ArrayNode fields = (ArrayNode) marketoBulkExtractRequest.get("fields");
        assertEquals("extract_field1", fields.get(0).textValue());
        assertEquals("extract_field2", fields.get(1).textValue());
    }

    @Test
    public void createLeadBulkExtractWithError() throws Exception
    {
        MarketoResponse<ObjectNode> marketoResponse = new MarketoResponse<>();
        marketoResponse.setSuccess(false);
        MarketoError marketoError = new MarketoError();
        marketoError.setCode("ErrorCode1");
        marketoError.setMessage("Message");
        marketoResponse.setErrors(Arrays.asList(marketoError));
        doReturn(marketoResponse).when(marketoRestClient).doPost(eq(END_POINT + MarketoRESTEndpoint.CREATE_LEAD_EXTRACT.getEndpoint()), isNull(Map.class), isNull(ImmutableListMultimap.class), anyString(), any(MarketoResponseJetty92EntityReader.class));
        String filterField = "filterField";
        try {
            marketoRestClient.createLeadBulkExtract(new Date(), new Date(), Arrays.asList("extract_field1", "extract_field2"), filterField);
        } catch (DataException ex) {
            assertEquals("ErrorCode1: Message", ex.getMessage());
            return;
        }
        fail();
    }

    @Test()
    public void createLeadBulkExtractWithParseError() throws Exception
    {
        doThrow(JsonProcessingException.class).when(marketoRestClient).doPost(eq(END_POINT + MarketoRESTEndpoint.CREATE_LEAD_EXTRACT.getEndpoint()), isNull(Map.class), isNull(ImmutableListMultimap.class), anyString(), any(MarketoResponseJetty92EntityReader.class));
        String filterField = "filterField";
        try {
            marketoRestClient.createLeadBulkExtract(new Date(), new Date(), Arrays.asList("extract_field1", "extract_field2"), filterField);
        } catch (DataException ex) {
            return;
        }
        fail();
    }

    @Test
    public void createActitvityExtract() throws Exception {
        MarketoResponse<ObjectNode> marketoResponse = new MarketoResponse<>();
        marketoResponse.setSuccess(true);
        Date startDate = new Date(1506865856000L);
        Date endDate = new Date(1507297856000L);
        ObjectNode bulkExtractResult = OBJECT_MAPPER.createObjectNode();
        bulkExtractResult.set("exportId", new TextNode("bulkExtractId"));
        marketoResponse.setResult(Arrays.asList(bulkExtractResult));
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        doReturn(marketoResponse).when(marketoRestClient).doPost(eq(END_POINT + MarketoRESTEndpoint.CREATE_ACTIVITY_EXTRACT.getEndpoint()), isNull(Map.class), isNull(ImmutableListMultimap.class), argumentCaptor.capture(), any(MarketoResponseJetty92EntityReader.class));
        String bulkExtractId = marketoRestClient.createActivityExtract(startDate, endDate);
        assertEquals("bulkExtractId", bulkExtractId);
        String postContent = argumentCaptor.getValue();
        ObjectNode marketoBulkExtractRequest = (ObjectNode) OBJECT_MAPPER.readTree(postContent);
        ObjectNode filter = (ObjectNode) marketoBulkExtractRequest.get("filter");
        assertTrue(filter.has("createdAt"));
    }

    @Test
    public void startLeadBulkExtract() throws Exception {
        String bulkExportId = "bulkExportId";
        Map<String, String> pathParams = new HashMap<>();
        pathParams.put("export_id", bulkExportId);
        MarketoResponse<ObjectNode> marketoResponse = new MarketoResponse<>();
        marketoResponse.setSuccess(true);
        doReturn(marketoResponse).when(marketoRestClient).doPost(eq(END_POINT + MarketoRESTEndpoint.START_LEAD_EXPORT_JOB.getEndpoint(pathParams)), isNull(Map.class), isNull(ImmutableListMultimap.class), isNull(String.class), any(MarketoResponseJetty92EntityReader.class));
        marketoRestClient.startLeadBulkExtract(bulkExportId);
        verify(marketoRestClient, times(1)).doPost(eq(END_POINT + MarketoRESTEndpoint.START_LEAD_EXPORT_JOB.getEndpoint(pathParams)), isNull(Map.class), isNull(ImmutableListMultimap.class), isNull(String.class), any(MarketoResponseJetty92EntityReader.class));
    }

    @Test
    public void startLeadBulkExtractWithError() throws Exception {
        String bulkExportId = "bulkExportId";
        Map<String, String> pathParams = new HashMap<>();
        pathParams.put("export_id", bulkExportId);
        MarketoResponse<ObjectNode> marketoResponse = new MarketoResponse<>();
        marketoResponse.setSuccess(false);
        MarketoError marketoError = new MarketoError();
        marketoError.setCode("ErrorCode");
        marketoError.setMessage("Message");
        marketoResponse.setErrors(Arrays.asList(marketoError));
        doReturn(marketoResponse).when(marketoRestClient).doPost(eq(END_POINT + MarketoRESTEndpoint.START_LEAD_EXPORT_JOB.getEndpoint(pathParams)), isNull(Map.class), isNull(ImmutableListMultimap.class), isNull(String.class), any(MarketoResponseJetty92EntityReader.class));
        try {
            marketoRestClient.startLeadBulkExtract(bulkExportId);
        } catch (DataException ex) {
            verify(marketoRestClient, times(1)).doPost(eq(END_POINT + MarketoRESTEndpoint.START_LEAD_EXPORT_JOB.getEndpoint(pathParams)), isNull(Map.class), isNull(ImmutableListMultimap.class), isNull(String.class), any(MarketoResponseJetty92EntityReader.class));
            return;
        }
        fail();
    }

    @Test
    public void startActivityBulkExtract() throws Exception
    {
        String bulkExportId = "bulkExportId";
        Map<String, String> pathParams = new HashMap<>();
        pathParams.put("export_id", bulkExportId);
        MarketoResponse<ObjectNode> marketoResponse = new MarketoResponse<>();
        marketoResponse.setSuccess(true);
        doReturn(marketoResponse).when(marketoRestClient).doPost(eq(END_POINT + MarketoRESTEndpoint.START_ACTIVITY_EXPORT_JOB.getEndpoint(pathParams)), isNull(Map.class), isNull(ImmutableListMultimap.class), isNull(String.class), any(MarketoResponseJetty92EntityReader.class));
        marketoRestClient.startActitvityBulkExtract(bulkExportId);
        verify(marketoRestClient, times(1)).doPost(eq(END_POINT + MarketoRESTEndpoint.START_ACTIVITY_EXPORT_JOB.getEndpoint(pathParams)), isNull(Map.class), isNull(ImmutableListMultimap.class), isNull(String.class), any(MarketoResponseJetty92EntityReader.class));
    }

    @Test
    public void waitLeadExportJobComplete() throws Exception {
        String bulkExportId = "bulkExportId";
        Map<String, String> pathParams = new HashMap<>();
        pathParams.put("export_id", bulkExportId);
        MarketoResponse<ObjectNode> marketoResponse = mock(MarketoResponse.class);
        when(marketoResponse.isSuccess()).thenReturn(true);
        ObjectNode result = mock(ObjectNode.class);
        when(marketoResponse.getResult()).thenReturn(Arrays.asList(result));
        when(result.get("status")).thenReturn(new TextNode("Queued")).thenReturn(new TextNode("Processing")).thenReturn(new TextNode("Completed"));
        doReturn(marketoResponse).when(marketoRestClient).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), isNull(Map.class), isNull(ImmutableListMultimap.class), any(MarketoResponseJetty92EntityReader.class));
        marketoRestClient.waitLeadExportJobComplete(bulkExportId, 1, 4);
        verify(marketoRestClient, times(3)).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), isNull(Map.class), isNull(ImmutableListMultimap.class), any(MarketoResponseJetty92EntityReader.class));
    }

    @Test
    public void waitLeadExportJobTimeOut() throws Exception {
        String bulkExportId = "bulkExportId";
        Map<String, String> pathParams = new HashMap<>();
        pathParams.put("export_id", bulkExportId);
        MarketoResponse<ObjectNode> marketoResponse = mock(MarketoResponse.class);
        when(marketoResponse.isSuccess()).thenReturn(true);
        ObjectNode result = mock(ObjectNode.class);
        when(marketoResponse.getResult()).thenReturn(Arrays.asList(result));
        when(result.get("status")).thenReturn(new TextNode("Queued")).thenReturn(new TextNode("Processing"));
        doReturn(marketoResponse).when(marketoRestClient).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), isNull(Map.class), isNull(ImmutableListMultimap.class), any(MarketoResponseJetty92EntityReader.class));
        try {
            marketoRestClient.waitLeadExportJobComplete(bulkExportId, 1, 4);
        } catch (DataException e) {
            assertTrue(e.getMessage().contains("Job timeout exception"));
            verify(marketoRestClient, times(3)).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), isNull(Map.class), isNull(ImmutableListMultimap.class), any(MarketoResponseJetty92EntityReader.class));
            return;
        }
        fail();
    }

    @Test
    public void waitLeadExportJobFailed() throws Exception {
        String bulkExportId = "bulkExportId";
        Map<String, String> pathParams = new HashMap<>();
        pathParams.put("export_id", bulkExportId);
        MarketoResponse<ObjectNode> marketoResponse = mock(MarketoResponse.class);
        when(marketoResponse.isSuccess()).thenReturn(true);
        ObjectNode result = mock(ObjectNode.class);
        when(marketoResponse.getResult()).thenReturn(Arrays.asList(result));
        when(result.get("status")).thenReturn(new TextNode("Queued")).thenReturn(new TextNode("Failed"));
        when(result.get("errorMsg")).thenReturn(new TextNode("ErrorMessage"));
        doReturn(marketoResponse).when(marketoRestClient).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), isNull(Map.class), isNull(ImmutableListMultimap.class), any(MarketoResponseJetty92EntityReader.class));
        try {
            marketoRestClient.waitLeadExportJobComplete(bulkExportId, 1, 4);
        } catch (DataException e) {
            assertTrue(e.getMessage().contains("Bulk extract job failed"));
            assertTrue(e.getMessage().contains("ErrorMessage"));
            verify(marketoRestClient, times(2)).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), isNull(Map.class), isNull(ImmutableListMultimap.class), any(MarketoResponseJetty92EntityReader.class));
            return;
        }
        fail();
    }

    @Test
    public void waitActitvityExportJobComplete() throws Exception {

    }

    @Test
    public void getLeadBulkExtractResult() throws Exception {
    }

    @Test
    public void getActivitiesBulkExtractResult() throws Exception {
    }

    @Test
    public void getLists() throws Exception {
    }

    @Test
    public void getPrograms() throws Exception {
    }

    @Test
    public void getLeadsByProgram() throws Exception {
    }

    @Test
    public void getLeadsByList() throws Exception {
    }

    @Test
    public void getCampaign() throws Exception {
    }

}