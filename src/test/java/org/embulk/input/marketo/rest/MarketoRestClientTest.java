package org.embulk.input.marketo.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteStreams;
import org.eclipse.jetty.client.api.ContentProvider;
import org.eclipse.jetty.client.util.FormContentProvider;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.BulkExtractRangeHeader;
import org.embulk.input.marketo.model.MarketoError;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.model.MarketoResponse;
import org.embulk.spi.DataException;
import org.embulk.util.retryhelper.jetty92.Jetty92ResponseReader;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by khuutantaitai on 10/3/17.
 */
public class MarketoRestClientTest
{
    private static final String TEST_ACCOUNT_ID = "test_account_id";

    private static final String TEST_CLIENT_SECRET = "test_client_secret";

    private static final String TEST_CLIENT_ID = "test_client_id";

    private static final String END_POINT = MarketoUtils.getEndPoint(TEST_ACCOUNT_ID);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

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
        configSource.set("max_return", 2);
        MarketoRestClient.PluginTask task = configSource.loadConfig(MarketoRestClient.PluginTask.class);
        mockRetryHelper = mock(Jetty92RetryHelper.class);
        MarketoRestClient realRestClient = new MarketoRestClient(task, mockRetryHelper);
        marketoRestClient = spy(realRestClient);
    }

    @Test
    public void describeLead() throws Exception
    {
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
        }
        catch (DataException ex) {
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
        }
        catch (DataException ex) {
            return;
        }
        fail();
    }

    @Test
    public void createActitvityExtract() throws Exception
    {
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
    public void startLeadBulkExtract() throws Exception
    {
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
    public void startLeadBulkExtractWithError() throws Exception
    {
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
        }
        catch (DataException ex) {
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
    public void waitLeadExportJobComplete() throws Exception
    {
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
    public void waitLeadExportJobTimeOut() throws Exception
    {
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
        }
        catch (DataException e) {
            assertTrue(e.getMessage().contains("Job timeout exception"));
            verify(marketoRestClient, times(3)).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), isNull(Map.class), isNull(ImmutableListMultimap.class), any(MarketoResponseJetty92EntityReader.class));
            return;
        }
        fail();
    }

    @Test
    public void waitLeadExportJobFailed() throws Exception
    {
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
        }
        catch (DataException e) {
            assertTrue(e.getMessage().contains("Bulk extract job failed"));
            assertTrue(e.getMessage().contains("ErrorMessage"));
            verify(marketoRestClient, times(2)).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), isNull(Map.class), isNull(ImmutableListMultimap.class), any(MarketoResponseJetty92EntityReader.class));
            return;
        }
        fail();
    }

    @Test
    public void waitActitvityExportJobComplete() throws Exception
    {
        String exportId = "exportId";
        Map<String, String> pathParamMap = new HashMap<>();
        pathParamMap.put("export_id", exportId);
        MarketoResponse<ObjectNode> marketoResponse = mock(MarketoResponse.class);
        when(marketoResponse.isSuccess()).thenReturn(true);
        ObjectNode mockObjectNode = mock(ObjectNode.class);
        when(marketoResponse.getResult()).thenReturn(Arrays.asList(mockObjectNode));
        when(mockObjectNode.get("status")).thenReturn(new TextNode("Completed"));
        doReturn(marketoResponse).when(marketoRestClient).doGet(anyString(), isNull(Map.class), isNull(ImmutableListMultimap.class), any(Jetty92ResponseReader.class));
        marketoRestClient.waitActitvityExportJobComplete(exportId, 1, 3);
        verify(marketoRestClient, times(1)).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_ACTIVITY_EXPORT_STATUS.getEndpoint(pathParamMap)), isNull(Map.class), isNull(ImmutableListMultimap.class), any(Jetty92ResponseReader.class));
    }

    @Test
    public void getLeadBulkExtractResult() throws Exception
    {
        String exportId = "exportId";
        Map<String, String> pathParamMap = new HashMap<>();
        pathParamMap.put("export_id", exportId);
        doReturn(mock(InputStream.class)).when(marketoRestClient).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_RESULT.getEndpoint(pathParamMap)), any(Map.class), isNull(ImmutableListMultimap.class), any(MarketoInputStreamResponseEntityReader.class));
        marketoRestClient.getLeadBulkExtractResult(exportId, null);
        verify(marketoRestClient, times(1)).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_RESULT.getEndpoint(pathParamMap)), any(Map.class), isNull(ImmutableListMultimap.class), any(MarketoInputStreamResponseEntityReader.class));
    }

    @Test
    public void getActivitiesBulkExtractResult() throws Exception
    {
        String exportId = "exportId";
        Map<String, String> pathParamMap = new HashMap<>();
        pathParamMap.put("export_id", exportId);
        doReturn(mock(InputStream.class)).when(marketoRestClient).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_ACTIVITY_EXPORT_RESULT.getEndpoint(pathParamMap)), any(Map.class), isNull(ImmutableListMultimap.class), any(MarketoInputStreamResponseEntityReader.class));
        marketoRestClient.getActivitiesBulkExtractResult(exportId, null);
        verify(marketoRestClient, times(1)).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_ACTIVITY_EXPORT_RESULT.getEndpoint(pathParamMap)), any(Map.class), isNull(ImmutableListMultimap.class), any(MarketoInputStreamResponseEntityReader.class));
    }

    @Test
    public void getLists() throws Exception
    {
        mockMarketoPageResponse("/fixtures/lists_response.json", END_POINT + MarketoRESTEndpoint.GET_LISTS.getEndpoint());
        RecordPagingIterable<ObjectNode> lists = marketoRestClient.getLists();
        Iterator<ObjectNode> iterator = lists.iterator();
        ObjectNode list1 = iterator.next();
        ObjectNode list2 = iterator.next();
        assertFalse(iterator.hasNext());
        assertEquals("Test list 1", list1.get("name").asText());
        assertEquals("Test list 2", list2.get("name").asText());
        ArgumentCaptor<Multimap> immutableListMultimapArgumentCaptor = ArgumentCaptor.forClass(Multimap.class);
        ArgumentCaptor<FormContentProvider> formContentProviderArgumentCaptor = ArgumentCaptor.forClass(FormContentProvider.class);
        verify(marketoRestClient, times(2)).doPost(eq(END_POINT + MarketoRESTEndpoint.GET_LISTS.getEndpoint()), isNull(Map.class), immutableListMultimapArgumentCaptor.capture(), any(MarketoResponseJetty92EntityReader.class), formContentProviderArgumentCaptor.capture());
        List<Multimap> params = immutableListMultimapArgumentCaptor.getAllValues();
        Multimap params1 = params.get(0);
        assertEquals("GET", params1.get("_method").iterator().next());
        assertEquals("nextPageToken=GWP55GLCVCZLPE6SS7OCG5IEXQ%3D%3D%3D%3D%3D%3D&batchSize=300", fromContentProviderToString(formContentProviderArgumentCaptor.getValue()));

    }

    @Test
    public void getPrograms() throws Exception
    {
        ArrayNode listPages = (ArrayNode) OBJECT_MAPPER.readTree(new String(ByteStreams.toByteArray(this.getClass().getResourceAsStream("/fixtures/program_response.json")))).get("responses");
        MarketoResponse<ObjectNode> page1 = OBJECT_MAPPER.readValue(listPages.get(0).toString(), RESPONSE_TYPE);
        MarketoResponse<ObjectNode> page2 = OBJECT_MAPPER.readValue(listPages.get(1).toString(), RESPONSE_TYPE);
        doReturn(page1).doReturn(page2).when(marketoRestClient).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_PROGRAMS.getEndpoint()), isNull(Map.class), any(Multimap.class), any(MarketoResponseJetty92EntityReader.class));
        RecordPagingIterable<ObjectNode> lists = marketoRestClient.getPrograms();
        Iterator<ObjectNode> iterator = lists.iterator();
        ObjectNode program1 = iterator.next();
        ObjectNode program2 = iterator.next();
        ObjectNode program3 = iterator.next();
        assertFalse(iterator.hasNext());
        assertEquals("MB_Sep_25_test_program", program1.get("name").asText());
        assertEquals("TD Output Test Program", program2.get("name").asText());
        assertEquals("Bill_progream", program3.get("name").asText());
        ArgumentCaptor<ImmutableListMultimap> immutableListMultimapArgumentCaptor = ArgumentCaptor.forClass(ImmutableListMultimap.class);
        verify(marketoRestClient, times(2)).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_PROGRAMS.getEndpoint()), isNull(Map.class), immutableListMultimapArgumentCaptor.capture(), any(MarketoResponseJetty92EntityReader.class));
        List<ImmutableListMultimap> params = immutableListMultimapArgumentCaptor.getAllValues();
        ImmutableListMultimap params1 = params.get(0);
        assertEquals("0", params1.get("offset").get(0));
        assertEquals("2", params1.get("maxReturn").get(0));
        ImmutableListMultimap params2 = params.get(1);
        assertEquals("2", params2.get("offset").get(0));
    }

    private void mockMarketoPageResponse(String fixtureName, String mockEndpoint) throws IOException
    {
        ArrayNode listPages = (ArrayNode) OBJECT_MAPPER.readTree(new String(ByteStreams.toByteArray(this.getClass().getResourceAsStream(fixtureName)))).get("responses");
        MarketoResponse<ObjectNode> page1 = OBJECT_MAPPER.readValue(listPages.get(0).toString(), RESPONSE_TYPE);
        MarketoResponse<ObjectNode> page2 = OBJECT_MAPPER.readValue(listPages.get(1).toString(), RESPONSE_TYPE);
        doReturn(page1).doReturn(page2).when(marketoRestClient).doPost(eq(mockEndpoint), isNull(Map.class), any(Multimap.class), any(MarketoResponseJetty92EntityReader.class), any(FormContentProvider.class));
    }

    @Test
    public void getLeadsByProgram() throws Exception
    {
        String programId = "programId";
        Map<String, String> pathParamPath = new HashMap<>();
        pathParamPath.put("program_id", programId);
        mockMarketoPageResponse("/fixtures/lead_by_program_response.json", END_POINT + MarketoRESTEndpoint.GET_LEADS_BY_PROGRAM.getEndpoint(pathParamPath));
        RecordPagingIterable<ObjectNode> lists = marketoRestClient.getLeadsByProgram(programId, "firstName,lastName");
        Iterator<ObjectNode> iterator = lists.iterator();
        ObjectNode lead1 = iterator.next();
        ObjectNode lead2 = iterator.next();
        assertFalse(iterator.hasNext());
        assertEquals("Tai 1", lead1.get("firstName").asText());
        assertEquals("Tai", lead2.get("firstName").asText());
        ArgumentCaptor<ImmutableListMultimap> immutableListMultimapArgumentCaptor = ArgumentCaptor.forClass(ImmutableListMultimap.class);
        ArgumentCaptor<FormContentProvider> formContentProviderArgumentCaptor = ArgumentCaptor.forClass(FormContentProvider.class);
        verify(marketoRestClient, times(2)).doPost(eq(END_POINT + MarketoRESTEndpoint.GET_LEADS_BY_PROGRAM.getEndpoint(pathParamPath)), isNull(Map.class), immutableListMultimapArgumentCaptor.capture(), any(MarketoResponseJetty92EntityReader.class), formContentProviderArgumentCaptor.capture());
        String formContent = fromContentProviderToString(formContentProviderArgumentCaptor.getValue());
        List<ImmutableListMultimap> params = immutableListMultimapArgumentCaptor.getAllValues();
        Multimap params1 = params.get(0);
        assertEquals("GET", params1.get("_method").iterator().next());
        assertEquals("nextPageToken=z4MgsIiC5C%3D%3D%3D%3D%3D%3D&batchSize=300&fields=firstName%2ClastName", formContent);
    }

    private String fromContentProviderToString(ContentProvider formContentProvider)
    {
        Iterator<ByteBuffer> byteBufferIterator = formContentProvider.iterator();
        StringBuilder stringBuilder = new StringBuilder();
        while (byteBufferIterator.hasNext()) {
            ByteBuffer next = byteBufferIterator.next();
            stringBuilder.append(new String(next.array()));
        }
        return stringBuilder.toString();
    }

    @Test
    public void getLeadsByList() throws Exception
    {
        String listId = "listId";
        Map<String, String> pathParamPath = new HashMap<>();
        pathParamPath.put("list_id", listId);
        mockMarketoPageResponse("/fixtures/lead_by_list.json", END_POINT + MarketoRESTEndpoint.GET_LEADS_BY_LIST.getEndpoint(pathParamPath));
        RecordPagingIterable<ObjectNode> lists = marketoRestClient.getLeadsByList(listId, "firstName,lastName");
        Iterator<ObjectNode> iterator = lists.iterator();
        ObjectNode lead1 = iterator.next();
        ObjectNode lead2 = iterator.next();
        assertFalse(iterator.hasNext());
        assertEquals("John10093", lead1.get("firstName").asText());
        assertEquals("John10094", lead2.get("firstName").asText());
        ArgumentCaptor<ImmutableListMultimap> immutableListMultimapArgumentCaptor = ArgumentCaptor.forClass(ImmutableListMultimap.class);
        ArgumentCaptor<FormContentProvider> formContentProviderArgumentCaptor = ArgumentCaptor.forClass(FormContentProvider.class);
        verify(marketoRestClient, times(2)).doPost(eq(END_POINT + MarketoRESTEndpoint.GET_LEADS_BY_LIST.getEndpoint(pathParamPath)), isNull(Map.class), immutableListMultimapArgumentCaptor.capture(), any(MarketoResponseJetty92EntityReader.class), formContentProviderArgumentCaptor.capture());
        String formContent = fromContentProviderToString(formContentProviderArgumentCaptor.getValue());
        List<ImmutableListMultimap> params = immutableListMultimapArgumentCaptor.getAllValues();
        Multimap params1 = params.get(0);
        assertEquals("GET", params1.get("_method").iterator().next());
        assertEquals("nextPageToken=z4MgsIiC5C%3D%3D%3D%3D%3D%3D&batchSize=300&fields=firstName%2ClastName", formContent);
    }

    @Test
    public void getCampaign() throws Exception
    {
        mockMarketoPageResponse("/fixtures/campaign_response.json", END_POINT + MarketoRESTEndpoint.GET_CAMPAIGN.getEndpoint());
        RecordPagingIterable<ObjectNode> lists = marketoRestClient.getCampaign();
        Iterator<ObjectNode> iterator = lists.iterator();
        ObjectNode campaign1 = iterator.next();
        ObjectNode campaign2 = iterator.next();
        assertFalse(iterator.hasNext());
        assertEquals("Opened Sales Email", campaign1.get("name").asText());
        assertEquals("Clicks Link in Email", campaign2.get("name").asText());
        ArgumentCaptor<ImmutableListMultimap> immutableListMultimapArgumentCaptor = ArgumentCaptor.forClass(ImmutableListMultimap.class);
        ArgumentCaptor<FormContentProvider> formContentProviderArgumentCaptor = ArgumentCaptor.forClass(FormContentProvider.class);
        verify(marketoRestClient, times(2)).doPost(eq(END_POINT + MarketoRESTEndpoint.GET_CAMPAIGN.getEndpoint()), isNull(Map.class), immutableListMultimapArgumentCaptor.capture(), any(MarketoResponseJetty92EntityReader.class), formContentProviderArgumentCaptor.capture());
        String content = fromContentProviderToString(formContentProviderArgumentCaptor.getValue());

        List<ImmutableListMultimap> params = immutableListMultimapArgumentCaptor.getAllValues();
        Multimap params1 = params.get(0);
        assertEquals("GET", params1.get("_method").iterator().next());
        assertEquals("nextPageToken=z4MgsIiC5C%3D%3D%3D%3D%3D%3D&batchSize=300", content);
    }
}
