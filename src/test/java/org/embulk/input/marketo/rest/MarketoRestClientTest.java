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
import com.sun.org.apache.xpath.internal.SourceTree;
import org.eclipse.jetty.client.api.ContentProvider;
import org.eclipse.jetty.client.util.FormContentProvider;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.MarketoError;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.model.MarketoResponse;
import org.embulk.spi.DataException;
import org.embulk.util.retryhelper.jetty92.Jetty92ResponseReader;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

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
        mockRetryHelper = Mockito.mock(Jetty92RetryHelper.class);
        MarketoRestClient realRestClient = new MarketoRestClient(task, mockRetryHelper);
        marketoRestClient = Mockito.spy(realRestClient);
    }

    @Test
    public void describeLead() throws Exception
    {
        String leadSchema = new String(ByteStreams.toByteArray(this.getClass().getResourceAsStream("/fixtures/lead_describe.json")));
        MarketoResponse<ObjectNode> marketoResponse = OBJECT_MAPPER.readValue(leadSchema, RESPONSE_TYPE);
        Mockito.doReturn(marketoResponse).when(marketoRestClient).doGet(Mockito.eq(END_POINT + MarketoRESTEndpoint.DESCRIBE_LEAD.getEndpoint()), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.any(MarketoResponseJetty92EntityReader.class));
        List<MarketoField> marketoFields = marketoRestClient.describeLead();
        Assert.assertEquals(16, marketoFields.size());
        JavaType marketoFieldType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<MarketoField> expectedFields = OBJECT_MAPPER.readValue(new String(ByteStreams.toByteArray(this.getClass().getResourceAsStream("/fixtures/lead_describe_expected.json"))), marketoFieldType);
        Assert.assertArrayEquals(expectedFields.toArray(), marketoFields.toArray());
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
        Mockito.doReturn(marketoResponse).when(marketoRestClient).doPost(Mockito.eq(END_POINT + MarketoRESTEndpoint.CREATE_LEAD_EXTRACT.getEndpoint()), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), argumentCaptor.capture(), Mockito.any(MarketoResponseJetty92EntityReader.class));
        String filterField = "filterField";
        String bulkExtractId = marketoRestClient.createLeadBulkExtract(startDate, endDate, Arrays.asList("extract_field1", "extract_field2"), filterField);
        Assert.assertEquals("bulkExtractId", bulkExtractId);
        String postContent = argumentCaptor.getValue();
        ObjectNode marketoBulkExtractRequest = (ObjectNode) OBJECT_MAPPER.readTree(postContent);
        ObjectNode filter = (ObjectNode) marketoBulkExtractRequest.get("filter");
        ObjectNode dateRangeFilter = (ObjectNode) filter.get(filterField);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(MarketoUtils.MARKETO_DATE_SIMPLE_DATE_FORMAT);
        Assert.assertEquals(simpleDateFormat.format(startDate), dateRangeFilter.get("startAt").textValue());
        Assert.assertEquals(simpleDateFormat.format(endDate), dateRangeFilter.get("endAt").textValue());
        Assert.assertEquals("CSV", marketoBulkExtractRequest.get("format").textValue());
        ArrayNode fields = (ArrayNode) marketoBulkExtractRequest.get("fields");
        Assert.assertEquals("extract_field1", fields.get(0).textValue());
        Assert.assertEquals("extract_field2", fields.get(1).textValue());
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
        Mockito.doReturn(marketoResponse).when(marketoRestClient).doPost(Mockito.eq(END_POINT + MarketoRESTEndpoint.CREATE_LEAD_EXTRACT.getEndpoint()), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.anyString(), Mockito.any(MarketoResponseJetty92EntityReader.class));
        String filterField = "filterField";
        try {
            marketoRestClient.createLeadBulkExtract(new Date(), new Date(), Arrays.asList("extract_field1", "extract_field2"), filterField);
        }
        catch (DataException ex) {
            Assert.assertEquals("ErrorCode1: Message", ex.getMessage());
            return;
        }
        Assert.fail();
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
        Mockito.doReturn(marketoResponse).when(marketoRestClient).doPost(Mockito.eq(END_POINT + MarketoRESTEndpoint.CREATE_ACTIVITY_EXTRACT.getEndpoint()), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), argumentCaptor.capture(), Mockito.any(MarketoResponseJetty92EntityReader.class));
        String bulkExtractId = marketoRestClient.createActivityExtract(startDate, endDate);
        Assert.assertEquals("bulkExtractId", bulkExtractId);
        String postContent = argumentCaptor.getValue();
        ObjectNode marketoBulkExtractRequest = (ObjectNode) OBJECT_MAPPER.readTree(postContent);
        ObjectNode filter = (ObjectNode) marketoBulkExtractRequest.get("filter");
        Assert.assertTrue(filter.has("createdAt"));
    }

    @Test
    public void startLeadBulkExtract() throws Exception
    {
        String bulkExportId = "bulkExportId";
        Map<String, String> pathParams = new HashMap<>();
        pathParams.put("export_id", bulkExportId);
        MarketoResponse<ObjectNode> marketoResponse = new MarketoResponse<>();
        marketoResponse.setSuccess(true);
        Mockito.doReturn(marketoResponse).when(marketoRestClient).doPost(Mockito.eq(END_POINT + MarketoRESTEndpoint.START_LEAD_EXPORT_JOB.getEndpoint(pathParams)), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.isNull(String.class), Mockito.any(MarketoResponseJetty92EntityReader.class));
        marketoRestClient.startLeadBulkExtract(bulkExportId);
        Mockito.verify(marketoRestClient, Mockito.times(1)).doPost(Mockito.eq(END_POINT + MarketoRESTEndpoint.START_LEAD_EXPORT_JOB.getEndpoint(pathParams)), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.isNull(String.class), Mockito.any(MarketoResponseJetty92EntityReader.class));
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
        Mockito.doReturn(marketoResponse).when(marketoRestClient).doPost(Mockito.eq(END_POINT + MarketoRESTEndpoint.START_LEAD_EXPORT_JOB.getEndpoint(pathParams)), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.isNull(String.class), Mockito.any(MarketoResponseJetty92EntityReader.class));
        try {
            marketoRestClient.startLeadBulkExtract(bulkExportId);
        }
        catch (DataException ex) {
            Mockito.verify(marketoRestClient, Mockito.times(1)).doPost(Mockito.eq(END_POINT + MarketoRESTEndpoint.START_LEAD_EXPORT_JOB.getEndpoint(pathParams)), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.isNull(String.class), Mockito.any(MarketoResponseJetty92EntityReader.class));
            return;
        }
        Assert.fail();
    }

    @Test
    public void startActivityBulkExtract() throws Exception
    {
        String bulkExportId = "bulkExportId";
        Map<String, String> pathParams = new HashMap<>();
        pathParams.put("export_id", bulkExportId);
        MarketoResponse<ObjectNode> marketoResponse = new MarketoResponse<>();
        marketoResponse.setSuccess(true);
        Mockito.doReturn(marketoResponse).when(marketoRestClient).doPost(Mockito.eq(END_POINT + MarketoRESTEndpoint.START_ACTIVITY_EXPORT_JOB.getEndpoint(pathParams)), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.isNull(String.class), Mockito.any(MarketoResponseJetty92EntityReader.class));
        marketoRestClient.startActitvityBulkExtract(bulkExportId);
        Mockito.verify(marketoRestClient, Mockito.times(1)).doPost(Mockito.eq(END_POINT + MarketoRESTEndpoint.START_ACTIVITY_EXPORT_JOB.getEndpoint(pathParams)), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.isNull(String.class), Mockito.any(MarketoResponseJetty92EntityReader.class));
    }

    @Test
    public void waitLeadExportJobComplete() throws Exception
    {
        String bulkExportId = "bulkExportId";
        Map<String, String> pathParams = new HashMap<>();
        pathParams.put("export_id", bulkExportId);
        MarketoResponse<ObjectNode> marketoResponse = Mockito.mock(MarketoResponse.class);
        Mockito.when(marketoResponse.isSuccess()).thenReturn(true);
        ObjectNode result = Mockito.mock(ObjectNode.class);
        Mockito.when(marketoResponse.getResult()).thenReturn(Arrays.asList(result));
        Mockito.when(result.get("status")).thenReturn(new TextNode("Queued")).thenReturn(new TextNode("Processing")).thenReturn(new TextNode("Completed"));
        Mockito.doReturn(marketoResponse).when(marketoRestClient).doGet(Mockito.eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.any(MarketoResponseJetty92EntityReader.class));
        marketoRestClient.waitLeadExportJobComplete(bulkExportId, 1, 4);
        Mockito.verify(marketoRestClient, Mockito.times(3)).doGet(Mockito.eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.any(MarketoResponseJetty92EntityReader.class));
    }

    @Test
    public void waitLeadExportJobTimeOut() throws Exception
    {
        String bulkExportId = "bulkExportId";
        Map<String, String> pathParams = new HashMap<>();
        pathParams.put("export_id", bulkExportId);
        MarketoResponse<ObjectNode> marketoResponse = Mockito.mock(MarketoResponse.class);
        Mockito.when(marketoResponse.isSuccess()).thenReturn(true);
        ObjectNode result = Mockito.mock(ObjectNode.class);
        Mockito.when(marketoResponse.getResult()).thenReturn(Arrays.asList(result));
        Mockito.when(result.get("status")).thenReturn(new TextNode("Queued")).thenReturn(new TextNode("Processing"));
        Mockito.doReturn(marketoResponse).when(marketoRestClient).doGet(Mockito.eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.any(MarketoResponseJetty92EntityReader.class));
        try {
            marketoRestClient.waitLeadExportJobComplete(bulkExportId, 2, 4);
        }
        catch (DataException e) {
            Assert.assertTrue(e.getMessage().contains("Job timeout exception"));
            Mockito.verify(marketoRestClient, Mockito.times(2)).doGet(Mockito.eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.any(MarketoResponseJetty92EntityReader.class));
            return;
        }
        Assert.fail();
    }

    @Test
    public void waitLeadExportJobFailed() throws Exception
    {
        String bulkExportId = "bulkExportId";
        Map<String, String> pathParams = new HashMap<>();
        pathParams.put("export_id", bulkExportId);
        MarketoResponse<ObjectNode> marketoResponse = Mockito.mock(MarketoResponse.class);
        Mockito.when(marketoResponse.isSuccess()).thenReturn(true);
        ObjectNode result = Mockito.mock(ObjectNode.class);
        Mockito.when(marketoResponse.getResult()).thenReturn(Arrays.asList(result));
        Mockito.when(result.get("status")).thenReturn(new TextNode("Queued")).thenReturn(new TextNode("Failed"));
        Mockito.when(result.get("errorMsg")).thenReturn(new TextNode("ErrorMessage"));
        Mockito.doReturn(marketoResponse).when(marketoRestClient).doGet(Mockito.eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.any(MarketoResponseJetty92EntityReader.class));
        try {
            marketoRestClient.waitLeadExportJobComplete(bulkExportId, 1, 4);
        }
        catch (DataException e) {
            Assert.assertTrue(e.getMessage().contains("Bulk extract job failed"));
            Assert.assertTrue(e.getMessage().contains("ErrorMessage"));
            Mockito.verify(marketoRestClient, Mockito.times(2)).doGet(Mockito.eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.any(MarketoResponseJetty92EntityReader.class));
            return;
        }
        Assert.fail();
    }

    @Test
    public void waitActitvityExportJobComplete() throws Exception
    {
        String exportId = "exportId";
        Map<String, String> pathParamMap = new HashMap<>();
        pathParamMap.put("export_id", exportId);
        MarketoResponse<ObjectNode> marketoResponse = Mockito.mock(MarketoResponse.class);
        Mockito.when(marketoResponse.isSuccess()).thenReturn(true);
        ObjectNode mockObjectNode = Mockito.mock(ObjectNode.class);
        Mockito.when(marketoResponse.getResult()).thenReturn(Arrays.asList(mockObjectNode));
        Mockito.when(mockObjectNode.get("status")).thenReturn(new TextNode("Completed"));
        Mockito.doReturn(marketoResponse).when(marketoRestClient).doGet(Mockito.anyString(), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.any(Jetty92ResponseReader.class));
        marketoRestClient.waitActitvityExportJobComplete(exportId, 1, 3);
        Mockito.verify(marketoRestClient, Mockito.times(1)).doGet(Mockito.eq(END_POINT + MarketoRESTEndpoint.GET_ACTIVITY_EXPORT_STATUS.getEndpoint(pathParamMap)), Mockito.isNull(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.any(Jetty92ResponseReader.class));
    }

    @Test
    public void getLeadBulkExtractResult() throws Exception
    {
        String exportId = "exportId";
        Map<String, String> pathParamMap = new HashMap<>();
        pathParamMap.put("export_id", exportId);
        Mockito.doReturn(Mockito.mock(InputStream.class)).when(marketoRestClient).doGet(Mockito.eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_RESULT.getEndpoint(pathParamMap)), Mockito.any(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.any(MarketoInputStreamResponseEntityReader.class));
        marketoRestClient.getLeadBulkExtractResult(exportId, null);
        Mockito.verify(marketoRestClient, Mockito.times(1)).doGet(Mockito.eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_RESULT.getEndpoint(pathParamMap)), Mockito.any(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.any(MarketoInputStreamResponseEntityReader.class));
    }

    @Test
    public void getActivitiesBulkExtractResult() throws Exception
    {
        String exportId = "exportId";
        Map<String, String> pathParamMap = new HashMap<>();
        pathParamMap.put("export_id", exportId);
        Mockito.doReturn(Mockito.mock(InputStream.class)).when(marketoRestClient).doGet(Mockito.eq(END_POINT + MarketoRESTEndpoint.GET_ACTIVITY_EXPORT_RESULT.getEndpoint(pathParamMap)), Mockito.any(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.any(MarketoInputStreamResponseEntityReader.class));
        marketoRestClient.getActivitiesBulkExtractResult(exportId, null);
        Mockito.verify(marketoRestClient, Mockito.times(1)).doGet(Mockito.eq(END_POINT + MarketoRESTEndpoint.GET_ACTIVITY_EXPORT_RESULT.getEndpoint(pathParamMap)), Mockito.any(Map.class), Mockito.isNull(ImmutableListMultimap.class), Mockito.any(MarketoInputStreamResponseEntityReader.class));
    }

    @Test
    public void getLists() throws Exception
    {
        mockMarketoPageResponse("/fixtures/lists_response.json", END_POINT + MarketoRESTEndpoint.GET_LISTS.getEndpoint());
        RecordPagingIterable<ObjectNode> lists = marketoRestClient.getLists();
        Iterator<ObjectNode> iterator = lists.iterator();
        ObjectNode list1 = iterator.next();
        ObjectNode list2 = iterator.next();
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals("Test list 1", list1.get("name").asText());
        Assert.assertEquals("Test list 2", list2.get("name").asText());
        ArgumentCaptor<Multimap> immutableListMultimapArgumentCaptor = ArgumentCaptor.forClass(Multimap.class);
        ArgumentCaptor<FormContentProvider> formContentProviderArgumentCaptor = ArgumentCaptor.forClass(FormContentProvider.class);
        Mockito.verify(marketoRestClient, Mockito.times(2)).doPost(Mockito.eq(END_POINT + MarketoRESTEndpoint.GET_LISTS.getEndpoint()), Mockito.isNull(Map.class), immutableListMultimapArgumentCaptor.capture(), Mockito.any(MarketoResponseJetty92EntityReader.class), formContentProviderArgumentCaptor.capture());
        List<Multimap> params = immutableListMultimapArgumentCaptor.getAllValues();
        Multimap params1 = params.get(0);
        Assert.assertEquals("GET", params1.get("_method").iterator().next());
        Assert.assertEquals("nextPageToken=GWP55GLCVCZLPE6SS7OCG5IEXQ%3D%3D%3D%3D%3D%3D&batchSize=300", fromContentProviderToString(formContentProviderArgumentCaptor.getValue()));
    }

    @Test
    public void getPrograms() throws Exception
    {
        ArrayNode listPages = (ArrayNode) OBJECT_MAPPER.readTree(new String(ByteStreams.toByteArray(this.getClass().getResourceAsStream("/fixtures/program_response.json")))).get("responses");
        MarketoResponse<ObjectNode> page1 = OBJECT_MAPPER.readValue(listPages.get(0).toString(), RESPONSE_TYPE);
        MarketoResponse<ObjectNode> page2 = OBJECT_MAPPER.readValue(listPages.get(1).toString(), RESPONSE_TYPE);
        Mockito.doReturn(page1).doReturn(page2).when(marketoRestClient).doGet(Mockito.eq(END_POINT + MarketoRESTEndpoint.GET_PROGRAMS.getEndpoint()), Mockito.isNull(Map.class), Mockito.any(Multimap.class), Mockito.any(MarketoResponseJetty92EntityReader.class));
        RecordPagingIterable<ObjectNode> lists = marketoRestClient.getPrograms();
        Iterator<ObjectNode> iterator = lists.iterator();
        ObjectNode program1 = iterator.next();
        ObjectNode program2 = iterator.next();
        ObjectNode program3 = iterator.next();
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals("MB_Sep_25_test_program", program1.get("name").asText());
        Assert.assertEquals("TD Output Test Program", program2.get("name").asText());
        Assert.assertEquals("Bill_progream", program3.get("name").asText());
        ArgumentCaptor<ImmutableListMultimap> immutableListMultimapArgumentCaptor = ArgumentCaptor.forClass(ImmutableListMultimap.class);
        Mockito.verify(marketoRestClient, Mockito.times(2)).doGet(Mockito.eq(END_POINT + MarketoRESTEndpoint.GET_PROGRAMS.getEndpoint()), Mockito.isNull(Map.class), immutableListMultimapArgumentCaptor.capture(), Mockito.any(MarketoResponseJetty92EntityReader.class));
        List<ImmutableListMultimap> params = immutableListMultimapArgumentCaptor.getAllValues();
        ImmutableListMultimap params1 = params.get(0);
        Assert.assertEquals("0", params1.get("offset").get(0));
        Assert.assertEquals("2", params1.get("maxReturn").get(0));
        ImmutableListMultimap params2 = params.get(1);
        Assert.assertEquals("2", params2.get("offset").get(0));
    }

    private void mockMarketoPageResponse(String fixtureName, String mockEndpoint) throws IOException
    {
        ArrayNode listPages = (ArrayNode) OBJECT_MAPPER.readTree(new String(ByteStreams.toByteArray(this.getClass().getResourceAsStream(fixtureName)))).get("responses");
        MarketoResponse<ObjectNode> page1 = OBJECT_MAPPER.readValue(listPages.get(0).toString(), RESPONSE_TYPE);
        MarketoResponse<ObjectNode> page2 = OBJECT_MAPPER.readValue(listPages.get(1).toString(), RESPONSE_TYPE);
        Mockito.doReturn(page1).doReturn(page2).when(marketoRestClient).doPost(Mockito.eq(mockEndpoint), Mockito.isNull(Map.class), Mockito.any(Multimap.class), Mockito.any(MarketoResponseJetty92EntityReader.class), Mockito.any(FormContentProvider.class));
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
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals("Tai 1", lead1.get("firstName").asText());
        Assert.assertEquals("Tai", lead2.get("firstName").asText());
        ArgumentCaptor<ImmutableListMultimap> immutableListMultimapArgumentCaptor = ArgumentCaptor.forClass(ImmutableListMultimap.class);
        ArgumentCaptor<FormContentProvider> formContentProviderArgumentCaptor = ArgumentCaptor.forClass(FormContentProvider.class);
        Mockito.verify(marketoRestClient, Mockito.times(2)).doPost(Mockito.eq(END_POINT + MarketoRESTEndpoint.GET_LEADS_BY_PROGRAM.getEndpoint(pathParamPath)), Mockito.isNull(Map.class), immutableListMultimapArgumentCaptor.capture(), Mockito.any(MarketoResponseJetty92EntityReader.class), formContentProviderArgumentCaptor.capture());
        String formContent = fromContentProviderToString(formContentProviderArgumentCaptor.getValue());
        List<ImmutableListMultimap> params = immutableListMultimapArgumentCaptor.getAllValues();
        Multimap params1 = params.get(0);
        Assert.assertEquals("GET", params1.get("_method").iterator().next());
        Assert.assertEquals("nextPageToken=z4MgsIiC5C%3D%3D%3D%3D%3D%3D&batchSize=300&fields=firstName%2ClastName", formContent);
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
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals("John10093", lead1.get("firstName").asText());
        Assert.assertEquals("John10094", lead2.get("firstName").asText());
        ArgumentCaptor<ImmutableListMultimap> immutableListMultimapArgumentCaptor = ArgumentCaptor.forClass(ImmutableListMultimap.class);
        ArgumentCaptor<FormContentProvider> formContentProviderArgumentCaptor = ArgumentCaptor.forClass(FormContentProvider.class);
        Mockito.verify(marketoRestClient, Mockito.times(2)).doPost(Mockito.eq(END_POINT + MarketoRESTEndpoint.GET_LEADS_BY_LIST.getEndpoint(pathParamPath)), Mockito.isNull(Map.class), immutableListMultimapArgumentCaptor.capture(), Mockito.any(MarketoResponseJetty92EntityReader.class), formContentProviderArgumentCaptor.capture());
        String formContent = fromContentProviderToString(formContentProviderArgumentCaptor.getValue());
        List<ImmutableListMultimap> params = immutableListMultimapArgumentCaptor.getAllValues();
        Multimap params1 = params.get(0);
        Assert.assertEquals("GET", params1.get("_method").iterator().next());
        Assert.assertEquals("nextPageToken=z4MgsIiC5C%3D%3D%3D%3D%3D%3D&batchSize=300&fields=firstName%2ClastName", formContent);
    }

    @Test
    public void getCampaign() throws Exception
    {
        mockMarketoPageResponse("/fixtures/campaign_response.json", END_POINT + MarketoRESTEndpoint.GET_CAMPAIGN.getEndpoint());
        RecordPagingIterable<ObjectNode> lists = marketoRestClient.getCampaign();
        Iterator<ObjectNode> iterator = lists.iterator();
        ObjectNode campaign1 = iterator.next();
        ObjectNode campaign2 = iterator.next();
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals("Opened Sales Email", campaign1.get("name").asText());
        Assert.assertEquals("Clicks Link in Email", campaign2.get("name").asText());
        ArgumentCaptor<ImmutableListMultimap> immutableListMultimapArgumentCaptor = ArgumentCaptor.forClass(ImmutableListMultimap.class);
        ArgumentCaptor<FormContentProvider> formContentProviderArgumentCaptor = ArgumentCaptor.forClass(FormContentProvider.class);
        Mockito.verify(marketoRestClient, Mockito.times(2)).doPost(Mockito.eq(END_POINT + MarketoRESTEndpoint.GET_CAMPAIGN.getEndpoint()), Mockito.isNull(Map.class), immutableListMultimapArgumentCaptor.capture(), Mockito.any(MarketoResponseJetty92EntityReader.class), formContentProviderArgumentCaptor.capture());
        String content = fromContentProviderToString(formContentProviderArgumentCaptor.getValue());

        List<ImmutableListMultimap> params = immutableListMultimapArgumentCaptor.getAllValues();
        Multimap params1 = params.get(0);
        Assert.assertEquals("GET", params1.get("_method").iterator().next());
        Assert.assertEquals("nextPageToken=z4MgsIiC5C%3D%3D%3D%3D%3D%3D&batchSize=300", content);
    }
}
