package org.embulk.input.marketo.rest;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteStreams;
import com.google.common.base.Optional;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.embulk.input.marketo.MarketoInputPlugin.CONFIG_MAPPER_FACTORY;
import static org.embulk.input.marketo.MarketoUtilsTest.CONFIG_MAPPER;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by khuutantaitai on 10/3/17.
 */
public class MarketoRestClientTest
{
    private static final String TEST_ACCOUNT_ID = "test_account_id";

    private static final String TEST_CLIENT_SECRET = "test_client_secret";

    private static final String TEST_CLIENT_ID = "test_client_id";

    private static final Optional<String> TEST_ENDPOINT = Optional.absent();

    private static final String END_POINT = MarketoUtils.getEndPoint(TEST_ACCOUNT_ID,TEST_ENDPOINT);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();

    private MarketoRestClient marketoRestClient;

    private static final JavaType RESPONSE_TYPE = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(MarketoResponse.class, MarketoResponse.class, ObjectNode.class);

    @Before
    public void prepare()
    {
        ConfigSource configSource = CONFIG_MAPPER_FACTORY.newConfigSource();
        configSource.set("account_id", TEST_ACCOUNT_ID);
        configSource.set("client_secret", TEST_CLIENT_SECRET);
        configSource.set("client_id", TEST_CLIENT_ID);
        configSource.set("max_return", 2);
        MarketoRestClient.PluginTask task = CONFIG_MAPPER.map(configSource, MarketoRestClient.PluginTask.class);
        MarketoRestClient realRestClient = new MarketoRestClient(task);
        marketoRestClient = spy(realRestClient);
    }

    @Test
    public void describeLead() throws Exception
    {
        String leadSchema = new String(ByteStreams.toByteArray(this.getClass().getResourceAsStream("/fixtures/lead_describe.json")));
        MarketoResponse<ObjectNode> marketoResponse = OBJECT_MAPPER.readValue(leadSchema, RESPONSE_TYPE);
        doReturn(marketoResponse).when(marketoRestClient).doGet(eq(END_POINT + MarketoRESTEndpoint.DESCRIBE_LEAD.getEndpoint()), isNull(), isNull(), any(MarketoResponseJetty92EntityReader.class));
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
        doReturn(marketoResponse).when(marketoRestClient).doPost(eq(END_POINT + MarketoRESTEndpoint.CREATE_LEAD_EXTRACT.getEndpoint()), isNull(), isNull(), argumentCaptor.capture(), any(MarketoResponseJetty92EntityReader.class));
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
    public void createLeadBulkExtractWithError()
    {
        MarketoResponse<ObjectNode> marketoResponse = new MarketoResponse<>();
        marketoResponse.setSuccess(false);
        MarketoError marketoError = new MarketoError();
        marketoError.setCode("ErrorCode1");
        marketoError.setMessage("Message");
        marketoResponse.setErrors(Arrays.asList(marketoError));
        doReturn(marketoResponse).when(marketoRestClient).doPost(eq(END_POINT + MarketoRESTEndpoint.CREATE_LEAD_EXTRACT.getEndpoint()), isNull(), isNull(), anyString(), any(MarketoResponseJetty92EntityReader.class));
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
    public void createActivityExtract() throws Exception
    {
        MarketoResponse<ObjectNode> marketoResponse = new MarketoResponse<>();
        marketoResponse.setSuccess(true);
        Date startDate = new Date(1506865856000L);
        Date endDate = new Date(1507297856000L);
        List<Integer> activityTypeIds = new ArrayList<>();
        ObjectNode bulkExtractResult = OBJECT_MAPPER.createObjectNode();
        bulkExtractResult.set("exportId", new TextNode("bulkExtractId"));
        marketoResponse.setResult(Arrays.asList(bulkExtractResult));
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        doReturn(marketoResponse).when(marketoRestClient).doPost(eq(END_POINT + MarketoRESTEndpoint.CREATE_ACTIVITY_EXTRACT.getEndpoint()), isNull(), isNull(), argumentCaptor.capture(), any(MarketoResponseJetty92EntityReader.class));
        String bulkExtractId = marketoRestClient.createActivityExtract(activityTypeIds, startDate, endDate);
        Assert.assertEquals("bulkExtractId", bulkExtractId);
        String postContent = argumentCaptor.getValue();
        ObjectNode marketoBulkExtractRequest = (ObjectNode) OBJECT_MAPPER.readTree(postContent);
        ObjectNode filter = (ObjectNode) marketoBulkExtractRequest.get("filter");
        Assert.assertTrue(filter.has("createdAt"));
    }

    @Test
    public void startLeadBulkExtract()
    {
        String bulkExportId = "bulkExportId";
        Map<String, String> pathParams = new HashMap<>();
        pathParams.put("export_id", bulkExportId);
        MarketoResponse<ObjectNode> marketoResponse = new MarketoResponse<>();
        marketoResponse.setSuccess(true);
        doReturn(marketoResponse).when(marketoRestClient).doPost(eq(END_POINT + MarketoRESTEndpoint.START_LEAD_EXPORT_JOB.getEndpoint(pathParams)), isNull(), isNull(), isNull(), any(MarketoResponseJetty92EntityReader.class));
        marketoRestClient.startLeadBulkExtract(bulkExportId);
        verify(marketoRestClient, times(1)).doPost(eq(END_POINT + MarketoRESTEndpoint.START_LEAD_EXPORT_JOB.getEndpoint(pathParams)), isNull(), isNull(), isNull(), any(MarketoResponseJetty92EntityReader.class));
    }

    @Test
    public void startLeadBulkExtractWithError()
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
        doReturn(marketoResponse).when(marketoRestClient).doPost(eq(END_POINT + MarketoRESTEndpoint.START_LEAD_EXPORT_JOB.getEndpoint(pathParams)), isNull(), isNull(), isNull(), any(MarketoResponseJetty92EntityReader.class));
        try {
            marketoRestClient.startLeadBulkExtract(bulkExportId);
        }
        catch (DataException ex) {
            verify(marketoRestClient, times(1)).doPost(eq(END_POINT + MarketoRESTEndpoint.START_LEAD_EXPORT_JOB.getEndpoint(pathParams)), isNull(), isNull(), isNull(), any(MarketoResponseJetty92EntityReader.class));
            return;
        }
        Assert.fail();
    }

    @Test
    public void startActivityBulkExtract()
    {
        String bulkExportId = "bulkExportId";
        Map<String, String> pathParams = new HashMap<>();
        pathParams.put("export_id", bulkExportId);
        MarketoResponse<ObjectNode> marketoResponse = new MarketoResponse<>();
        marketoResponse.setSuccess(true);
        doReturn(marketoResponse).when(marketoRestClient).doPost(eq(END_POINT + MarketoRESTEndpoint.START_ACTIVITY_EXPORT_JOB.getEndpoint(pathParams)), isNull(), isNull(), isNull(), any(MarketoResponseJetty92EntityReader.class));
        marketoRestClient.startActitvityBulkExtract(bulkExportId);
        verify(marketoRestClient, times(1)).doPost(eq(END_POINT + MarketoRESTEndpoint.START_ACTIVITY_EXPORT_JOB.getEndpoint(pathParams)), isNull(), isNull(), isNull(), any(MarketoResponseJetty92EntityReader.class));
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
        doReturn(marketoResponse).when(marketoRestClient).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), isNull(), isNull(), any(MarketoResponseJetty92EntityReader.class));
        marketoRestClient.waitLeadExportJobComplete(bulkExportId, 1, 4);
        verify(marketoRestClient, times(3)).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), isNull(), isNull(), any(MarketoResponseJetty92EntityReader.class));
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
        doReturn(marketoResponse).when(marketoRestClient).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), isNull(), isNull(), any(MarketoResponseJetty92EntityReader.class));
        try {
            marketoRestClient.waitLeadExportJobComplete(bulkExportId, 2, 4);
        }
        catch (DataException e) {
            Assert.assertTrue(e.getMessage().contains("Job timeout exception"));
            verify(marketoRestClient, times(2)).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), isNull(), isNull(), any(MarketoResponseJetty92EntityReader.class));
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
        MarketoResponse<ObjectNode> marketoResponse = mock(MarketoResponse.class);
        when(marketoResponse.isSuccess()).thenReturn(true);
        ObjectNode result = mock(ObjectNode.class);
        when(marketoResponse.getResult()).thenReturn(Arrays.asList(result));
        when(result.get("status")).thenReturn(new TextNode("Queued")).thenReturn(new TextNode("Failed"));
        when(result.get("errorMsg")).thenReturn(new TextNode("ErrorMessage"));
        doReturn(marketoResponse).when(marketoRestClient).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), isNull(), isNull(), any(MarketoResponseJetty92EntityReader.class));
        try {
            marketoRestClient.waitLeadExportJobComplete(bulkExportId, 1, 4);
        }
        catch (DataException e) {
            Assert.assertTrue(e.getMessage().contains("Bulk extract job failed"));
            Assert.assertTrue(e.getMessage().contains("ErrorMessage"));
            verify(marketoRestClient, times(2)).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS.getEndpoint(pathParams)), isNull(), isNull(), any(MarketoResponseJetty92EntityReader.class));
            return;
        }
        Assert.fail();
    }

    @Test
    public void waitActivityExportJobComplete() throws Exception
    {
        String exportId = "exportId";
        Map<String, String> pathParamMap = new HashMap<>();
        pathParamMap.put("export_id", exportId);
        MarketoResponse<ObjectNode> marketoResponse = mock(MarketoResponse.class);
        when(marketoResponse.isSuccess()).thenReturn(true);
        ObjectNode mockObjectNode = mock(ObjectNode.class);
        when(marketoResponse.getResult()).thenReturn(Arrays.asList(mockObjectNode));
        when(mockObjectNode.get("status")).thenReturn(new TextNode("Completed"));
        doReturn(marketoResponse).when(marketoRestClient).doGet(anyString(), isNull(), isNull(), any(Jetty92ResponseReader.class));
        marketoRestClient.waitActitvityExportJobComplete(exportId, 1, 3);
        verify(marketoRestClient, times(1)).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_ACTIVITY_EXPORT_STATUS.getEndpoint(pathParamMap)), isNull(), isNull(), any(Jetty92ResponseReader.class));
    }

    @Test
    public void getLeadBulkExtractResult()
    {
        String exportId = "exportId";
        Map<String, String> pathParamMap = new HashMap<>();
        pathParamMap.put("export_id", exportId);
        doReturn(mock(InputStream.class)).when(marketoRestClient).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_RESULT.getEndpoint(pathParamMap)), any(Map.class), isNull(), any(MarketoInputStreamResponseEntityReader.class));
        marketoRestClient.getLeadBulkExtractResult(exportId, null);
        verify(marketoRestClient, times(1)).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_LEAD_EXPORT_RESULT.getEndpoint(pathParamMap)), any(Map.class), isNull(), any(MarketoInputStreamResponseEntityReader.class));
    }

    @Test
    public void getActivitiesBulkExtractResult()
    {
        String exportId = "exportId";
        Map<String, String> pathParamMap = new HashMap<>();
        pathParamMap.put("export_id", exportId);
        doReturn(mock(InputStream.class)).when(marketoRestClient).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_ACTIVITY_EXPORT_RESULT.getEndpoint(pathParamMap)), any(Map.class), isNull(), any(MarketoInputStreamResponseEntityReader.class));
        marketoRestClient.getActivitiesBulkExtractResult(exportId, null);
        verify(marketoRestClient, times(1)).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_ACTIVITY_EXPORT_RESULT.getEndpoint(pathParamMap)), any(Map.class), isNull(), any(MarketoInputStreamResponseEntityReader.class));
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
        verify(marketoRestClient, times(2)).doPost(eq(END_POINT + MarketoRESTEndpoint.GET_LISTS.getEndpoint()), isNull(), immutableListMultimapArgumentCaptor.capture(), any(MarketoResponseJetty92EntityReader.class), formContentProviderArgumentCaptor.capture());
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
        doReturn(page1).doReturn(page2).when(marketoRestClient).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_PROGRAMS.getEndpoint()), isNull(), any(Multimap.class), any(MarketoResponseJetty92EntityReader.class));
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
        verify(marketoRestClient, times(2)).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_PROGRAMS.getEndpoint()), isNull(), immutableListMultimapArgumentCaptor.capture(), any(MarketoResponseJetty92EntityReader.class));
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
        doReturn(page1).doReturn(page2).when(marketoRestClient).doPost(eq(mockEndpoint), isNull(), any(Multimap.class), any(MarketoResponseJetty92EntityReader.class), any(FormContentProvider.class));
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
        verify(marketoRestClient, times(2)).doPost(eq(END_POINT + MarketoRESTEndpoint.GET_LEADS_BY_PROGRAM.getEndpoint(pathParamPath)), isNull(), immutableListMultimapArgumentCaptor.capture(), any(MarketoResponseJetty92EntityReader.class), formContentProviderArgumentCaptor.capture());
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
        verify(marketoRestClient, times(2)).doPost(eq(END_POINT + MarketoRESTEndpoint.GET_LEADS_BY_LIST.getEndpoint(pathParamPath)), isNull(), immutableListMultimapArgumentCaptor.capture(), any(MarketoResponseJetty92EntityReader.class), formContentProviderArgumentCaptor.capture());
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
        verify(marketoRestClient, times(2)).doPost(eq(END_POINT + MarketoRESTEndpoint.GET_CAMPAIGN.getEndpoint()), isNull(), immutableListMultimapArgumentCaptor.capture(), any(MarketoResponseJetty92EntityReader.class), formContentProviderArgumentCaptor.capture());
        String content = fromContentProviderToString(formContentProviderArgumentCaptor.getValue());

        List<ImmutableListMultimap> params = immutableListMultimapArgumentCaptor.getAllValues();
        Multimap params1 = params.get(0);
        Assert.assertEquals("GET", params1.get("_method").iterator().next());
        Assert.assertEquals("nextPageToken=z4MgsIiC5C%3D%3D%3D%3D%3D%3D&batchSize=300", content);
    }

    @Test
    public void testGetProgramsByTagType() throws Exception
    {
        ArrayNode listPages = (ArrayNode) OBJECT_MAPPER.readTree(new String(ByteStreams.toByteArray(this.getClass().getResourceAsStream("/fixtures/program_response.json")))).get("responses");
        MarketoResponse<ObjectNode> page1 = OBJECT_MAPPER.readValue(listPages.get(0).toString(), RESPONSE_TYPE);
        MarketoResponse<ObjectNode> page2 = OBJECT_MAPPER.readValue(listPages.get(1).toString(), RESPONSE_TYPE);

        final String tagType = "dummy_tag";
        final String tagValue = "dummy_value";

        doReturn(page1).doReturn(page2).when(marketoRestClient).doGet(
                        eq(END_POINT + MarketoRESTEndpoint.GET_PROGRAMS_BY_TAG.getEndpoint()),
                        ArgumentMatchers.isNull(),
                        any(Multimap.class),
                        any(MarketoResponseJetty92EntityReader.class));
        Iterable<ObjectNode> lists = marketoRestClient.getProgramsByTag(tagType, tagValue);
        Iterator<ObjectNode> iterator = lists.iterator();
        ObjectNode program1 = iterator.next();
        ObjectNode program2 = iterator.next();
        ObjectNode program3 = iterator.next();
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals("MB_Sep_25_test_program", program1.get("name").asText());
        Assert.assertEquals("TD Output Test Program", program2.get("name").asText());
        Assert.assertEquals("Bill_progream", program3.get("name").asText());

        ArgumentCaptor<ImmutableListMultimap> immutableListMultimapArgumentCaptor = ArgumentCaptor.forClass(ImmutableListMultimap.class);
        verify(marketoRestClient, times(2)).doGet(
                        eq(END_POINT + MarketoRESTEndpoint.GET_PROGRAMS_BY_TAG.getEndpoint()),
                        ArgumentMatchers.isNull(),
                        immutableListMultimapArgumentCaptor.capture(),
                        any(MarketoResponseJetty92EntityReader.class));
        List<ImmutableListMultimap> params = immutableListMultimapArgumentCaptor.getAllValues();

        ImmutableListMultimap params1 = params.get(0);
        Assert.assertEquals("0", params1.get("offset").get(0));
        Assert.assertEquals("2", params1.get("maxReturn").get(0));
        Assert.assertEquals("dummy_tag", params1.get("tagType").get(0));
        Assert.assertEquals("dummy_value", params1.get("tagValue").get(0));

        ImmutableListMultimap params2 = params.get(1);
        Assert.assertEquals("2", params2.get("offset").get(0));
        Assert.assertEquals("dummy_tag", params2.get("tagType").get(0));
        Assert.assertEquals("dummy_value", params2.get("tagValue").get(0));
    }

    @Test
    public void TestProgramsByDateRange() throws Exception
    {
        ArrayNode listPages = (ArrayNode) OBJECT_MAPPER.readTree(new String(ByteStreams.toByteArray(this.getClass().getResourceAsStream("/fixtures/program_response.json")))).get("responses");
        MarketoResponse<ObjectNode> page1 = OBJECT_MAPPER.readValue(listPages.get(0).toString(), RESPONSE_TYPE);
        MarketoResponse<ObjectNode> page2 = OBJECT_MAPPER.readValue(listPages.get(1).toString(), RESPONSE_TYPE);

        doReturn(page1).doReturn(page2).when(marketoRestClient).doGet(
                        eq(END_POINT + MarketoRESTEndpoint.GET_PROGRAMS.getEndpoint()),
                        ArgumentMatchers.isNull(),
                        any(Multimap.class),
                        any(MarketoResponseJetty92EntityReader.class));
        OffsetDateTime earliestUpdatedAt = OffsetDateTime.now().minusDays(10);
        OffsetDateTime latestUpdatedAt = earliestUpdatedAt.plusDays(5);
        String filterType = "filter1";
        List<String> filterValues = Arrays.asList("value1", "value2");

        Iterable<ObjectNode> lists = marketoRestClient.getProgramsByDateRange(Date.from(earliestUpdatedAt.toInstant()),
                Date.from(latestUpdatedAt.toInstant()), filterType, filterValues);
        Iterator<ObjectNode> iterator = lists.iterator();
        ObjectNode program1 = iterator.next();
        ObjectNode program2 = iterator.next();
        ObjectNode program3 = iterator.next();
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals("MB_Sep_25_test_program", program1.get("name").asText());
        Assert.assertEquals("TD Output Test Program", program2.get("name").asText());
        Assert.assertEquals("Bill_progream", program3.get("name").asText());

        ArgumentCaptor<ImmutableListMultimap> immutableListMultimapArgumentCaptor = ArgumentCaptor.forClass(ImmutableListMultimap.class);
        verify(marketoRestClient, times(2)).doGet(
                        eq(END_POINT + MarketoRESTEndpoint.GET_PROGRAMS.getEndpoint()),
                        ArgumentMatchers.isNull(),
                        immutableListMultimapArgumentCaptor.capture(),
                        any(MarketoResponseJetty92EntityReader.class));
        List<ImmutableListMultimap> params = immutableListMultimapArgumentCaptor.getAllValues();
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern(MarketoUtils.MARKETO_DATE_SIMPLE_DATE_FORMAT);

        ImmutableListMultimap params1 = params.get(0);
        Assert.assertEquals("0", params1.get("offset").get(0));
        Assert.assertEquals("2", params1.get("maxReturn").get(0));
        Assert.assertEquals(earliestUpdatedAt.format(fmt), params1.get("earliestUpdatedAt").get(0));
        Assert.assertEquals(latestUpdatedAt.format(fmt), params1.get("latestUpdatedAt").get(0));
        Assert.assertEquals("filter1", params1.get("filterType").get(0));
        Assert.assertEquals(String.join(",", filterValues), params1.get("filterValues").get(0));

        ImmutableListMultimap params2 = params.get(1);
        Assert.assertEquals("2", params2.get("offset").get(0));
        Assert.assertEquals(earliestUpdatedAt.format(fmt), params2.get("earliestUpdatedAt").get(0));
        Assert.assertEquals(latestUpdatedAt.format(fmt), params2.get("latestUpdatedAt").get(0));
        Assert.assertEquals("filter1", params2.get("filterType").get(0));
        Assert.assertEquals(String.join(",", filterValues), params2.get("filterValues").get(0));
    }

    @Test
    public void describeCustomObject() throws Exception
    {
        String customObjectSchema = new String(ByteStreams.toByteArray(this.getClass().getResourceAsStream("/fixtures/custom_object_describe.json")));
        MarketoResponse<ObjectNode> marketoResponse = OBJECT_MAPPER.readValue(customObjectSchema, RESPONSE_TYPE);
        String apiName = "custom_object";
        Map<String, String> pathParams = new HashMap<>();
        pathParams.put("api_name", apiName);
        doReturn(marketoResponse).when(marketoRestClient).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_CUSTOM_OBJECT_DESCRIBE.getEndpoint(pathParams)), isNull(), isNull(), any(MarketoResponseJetty92EntityReader.class));
        List<MarketoField> marketoFields = marketoRestClient.describeCustomObject(apiName);
        Assert.assertEquals(16, marketoFields.size());
        JavaType marketoFieldType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<MarketoField> expectedFields = OBJECT_MAPPER.readValue(new String(ByteStreams.toByteArray(this.getClass().getResourceAsStream("/fixtures/custom_object_expected.json"))), marketoFieldType);
        Assert.assertArrayEquals(expectedFields.toArray(), marketoFields.toArray());
    }

    @Test
    public void getCustomObject() throws Exception
    {
        String apiName = "custom_object";
        Map<String, String> pathParams = new HashMap<>();
        pathParams.put("api_name", apiName);

        ArrayNode listPages = (ArrayNode) OBJECT_MAPPER.readTree(new String(ByteStreams.toByteArray(this.getClass().getResourceAsStream("/fixtures/custom_object_response.json")))).get("responses");
        MarketoResponse<ObjectNode> page1 = OBJECT_MAPPER.readValue(listPages.get(0).toString(), RESPONSE_TYPE);
        doReturn(page1).when(marketoRestClient).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_CUSTOM_OBJECT.getEndpoint(pathParams)), isNull(), any(ImmutableListMultimap.class), any(MarketoResponseJetty92EntityReader.class));
        RecordPagingIterable<ObjectNode> pages = (RecordPagingIterable<ObjectNode>) marketoRestClient.getCustomObject(apiName, "id", null, 1, 2);
        Iterator<ObjectNode> iterator = pages.iterator();
        ObjectNode customObject1 = iterator.next();
        ObjectNode customObject2 = iterator.next();
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals("1", customObject1.get("id").asText());
        Assert.assertEquals("2", customObject2.get("id").asText());
    }

    @Test
    public void testGetProgramsByIds() throws IOException
    {
        Set<String> ids = new HashSet<>();
        ids.add("123");
        ids.add("456");

        ArrayNode listPages = (ArrayNode) OBJECT_MAPPER.readTree(new String(ByteStreams.toByteArray(this.getClass().getResourceAsStream("/fixtures/program_response.json")))).get("responses");
        MarketoResponse<ObjectNode> page1 = OBJECT_MAPPER.readValue(listPages.get(0).toString(), RESPONSE_TYPE);
        MarketoResponse<ObjectNode> page2 = OBJECT_MAPPER.readValue(listPages.get(1).toString(), RESPONSE_TYPE);
        doReturn(page1).doReturn(page2).when(marketoRestClient).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_PROGRAMS.getEndpoint()), isNull(), any(Multimap.class), any(MarketoResponseJetty92EntityReader.class));
        RecordPagingIterable<ObjectNode> lists = marketoRestClient.getProgramsByIds(ids);
        Iterator<ObjectNode> iterator = lists.iterator();
        ObjectNode program1 = iterator.next();
        ObjectNode program2 = iterator.next();
        ObjectNode program3 = iterator.next();
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals("MB_Sep_25_test_program", program1.get("name").asText());
        Assert.assertEquals("TD Output Test Program", program2.get("name").asText());
        Assert.assertEquals("Bill_progream", program3.get("name").asText());

        ArgumentCaptor<ImmutableListMultimap> paramCaptor = ArgumentCaptor.forClass(ImmutableListMultimap.class);
        verify(marketoRestClient, times(2)).doGet(eq(END_POINT + MarketoRESTEndpoint.GET_PROGRAMS.getEndpoint()), isNull(), paramCaptor.capture(), any(MarketoResponseJetty92EntityReader.class));

        List<ImmutableListMultimap> params = paramCaptor.getAllValues();

        ImmutableListMultimap params1 = params.get(0);
        Assert.assertEquals("id", params1.get("filterType").get(0));
        Assert.assertEquals("123,456", params1.get("filterValues").get(0));

        ImmutableListMultimap params2 = params.get(1);
        Assert.assertEquals(params1.get("filterType").get(0), params2.get("filterType").get(0));
        Assert.assertEquals(params1.get("filterValues").get(0), params2.get("filterValues").get(0));
    }

    @Test
    public void testGetListsByIds() throws IOException
    {
        Set<String> ids = new HashSet<>();
        ids.add("123");
        ids.add("456");

        mockMarketoPageResponse("/fixtures/lists_response.json", END_POINT + MarketoRESTEndpoint.GET_LISTS.getEndpoint());
        RecordPagingIterable<ObjectNode> lists = marketoRestClient.getListsByIds(ids);
        Iterator<ObjectNode> iterator = lists.iterator();
        ObjectNode list1 = iterator.next();
        ObjectNode list2 = iterator.next();

        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals("Test list 1", list1.get("name").asText());
        Assert.assertEquals("Test list 2", list2.get("name").asText());

        ArgumentCaptor<ImmutableListMultimap> paramsCaptor = ArgumentCaptor.forClass(ImmutableListMultimap.class);
        ArgumentCaptor<FormContentProvider> formCaptor = ArgumentCaptor.forClass(FormContentProvider.class);
        verify(marketoRestClient, times(2)).doPost(eq(END_POINT + MarketoRESTEndpoint.GET_LISTS.getEndpoint()), isNull(), paramsCaptor.capture(), any(MarketoResponseJetty92EntityReader.class), formCaptor.capture());

        ImmutableListMultimap params = paramsCaptor.getValue();
        Assert.assertEquals("GET", params.get("_method").iterator().next());

        FormContentProvider form = formCaptor.getValue();
        Assert.assertEquals("nextPageToken=GWP55GLCVCZLPE6SS7OCG5IEXQ%3D%3D%3D%3D%3D%3D&id=123%2C456&batchSize=300", fromContentProviderToString(form));
    }
}
