package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.embulk.EmbulkTestRuntime;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.input.marketo.rest.RecordPagingIterable;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;

/**
 * Created by tai.khuu on 10/10/17.
 */
public class LeadWithListInputPluginTest
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();

    private ConfigSource configSource;

    private LeadWithListInputPlugin leadWithListInputPlugin;

    private MarketoRestClient mockMarketoRestClient;

    @Before
    public void setUp() throws Exception
    {
        leadWithListInputPlugin = Mockito.spy(new LeadWithListInputPlugin());
        ConfigLoader configLoader = embulkTestRuntime.getInjector().getInstance(ConfigLoader.class);
        configSource = configLoader.fromYaml(this.getClass().getResourceAsStream("/config/rest_config.yaml"));
        mockMarketoRestClient = Mockito.mock(MarketoRestClient.class);
        Mockito.doReturn(mockMarketoRestClient).when(leadWithListInputPlugin).createMarketoRestClient(any(LeadWithListInputPlugin.PluginTask.class));
    }

    @Test
    public void testRun() throws IOException
    {
        RecordPagingIterable<ObjectNode> mockLeadRecordPagingIterable = Mockito.mock(RecordPagingIterable.class);
        RecordPagingIterable<ObjectNode> mockLeadEmptyRecordPagingIterable = Mockito.mock(RecordPagingIterable.class);
        RecordPagingIterable<ObjectNode> mocklistRecords = Mockito.mock(RecordPagingIterable.class);

        Mockito.when(mockLeadEmptyRecordPagingIterable.iterator()).thenReturn(new ArrayList<ObjectNode>().iterator());
        JavaType objectNodeListType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, ObjectNode.class);
        JavaType marketoFieldsType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<ObjectNode> leads = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/lead_response_full.json"), objectNodeListType);
        List<ObjectNode> lists = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/list_reponse_full.json"), objectNodeListType);
        Mockito.when(mocklistRecords.iterator()).thenReturn(lists.iterator());
        List<MarketoField> marketoFields = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/lead_describe_marketo_fields_full.json"), marketoFieldsType);
        Mockito.when(mockLeadRecordPagingIterable.iterator()).thenReturn(leads.iterator());
        Mockito.when(mockMarketoRestClient.describeLead()).thenReturn(marketoFields);
        Mockito.when(mockMarketoRestClient.getLists()).thenReturn(mocklistRecords);
        List<String> fieldNameFromMarketoFields = MarketoUtils.getFieldNameFromMarketoFields(marketoFields);
        String fieldNameString = StringUtils.join(fieldNameFromMarketoFields, ",");
        Mockito.when(mockMarketoRestClient.getLeadsByList(Mockito.anyString(), eq(fieldNameString))).thenReturn(mockLeadEmptyRecordPagingIterable);
        Mockito.when(mockMarketoRestClient.getLeadsByList("1009", fieldNameString)).thenReturn(mockLeadRecordPagingIterable);

        LeadWithListInputPlugin.PluginTask task = configSource.loadConfig(LeadWithListInputPlugin.PluginTask.class);
        ServiceResponseMapper<? extends ValueLocator> mapper = leadWithListInputPlugin.buildServiceResponseMapper(task);

        RecordImporter recordImporter = mapper.createRecordImporter();
        PageBuilder mockPageBuilder = Mockito.mock(PageBuilder.class);
        leadWithListInputPlugin.ingestServiceData(task, recordImporter, 1, mockPageBuilder);
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).getLists();
        Mockito.verify(mockMarketoRestClient, Mockito.times(24)).getLeadsByList(Mockito.anyString(), eq(fieldNameString));
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).describeLead();

        Schema embulkSchema = mapper.getEmbulkSchema();
        ArgumentCaptor<Long> longArgumentCaptor = ArgumentCaptor.forClass(Long.class);

        Mockito.verify(mockPageBuilder, Mockito.times(300)).setLong(eq(embulkSchema.lookupColumn("mk_id")), longArgumentCaptor.capture());
        Mockito.verify(mockPageBuilder, Mockito.times(300)).setString(eq(embulkSchema.lookupColumn("mk_listId")), eq("1009"));

        List<Long> allValues = longArgumentCaptor.getAllValues();
        long actualValue = allValues.get(0);
        assertEquals(103280L, actualValue);
    }

    @Test
    public void testGetListsByIds()
    {
        ConfigSource cfgWithInputIds = configSource.set("list_ids", "123,12333").set("skip_invalid_list_id", true);
        LeadWithListInputPlugin.PluginTask task = cfgWithInputIds.loadConfig(LeadWithListInputPlugin.PluginTask.class);
        MarketoService service = Mockito.mock(MarketoService.class);
        Mockito.doReturn(Arrays.asList(new ObjectMapper().createObjectNode().put("id", "123"))).when(service).getListsByIds(anySet());

        leadWithListInputPlugin.getServiceRecords(service, task);

        Mockito.verify(service, Mockito.times(1)).getListsByIds(anySet());
        Mockito.verify(service, Mockito.never()).getLists();
        Mockito.verify(service, Mockito.times(1)).getAllListLead(anyList(), anyList());
    }

    @Test
    public void testGetAllLists()
    {
        LeadWithListInputPlugin.PluginTask task = configSource.loadConfig(LeadWithListInputPlugin.PluginTask.class);
        MarketoService service = Mockito.mock(MarketoService.class);
        leadWithListInputPlugin.getServiceRecords(service, task);

        Mockito.verify(service, Mockito.never()).getListsByIds(anySet());
        Mockito.verify(service, Mockito.times(1)).getLists();
        Mockito.verify(service, Mockito.times(1)).getAllListLead(anyList(), anyList());
    }
}
