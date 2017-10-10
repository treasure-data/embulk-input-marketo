package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.EmbulkTestRuntime;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

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
        leadWithListInputPlugin = spy(new LeadWithListInputPlugin());
        ConfigLoader configLoader = embulkTestRuntime.getInjector().getInstance(ConfigLoader.class);
        configSource = configLoader.fromYaml(this.getClass().getResourceAsStream("/config/rest_config.yaml"));
        mockMarketoRestClient = mock(MarketoRestClient.class);
        doReturn(mockMarketoRestClient).when(leadWithListInputPlugin).createMarketoRestClient(any(LeadWithListInputPlugin.PluginTask.class));
    }

    @Test
    public void testRun() throws IOException
    {
        RecordPagingIterable<ObjectNode> mockLeadRecordPagingIterable = mock(RecordPagingIterable.class);
        RecordPagingIterable<ObjectNode> mockLeadEmptyRecordPagingIterable = mock(RecordPagingIterable.class);
        RecordPagingIterable<ObjectNode> mocklistRecords = mock(RecordPagingIterable.class);

        when(mockLeadEmptyRecordPagingIterable.iterator()).thenReturn(new ArrayList<ObjectNode>().iterator());
        JavaType objectNodeListType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, ObjectNode.class);
        JavaType marketoFieldsType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<ObjectNode> leads = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/lead_response_full.json"), objectNodeListType);
        List<ObjectNode> lists = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/list_reponse_full.json"), objectNodeListType);
        when(mocklistRecords.iterator()).thenReturn(lists.iterator());
        List<MarketoField> marketoFields = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/lead_describe_marketo_fields_full.json"), marketoFieldsType);
        when(mockLeadRecordPagingIterable.iterator()).thenReturn(leads.iterator());
        when(mockMarketoRestClient.describeLead()).thenReturn(marketoFields);
        when(mockMarketoRestClient.getLists()).thenReturn(mocklistRecords);
        List<String> fieldNameFromMarketoFields = MarketoUtils.getFieldNameFromMarketoFields(marketoFields);
        when(mockMarketoRestClient.getLeadsByList(anyString(), eq(fieldNameFromMarketoFields))).thenReturn(mockLeadEmptyRecordPagingIterable);
        when(mockMarketoRestClient.getLeadsByList("1009", fieldNameFromMarketoFields)).thenReturn(mockLeadRecordPagingIterable);

        LeadWithListInputPlugin.PluginTask task = configSource.loadConfig(LeadWithListInputPlugin.PluginTask.class);
        ServiceResponseMapper<? extends ValueLocator> mapper = leadWithListInputPlugin.buildServiceResponseMapper(task);

        RecordImporter recordImporter = mapper.createRecordImporter();
        PageBuilder mockPageBuilder = mock(PageBuilder.class);
        leadWithListInputPlugin.ingestServiceData(task, recordImporter, 1, mockPageBuilder);
        verify(mockMarketoRestClient, times(1)).getLists();
        verify(mockMarketoRestClient, times(24)).getLeadsByList(anyString(), eq(fieldNameFromMarketoFields));
        verify(mockMarketoRestClient, times(1)).describeLead();

        Schema embulkSchema = mapper.getEmbulkSchema();
        ArgumentCaptor<Long> longArgumentCaptor = ArgumentCaptor.forClass(Long.class);

        verify(mockPageBuilder, times(300)).setLong(eq(embulkSchema.lookupColumn("mk_id")), longArgumentCaptor.capture());
        verify(mockPageBuilder, times(300)).setString(eq(embulkSchema.lookupColumn("mk_listId")), eq("1009"));

        List<Long> allValues = longArgumentCaptor.getAllValues();
        long actualValue = allValues.get(0);
        assertEquals(103280L, actualValue);
    }
}
