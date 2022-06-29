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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.embulk.input.marketo.MarketoUtilsTest.CONFIG_MAPPER;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by tai.khuu on 10/10/17.
 */
public class LeadWithProgramInputPluginTest
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();

    private ConfigSource configSource;

    private LeadWithProgramInputPlugin leadWithProgramInputPlugin;

    private MarketoRestClient mockMarketoRestClient;

    @Before
    public void setUp() throws Exception
    {
        leadWithProgramInputPlugin = spy(new LeadWithProgramInputPlugin());
        ConfigLoader configLoader = embulkTestRuntime.getInjector().getInstance(ConfigLoader.class);
        configSource = configLoader.fromYaml(this.getClass().getResourceAsStream("/config/rest_config.yaml"));
        mockMarketoRestClient = mock(MarketoRestClient.class);
        doReturn(mockMarketoRestClient).when(leadWithProgramInputPlugin).createMarketoRestClient(any(LeadWithProgramInputPlugin.PluginTask.class));
    }

    @Test
    public void testRun() throws IOException
    {
        RecordPagingIterable<ObjectNode> mockLeadRecordPagingIterable = mock(RecordPagingIterable.class);
        RecordPagingIterable<ObjectNode> mockLeadEmptyRecordPagingIterable = mock(RecordPagingIterable.class);
        RecordPagingIterable<ObjectNode> mockProgramRecords = mock(RecordPagingIterable.class);

        when(mockLeadEmptyRecordPagingIterable.iterator()).thenReturn(Collections.emptyIterator());
        JavaType objectNodeListType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, ObjectNode.class);
        JavaType marketoFieldsType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<ObjectNode> leads = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/lead_with_program_full.json"), objectNodeListType);
        List<ObjectNode> programs = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/all_program_full.json"), objectNodeListType);
        when(mockProgramRecords.iterator()).thenReturn(programs.iterator());
        List<MarketoField> marketoFields = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/lead_describe_marketo_fields_full.json"), marketoFieldsType);
        when(mockLeadRecordPagingIterable.iterator()).thenReturn(leads.iterator());
        when(mockMarketoRestClient.describeLead()).thenReturn(marketoFields);
        when(mockMarketoRestClient.getPrograms()).thenReturn(mockProgramRecords);
        List<String> fieldNameFromMarketoFields = MarketoUtils.getFieldNameFromMarketoFields(marketoFields);
        String fieldNameString = StringUtils.join(fieldNameFromMarketoFields, ",");
        when(mockMarketoRestClient.getLeadsByProgram(anyString(), eq(fieldNameString))).thenReturn(mockLeadEmptyRecordPagingIterable);
        when(mockMarketoRestClient.getLeadsByProgram("1003", fieldNameString)).thenReturn(mockLeadRecordPagingIterable);

        LeadWithProgramInputPlugin.PluginTask task = CONFIG_MAPPER.map(configSource, LeadWithProgramInputPlugin.PluginTask.class);
        ServiceResponseMapper<? extends ValueLocator> mapper = leadWithProgramInputPlugin.buildServiceResponseMapper(task);

        RecordImporter recordImporter = mapper.createRecordImporter();
        PageBuilder mockPageBuilder = mock(PageBuilder.class);

        leadWithProgramInputPlugin.ingestServiceData(task, recordImporter, 1, mockPageBuilder);
        verify(mockMarketoRestClient, times(1)).getPrograms();
        verify(mockMarketoRestClient, times(3)).getLeadsByProgram(anyString(), eq(fieldNameString));
        verify(mockMarketoRestClient, times(1)).describeLead();

        Schema embulkSchema = mapper.getEmbulkSchema();
        ArgumentCaptor<Long> longArgumentCaptor = ArgumentCaptor.forClass(Long.class);

        verify(mockPageBuilder, times(1)).setLong(eq(embulkSchema.lookupColumn("mk_id")), longArgumentCaptor.capture());
        verify(mockPageBuilder, times(1)).setString(eq(embulkSchema.lookupColumn("mk_programId")), eq("1003"));

        List<Long> allValues = longArgumentCaptor.getAllValues();
        long actualValue = allValues.get(0);
        assertEquals(102519L, actualValue);
    }

    @Test
    public void testGetProgramsByIds()
    {
        ConfigSource cfgWithInputIds = configSource.set("program_ids", "123,12333").set("skip_invalid_program_id", true);
        LeadWithProgramInputPlugin.PluginTask task = CONFIG_MAPPER.map(cfgWithInputIds, LeadWithProgramInputPlugin.PluginTask.class);
        MarketoService service = mock(MarketoService.class);
        doReturn(Arrays.asList(new ObjectMapper().createObjectNode().put("id", "123"))).when(service).getProgramsByIds(anySet());

        leadWithProgramInputPlugin.getServiceRecords(service, task);

        verify(service, times(1)).getProgramsByIds(anySet());
        verify(service, never()).getPrograms();
        verify(service, times(1)).getAllProgramLead(anyList(), anyList());
    }

    @Test
    public void testGetAllLists()
    {
        LeadWithProgramInputPlugin.PluginTask task = CONFIG_MAPPER.map(configSource, LeadWithProgramInputPlugin.PluginTask.class);
        MarketoService service = mock(MarketoService.class);
        leadWithProgramInputPlugin.getServiceRecords(service, task);

        verify(service, never()).getProgramsByIds(anySet());
        verify(service, times(1)).getPrograms();
        verify(service, times(1)).getAllProgramLead(anyList(), anyList());
    }
}
