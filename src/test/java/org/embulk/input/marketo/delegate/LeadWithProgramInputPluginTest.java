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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

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
        leadWithProgramInputPlugin = Mockito.spy(new LeadWithProgramInputPlugin());
        ConfigLoader configLoader = embulkTestRuntime.getInjector().getInstance(ConfigLoader.class);
        configSource = configLoader.fromYaml(this.getClass().getResourceAsStream("/config/rest_config.yaml"));
        mockMarketoRestClient = Mockito.mock(MarketoRestClient.class);
        Mockito.doReturn(mockMarketoRestClient).when(leadWithProgramInputPlugin).createMarketoRestClient(any(LeadWithProgramInputPlugin.PluginTask.class));
    }

    @Test
    public void testRun() throws IOException
    {
        RecordPagingIterable<ObjectNode> mockLeadRecordPagingIterable = Mockito.mock(RecordPagingIterable.class);
        RecordPagingIterable<ObjectNode> mockLeadEmptyRecordPagingIterable = Mockito.mock(RecordPagingIterable.class);
        RecordPagingIterable<ObjectNode> mockProgramRecords = Mockito.mock(RecordPagingIterable.class);

        Mockito.when(mockLeadEmptyRecordPagingIterable.iterator()).thenReturn(new ArrayList<ObjectNode>().iterator());
        JavaType objectNodeListType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, ObjectNode.class);
        JavaType marketoFieldsType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<ObjectNode> leads = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/lead_with_program_full.json"), objectNodeListType);
        List<ObjectNode> programs = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/all_program_full.json"), objectNodeListType);
        Mockito.when(mockProgramRecords.iterator()).thenReturn(programs.iterator());
        List<MarketoField> marketoFields = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/lead_describe_marketo_fields_full.json"), marketoFieldsType);
        Mockito.when(mockLeadRecordPagingIterable.iterator()).thenReturn(leads.iterator());
        Mockito.when(mockMarketoRestClient.describeLead()).thenReturn(marketoFields);
        Mockito.when(mockMarketoRestClient.getPrograms()).thenReturn(mockProgramRecords);
        List<String> fieldNameFromMarketoFields = MarketoUtils.getFieldNameFromMarketoFields(marketoFields);
        String fieldNameString = StringUtils.join(fieldNameFromMarketoFields, ",");
        Mockito.when(mockMarketoRestClient.getLeadsByProgram(anyString(), eq(fieldNameString))).thenReturn(mockLeadEmptyRecordPagingIterable);
        Mockito.when(mockMarketoRestClient.getLeadsByProgram("1003", fieldNameString)).thenReturn(mockLeadRecordPagingIterable);

        LeadWithProgramInputPlugin.PluginTask task = configSource.loadConfig(LeadWithProgramInputPlugin.PluginTask.class);
        ServiceResponseMapper<? extends ValueLocator> mapper = leadWithProgramInputPlugin.buildServiceResponseMapper(task);

        RecordImporter recordImporter = mapper.createRecordImporter();
        PageBuilder mockPageBuilder = Mockito.mock(PageBuilder.class);

        leadWithProgramInputPlugin.ingestServiceData(task, recordImporter, 1, mockPageBuilder);
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).getPrograms();
        Mockito.verify(mockMarketoRestClient, Mockito.times(3)).getLeadsByProgram(anyString(), eq(fieldNameString));
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).describeLead();

        Schema embulkSchema = mapper.getEmbulkSchema();
        ArgumentCaptor<Long> longArgumentCaptor = ArgumentCaptor.forClass(Long.class);

        Mockito.verify(mockPageBuilder, Mockito.times(1)).setLong(eq(embulkSchema.lookupColumn("mk_id")), longArgumentCaptor.capture());
        Mockito.verify(mockPageBuilder, Mockito.times(1)).setString(eq(embulkSchema.lookupColumn("mk_programId")), eq("1003"));

        List<Long> allValues = longArgumentCaptor.getAllValues();
        long actualValue = allValues.get(0);
        assertEquals(102519L, actualValue);
    }

    @Test
    public void testGetProgramsByIds()
    {
        ConfigSource cfgWithInputIds = configSource.set("program_ids", "123,12333").set("skip_invalid_program_id", true);
        LeadWithProgramInputPlugin.PluginTask task = cfgWithInputIds.loadConfig(LeadWithProgramInputPlugin.PluginTask.class);
        MarketoService service = Mockito.mock(MarketoService.class);
        Mockito.doReturn(Arrays.asList(new ObjectMapper().createObjectNode().put("id", "123"))).when(service).getProgramsByIds(anySet());

        leadWithProgramInputPlugin.getServiceRecords(service, task);

        Mockito.verify(service, Mockito.times(1)).getProgramsByIds(anySet());
        Mockito.verify(service, Mockito.never()).getPrograms();
        Mockito.verify(service, Mockito.times(1)).getAllProgramLead(anyList(), anyList());
    }

    @Test
    public void testGetAllLists()
    {
        LeadWithProgramInputPlugin.PluginTask task = configSource.loadConfig(LeadWithProgramInputPlugin.PluginTask.class);
        MarketoService service = Mockito.mock(MarketoService.class);
        leadWithProgramInputPlugin.getServiceRecords(service, task);

        Mockito.verify(service, Mockito.never()).getProgramsByIds(anySet());
        Mockito.verify(service, Mockito.times(1)).getPrograms();
        Mockito.verify(service, Mockito.times(1)).getAllProgramLead(anyList(), anyList());
    }
}
