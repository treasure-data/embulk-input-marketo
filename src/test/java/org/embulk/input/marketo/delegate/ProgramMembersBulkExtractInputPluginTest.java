package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.EmbulkTestRuntime;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.input.marketo.model.BulkExtractRangeHeader;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.input.marketo.rest.RecordPagingIterable;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.embulk.input.marketo.MarketoUtilsTest.CONFIG_MAPPER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProgramMembersBulkExtractInputPluginTest
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();

    private ProgramMembersBulkExtractInputPlugin bulkExtractInputPlugin;

    private ConfigSource configSource;

    private MarketoRestClient mockMarketoRestclient;

    @Before
    public void prepare() throws IOException
    {
        bulkExtractInputPlugin = spy(new ProgramMembersBulkExtractInputPlugin());
        ConfigLoader configLoader = embulkTestRuntime.getInjector().getInstance(ConfigLoader.class);
        configSource = configLoader.fromYaml(this.getClass().getResourceAsStream("/config/rest_config.yaml"));
        mockMarketoRestclient = mock(MarketoRestClient.class);
        doReturn(mockMarketoRestclient).when(bulkExtractInputPlugin).createMarketoRestClient(any(ProgramMembersBulkExtractInputPlugin.PluginTask.class));
    }

    @Test
    public void testRun() throws InterruptedException, IOException
    {
        RecordPagingIterable<ObjectNode> mockProgramRecords = mock(RecordPagingIterable.class);
        ProgramMembersBulkExtractInputPlugin.PluginTask task = CONFIG_MAPPER.map(configSource, ProgramMembersBulkExtractInputPlugin.PluginTask.class);
        PageBuilder pageBuilder = mock(PageBuilder.class);
        String exportId1 = "exportId1";
        List<ObjectNode> programs = new ArrayList<>();
        programs.add(OBJECT_MAPPER.createObjectNode().put("id", 100));
        ObjectNode marketoFields = (ObjectNode) OBJECT_MAPPER.readTree(this.getClass().getResourceAsStream("/fixtures/program_members_describe.json"));
        when(mockProgramRecords.iterator()).thenReturn(programs.iterator());
        ObjectNode objectNode = OBJECT_MAPPER.createObjectNode().put("numberOfRecords", 3);
        when(mockMarketoRestclient.waitProgramMembersExportJobComplete(anyString(), anyInt(), anyInt())).thenReturn(objectNode);
        when(mockMarketoRestclient.describeProgramMembers()).thenReturn(marketoFields);
        when(mockMarketoRestclient.createProgramMembersBulkExtract(any(List.class), any(Integer.class))).thenReturn(exportId1).thenReturn(null);
        when(mockMarketoRestclient.getProgramMemberBulkExtractResult(eq(exportId1), any(BulkExtractRangeHeader.class))).thenReturn(this.getClass().getResourceAsStream("/fixtures/program_members_extract.csv"));
        when(mockMarketoRestclient.getPrograms()).thenReturn(mockProgramRecords);
        bulkExtractInputPlugin.validateInputTask(task);
        ServiceResponseMapper<? extends ValueLocator> mapper = bulkExtractInputPlugin.buildServiceResponseMapper(task);
        bulkExtractInputPlugin.ingestServiceData(task, mapper.createRecordImporter(), 1, pageBuilder);
        ArgumentCaptor<Long> argumentCaptor = ArgumentCaptor.forClass(Long.class);
        Column idColumn = mapper.getEmbulkSchema().lookupColumn("mk_leadId");
        verify(pageBuilder, times(3)).setLong(eq(idColumn), argumentCaptor.capture());
        verify(mockMarketoRestclient, times(1)).startProgramMembersBulkExtract(eq(exportId1));
        verify(mockMarketoRestclient, times(1)).waitProgramMembersExportJobComplete(eq(exportId1), eq(task.getPollingIntervalSecond()), eq(task.getBulkJobTimeoutSecond()));
        verify(mockMarketoRestclient, times(1)).createProgramMembersBulkExtract(anyList(), anyInt());
        List<Long> leadIds = argumentCaptor.getAllValues();
        Assert.assertEquals(3, leadIds.size());
        Assert.assertTrue(leadIds.contains(452L));
        Assert.assertTrue(leadIds.contains(453L));
        Assert.assertTrue(leadIds.contains(454L));
    }
}
