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
import java.util.List;

import static org.embulk.input.marketo.MarketoUtilsTest.CONFIG_MAPPER;
import static org.junit.Assert.assertArrayEquals;

public class ListInputPluginTest
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();

    private ConfigSource configSource;

    private ListInputPlugin listInputPlugin;

    private MarketoRestClient mockMarketoRestClient;

    @Before
    public void setUp() throws Exception
    {
        listInputPlugin = Mockito.spy(new ListInputPlugin());
        ConfigLoader configLoader = embulkTestRuntime.getInjector().getInstance(ConfigLoader.class);
        configSource = configLoader.fromYaml(this.getClass().getResourceAsStream("/config/rest_config.yaml"));
        mockMarketoRestClient = Mockito.mock(MarketoRestClient.class);
        Mockito.doReturn(mockMarketoRestClient).when(listInputPlugin).createMarketoRestClient(Mockito.any(ListInputPlugin.PluginTask.class));
    }

    @Test
    public void testRun() throws IOException
    {
        RecordPagingIterable<ObjectNode> mockRecordPagingIterable = Mockito.mock(RecordPagingIterable.class);
        JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, ObjectNode.class);
        List<ObjectNode> objectNodeList = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/list_reponse_full.json"), javaType);
        Mockito.when(mockRecordPagingIterable.iterator()).thenReturn(objectNodeList.iterator());
        Mockito.when(mockMarketoRestClient.getLists()).thenReturn(mockRecordPagingIterable);
        ListInputPlugin.PluginTask task = CONFIG_MAPPER.map(configSource, ListInputPlugin.PluginTask.class);
        ServiceResponseMapper<? extends ValueLocator> mapper = listInputPlugin.buildServiceResponseMapper(task);
        RecordImporter recordImporter = mapper.createRecordImporter();
        PageBuilder mockPageBuilder = Mockito.mock(PageBuilder.class);
        listInputPlugin.ingestServiceData(task, recordImporter, 1, mockPageBuilder);
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).getLists();
        Schema embulkSchema = mapper.getEmbulkSchema();
        ArgumentCaptor<Long> longArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(mockPageBuilder, Mockito.times(24)).setLong(Mockito.eq(embulkSchema.lookupColumn("id")), longArgumentCaptor.capture());
        List<Long> allValues = longArgumentCaptor.getAllValues();
        assertArrayEquals(new Long[]{1007L, 1009L, 1010L, 1011L, 1012L, 1052L, 1063L, 1066L, 1067L, 1072L, 1073L, 1075L, 1076L, 1077L, 1078L, 1079L, 1080L, 1081L, 1082L, 1083L, 1084L, 1085L, 1086L, 1087L}, allValues.toArray());
    }
}
