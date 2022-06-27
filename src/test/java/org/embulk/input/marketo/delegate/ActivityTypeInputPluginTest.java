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

public class ActivityTypeInputPluginTest
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();

    private ConfigSource configSource;

    private ActivityTypeInputPlugin activityTypeInputPlugin;

    private MarketoRestClient mockMarketoRestClient;

    @Before
    public void setUp() throws Exception
    {
        activityTypeInputPlugin = Mockito.spy(new ActivityTypeInputPlugin());
        ConfigLoader configLoader = embulkTestRuntime.getInjector().getInstance(ConfigLoader.class);
        configSource = configLoader.fromYaml(this.getClass().getResourceAsStream("/config/rest_config.yaml"));
        mockMarketoRestClient = Mockito.mock(MarketoRestClient.class);
        Mockito.doReturn(mockMarketoRestClient).when(activityTypeInputPlugin).createMarketoRestClient(Mockito.any(ActivityTypeInputPlugin.PluginTask.class));
    }

    @Test
    public void testRun() throws IOException
    {
        RecordPagingIterable<ObjectNode> mockRecordPagingIterable = Mockito.mock(RecordPagingIterable.class);
        JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, ObjectNode.class);
        List<ObjectNode> objectNodeList = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/activity_type_response_full.json"), javaType);
        Mockito.when(mockRecordPagingIterable.iterator()).thenReturn(objectNodeList.iterator());
        Mockito.when(mockMarketoRestClient.getActivityTypes()).thenReturn(mockRecordPagingIterable);
        ActivityTypeInputPlugin.PluginTask task = CONFIG_MAPPER.map(configSource, ActivityTypeInputPlugin.PluginTask.class);
        ServiceResponseMapper<? extends ValueLocator> mapper = activityTypeInputPlugin.buildServiceResponseMapper(task);
        RecordImporter recordImporter = mapper.createRecordImporter();
        PageBuilder mockPageBuilder = Mockito.mock(PageBuilder.class);
        activityTypeInputPlugin.ingestServiceData(task, recordImporter, 1, mockPageBuilder);
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).getActivityTypes();
        Schema embulkSchema = mapper.getEmbulkSchema();
        ArgumentCaptor<Long> longArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(mockPageBuilder, Mockito.times(5)).setLong(Mockito.eq(embulkSchema.lookupColumn("id")), longArgumentCaptor.capture());
        List<Long> allValues = longArgumentCaptor.getAllValues();
        assertArrayEquals(new Long[]{1L, 2L, 3L, 4L, 5L}, allValues.toArray());
    }
}
