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
import org.embulk.input.marketo.delegate.FolderInputPlugin.PluginTask;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.input.marketo.rest.RecordPagingIterable;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.embulk.input.marketo.MarketoUtilsTest.CONFIG_MAPPER;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FolderInputPluginTest
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ConfigSource configSource;

    private FolderInputPlugin mockPlugin;

    private MarketoRestClient mockRestClient;

    @Before
    public void setUp() throws Exception
    {
        mockPlugin = spy(new FolderInputPlugin());
        ConfigLoader configLoader = runtime.getInjector().getInstance(ConfigLoader.class);
        configSource = configLoader.fromYaml(this.getClass().getResourceAsStream("/config/rest_config.yaml"));
        mockRestClient = mock(MarketoRestClient.class);
        doReturn(mockRestClient).when(mockPlugin).createMarketoRestClient(any(PluginTask.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRun() throws IOException
    {
        RecordPagingIterable<ObjectNode> mockRecordPagingIterable = mock(RecordPagingIterable.class);
        JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, ObjectNode.class);
        List<ObjectNode> objectNodeList = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/folder_response_full.json"), javaType);
        when(mockRecordPagingIterable.spliterator()).thenReturn(objectNodeList.spliterator());
        when(mockRestClient.getFolders(Optional.empty(), 2, Optional.empty())).thenReturn(mockRecordPagingIterable);

        PluginTask task = CONFIG_MAPPER.map(configSource, PluginTask.class);
        ServiceResponseMapper<? extends ValueLocator> mapper = mockPlugin.buildServiceResponseMapper(task);
        RecordImporter recordImporter = mapper.createRecordImporter();
        PageBuilder mockPageBuilder = mock(PageBuilder.class);
        mockPlugin.ingestServiceData(task, recordImporter, 1, mockPageBuilder);

        verify(mockRestClient, times(1)).getFolders(Optional.empty(), 2, Optional.empty());
        Schema embulkSchema = mapper.getEmbulkSchema();
        assertEquals(embulkSchema.size(), 14);
        ArgumentCaptor<Long> longArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        verify(mockPageBuilder, times(3)).setLong(eq(embulkSchema.lookupColumn("id")), longArgumentCaptor.capture());
        List<Long> allValues = longArgumentCaptor.getAllValues();
        assertArrayEquals(new Long[]{1001L, 1002L, 2001L}, allValues.toArray());
    }
}
