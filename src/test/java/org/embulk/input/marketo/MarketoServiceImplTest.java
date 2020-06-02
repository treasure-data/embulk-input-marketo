package org.embulk.input.marketo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.ByteStreams;
import org.embulk.EmbulkTestRuntime;
import org.embulk.input.marketo.model.BulkExtractRangeHeader;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.input.marketo.rest.RecordPagingIterable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;

/**
 * Created by tai.khuu on 10/9/17.
 */
public class MarketoServiceImplTest
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();
    private MarketoService marketoService;

    private MarketoRestClient mockMarketoRestClient;
    @Before
    public void prepare()
    {
        mockMarketoRestClient = Mockito.mock(MarketoRestClient.class);
        marketoService = new MarketoServiceImpl(mockMarketoRestClient);
    }

    @Test
    public void extractLead() throws Exception
    {
        Date startDate = new Date(1507223374000L);
        Date endDate = new Date(1507655374000L);
        List<String> extractedFields = Arrays.asList("field1", "fActivityBulkExtractInputPluginTest.java:78ield2");
        String filerField = "field1";
        String exportId = "exportId";
        Mockito.when(mockMarketoRestClient.createLeadBulkExtract(Mockito.eq(startDate), Mockito.eq(endDate), Mockito.eq(extractedFields), Mockito.eq(filerField))).thenReturn(exportId);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream("Test File Content".getBytes());
        Mockito.when(mockMarketoRestClient.getLeadBulkExtractResult(Mockito.eq(exportId), any(BulkExtractRangeHeader.class))).thenReturn(byteArrayInputStream);
        File file = marketoService.extractLead(startDate, endDate, extractedFields, filerField, 1, 3);
        Assert.assertEquals("Test File Content", new String(ByteStreams.toByteArray(new FileInputStream(file))));
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).startLeadBulkExtract(Mockito.eq(exportId));
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).waitLeadExportJobComplete(Mockito.eq(exportId), Mockito.eq(1), Mockito.eq(3));
    }

    @Test
    public void extractAllActivity() throws Exception
    {
        Date startDate = new Date(1507223374000L);
        Date endDate = new Date(1507655374000L);
        List<Integer> activityTypeIds = new ArrayList<>();
        String exportId = "exportId";
        Mockito.when(mockMarketoRestClient.createActivityExtract(any(List.class), Mockito.eq(startDate), Mockito.eq(endDate))).thenReturn(exportId);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream("Test File Content".getBytes());
        Mockito.when(mockMarketoRestClient.getActivitiesBulkExtractResult(Mockito.eq(exportId), any(BulkExtractRangeHeader.class))).thenReturn(byteArrayInputStream);
        File file = marketoService.extractAllActivity(activityTypeIds, startDate, endDate, 1, 3);
        Assert.assertEquals("Test File Content", new String(ByteStreams.toByteArray(new FileInputStream(file))));
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).startActitvityBulkExtract(Mockito.eq(exportId));
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).waitActitvityExportJobComplete(Mockito.eq(exportId), Mockito.eq(1), Mockito.eq(3));
    }

    @Test
    public void getAllListLead() throws Exception
    {
        List<String> extractFields = Arrays.asList("field1", "field2");
        List<ObjectNode> allLists = mockParent();

        List<ObjectNode> lead1 = new ArrayList<>();
        lead1.add((ObjectNode) OBJECT_MAPPER.readTree("{\"id\":\"lead1\"}"));
        List<ObjectNode> lead2 = new ArrayList<>();
        lead2.add((ObjectNode) OBJECT_MAPPER.readTree("{\"id\":\"lead2\"}"));

        RecordPagingIterable leadIterable1 = Mockito.mock(RecordPagingIterable.class);
        RecordPagingIterable leadsIterable2 = Mockito.mock(RecordPagingIterable.class);
        Mockito.when(leadIterable1.iterator()).thenReturn(lead1.iterator());
        Mockito.when(leadsIterable2.iterator()).thenReturn(lead2.iterator());

        Mockito.when(mockMarketoRestClient.getLeadsByList(Mockito.eq("1"), Mockito.eq("field1,field2"))).thenReturn(leadIterable1);
        Mockito.when(mockMarketoRestClient.getLeadsByList(Mockito.eq("2"), Mockito.eq("field1,field2"))).thenReturn(leadsIterable2);

        Iterable<ObjectNode> allListLead = marketoService.getAllListLead(extractFields, allLists);
        Assert.assertEquals(lead1.get(0), allListLead.iterator().next());
        Assert.assertEquals(lead2.get(0), allListLead.iterator().next());
    }

    private List<ObjectNode> mockParent() throws IOException
    {
        List<ObjectNode> allLists = new ArrayList<>();
        allLists.add((ObjectNode) OBJECT_MAPPER.readTree("{\"id\": 1}"));
        allLists.add((ObjectNode) OBJECT_MAPPER.readTree("{\"id\": 2}"));
        return allLists;
    }

    @Test
    public void getAllProgramLead() throws Exception
    {
        RecordPagingIterable<ObjectNode> listObjectNodes = Mockito.mock(RecordPagingIterable.class);
        Iterable listIterator = mockParent();

        List<ObjectNode> leadList1 = new ArrayList<>();
        leadList1.add((ObjectNode) OBJECT_MAPPER.readTree("{\"id\":\"lead1\"}"));
        List<ObjectNode> leadList2 = new ArrayList<>();
        leadList2.add((ObjectNode) OBJECT_MAPPER.readTree("{\"id\":\"lead2\"}"));
        Mockito.when(mockMarketoRestClient.getPrograms()).thenReturn(listObjectNodes);
        RecordPagingIterable leadIterable1 = Mockito.mock(RecordPagingIterable.class);
        RecordPagingIterable leadsIterable2 = Mockito.mock(RecordPagingIterable.class);
        Mockito.when(leadIterable1.iterator()).thenReturn(leadList1.iterator());
        Mockito.when(leadsIterable2.iterator()).thenReturn(leadList2.iterator());

        Mockito.when(mockMarketoRestClient.getLeadsByProgram(Mockito.eq("1"), Mockito.eq("field1,field2"))).thenReturn(leadIterable1);
        Mockito.when(mockMarketoRestClient.getLeadsByProgram(Mockito.eq("2"), Mockito.eq("field1,field2"))).thenReturn(leadsIterable2);

        Iterable<ObjectNode> allListLead = marketoService.getAllProgramLead(Arrays.asList("field1", "field2"), listIterator);
        Assert.assertEquals(leadList1.get(0), allListLead.iterator().next());
        Assert.assertEquals(leadList2.get(0), allListLead.iterator().next());
    }

    @Test
    public void getCampaign() throws Exception
    {
        marketoService.getCampaign();
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).getCampaign();
    }

    @Test
    public void describeLead() throws Exception
    {
        marketoService.describeLead();
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).describeLead();
    }
}
