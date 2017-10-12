package org.embulk.input.marketo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.ByteStreams;
import org.apache.commons.lang3.StringUtils;
import org.embulk.EmbulkTestRuntime;
import org.embulk.input.marketo.model.BulkExtractRangeHeader;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.input.marketo.rest.RecordPagingIterable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

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
        mockMarketoRestClient = mock(MarketoRestClient.class);
        marketoService = new MarketoServiceImpl(mockMarketoRestClient);
    }

    @Test
    public void extractLead() throws Exception
    {
        Date startDate = new Date(1507223374000L);
        Date endDate = new Date(1507655374000L);
        List<String> extractedFields = Arrays.asList("field1", "field2");
        String filerField = "field1";
        String exportId = "exportId";
        when(mockMarketoRestClient.createLeadBulkExtract(eq(startDate), eq(endDate), eq(extractedFields), eq(filerField))).thenReturn(exportId);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream("Test File Content".getBytes());
        when(mockMarketoRestClient.getLeadBulkExtractResult(eq(exportId), any(BulkExtractRangeHeader.class))).thenReturn(byteArrayInputStream);
        File file = marketoService.extractLead(startDate, endDate, extractedFields, filerField, 1, 3);
        assertEquals("Test File Content", new String(ByteStreams.toByteArray(new FileInputStream(file))));
        verify(mockMarketoRestClient, times(1)).startLeadBulkExtract(eq(exportId));
        verify(mockMarketoRestClient, times(1)).waitLeadExportJobComplete(eq(exportId), eq(1), eq(3));
    }

    @Test
    public void extractAllActivity() throws Exception
    {
        Date startDate = new Date(1507223374000L);
        Date endDate = new Date(1507655374000L);
        String exportId = "exportId";
        when(mockMarketoRestClient.createActivityExtract(eq(startDate), eq(endDate))).thenReturn(exportId);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream("Test File Content".getBytes());
        when(mockMarketoRestClient.getActivitiesBulkExtractResult(eq(exportId), any(BulkExtractRangeHeader.class))).thenReturn(byteArrayInputStream);
        File file = marketoService.extractAllActivity(startDate, endDate, 1, 3);
        assertEquals("Test File Content", new String(ByteStreams.toByteArray(new FileInputStream(file))));
        verify(mockMarketoRestClient, times(1)).startActitvityBulkExtract(eq(exportId));
        verify(mockMarketoRestClient, times(1)).waitActitvityExportJobComplete(eq(exportId), eq(1), eq(3));
    }

    @Test
    public void getAllListLead() throws Exception
    {
        List<String> extractFields = Arrays.asList("field1", "field2");
        RecordPagingIterable<ObjectNode> listObjectNodes = mock(RecordPagingIterable.class);
        Iterator listIterator = mock(Iterator.class);
        when(listIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(listIterator.next()).thenReturn(OBJECT_MAPPER.readTree("{\"id\":1}")).thenReturn(OBJECT_MAPPER.readTree("{\"id\":2}"));
        when(listObjectNodes.iterator()).thenReturn(listIterator);
        List<ObjectNode> leadList1 = new ArrayList<>();
        leadList1.add((ObjectNode) OBJECT_MAPPER.readTree("{\"id\":\"lead1\"}"));
        List<ObjectNode> leadList2 = new ArrayList<>();
        leadList2.add((ObjectNode) OBJECT_MAPPER.readTree("{\"id\":\"lead2\"}"));
        when(mockMarketoRestClient.getLists()).thenReturn(listObjectNodes);
        RecordPagingIterable leadIterable1 = mock(RecordPagingIterable.class);
        RecordPagingIterable leadsIterable2 = mock(RecordPagingIterable.class);
        when(leadIterable1.iterator()).thenReturn(leadList1.iterator());
        when(leadsIterable2.iterator()).thenReturn(leadList2.iterator());
        when(mockMarketoRestClient.getLeadsByList(eq("1"), eq("field1,field2"))).thenReturn(leadIterable1);
        when(mockMarketoRestClient.getLeadsByList(eq("2"), eq("field1,field2"))).thenReturn(leadsIterable2);
        Iterable<ObjectNode> allListLead = marketoService.getAllListLead(extractFields);
        assertEquals(leadList1.get(0), allListLead.iterator().next());
        assertEquals(leadList2.get(0), allListLead.iterator().next());
    }

    @Test
    public void getAllProgramLead() throws Exception
    {
        RecordPagingIterable<ObjectNode> listObjectNodes = mock(RecordPagingIterable.class);
        Iterator listIterator = mock(Iterator.class);
        when(listIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(listIterator.next()).thenReturn(OBJECT_MAPPER.readTree("{\"id\":1}")).thenReturn(OBJECT_MAPPER.readTree("{\"id\":2}"));
        when(listObjectNodes.iterator()).thenReturn(listIterator);
        List<ObjectNode> leadList1 = new ArrayList<>();
        leadList1.add((ObjectNode) OBJECT_MAPPER.readTree("{\"id\":\"lead1\"}"));
        List<ObjectNode> leadList2 = new ArrayList<>();
        leadList2.add((ObjectNode) OBJECT_MAPPER.readTree("{\"id\":\"lead2\"}"));
        when(mockMarketoRestClient.getPrograms()).thenReturn(listObjectNodes);
        RecordPagingIterable leadIterable1 = mock(RecordPagingIterable.class);
        RecordPagingIterable leadsIterable2 = mock(RecordPagingIterable.class);
        when(leadIterable1.iterator()).thenReturn(leadList1.iterator());
        when(leadsIterable2.iterator()).thenReturn(leadList2.iterator());
        when(mockMarketoRestClient.getLeadsByProgram(eq("1"), eq("field1,field2"))).thenReturn(leadIterable1);
        when(mockMarketoRestClient.getLeadsByProgram(eq("2"), eq("field1,field2"))).thenReturn(leadsIterable2);
        Iterable<ObjectNode> allListLead = marketoService.getAllProgramLead(Arrays.asList("field1", "field2"));
        assertEquals(leadList1.get(0), allListLead.iterator().next());
        assertEquals(leadList2.get(0), allListLead.iterator().next());
    }

    @Test
    public void getCampaign() throws Exception
    {
        marketoService.getCampaign();
        verify(mockMarketoRestClient, times(1)).getCampaign();
    }

    @Test
    public void describeLead() throws Exception
    {
        marketoService.describeLead();
        verify(mockMarketoRestClient, times(1)).describeLead();
    }

    @Test
    public void describeLeadByProgram() throws Exception
    {
        List<MarketoField> marketoFields = new ArrayList<>();
        when(mockMarketoRestClient.describeLead()).thenReturn(marketoFields);
        marketoService.describeLeadByProgram();
        verify(mockMarketoRestClient, times(1)).describeLead();
        assertEquals(1, marketoFields.size());
        assertEquals("programId", marketoFields.get(0).getName());
        assertEquals(MarketoField.MarketoDataType.STRING, marketoFields.get(0).getMarketoDataType());
    }

    @Test
    public void describeLeadByLists() throws Exception
    {
        List<MarketoField> marketoFields = new ArrayList<>();
        when(mockMarketoRestClient.describeLead()).thenReturn(marketoFields);
        marketoService.describeLeadByLists();
        verify(mockMarketoRestClient, times(1)).describeLead();
        assertEquals(1, marketoFields.size());
        assertEquals("listId", marketoFields.get(0).getName());
        assertEquals(MarketoField.MarketoDataType.STRING, marketoFields.get(0).getMarketoDataType());
    }
}
