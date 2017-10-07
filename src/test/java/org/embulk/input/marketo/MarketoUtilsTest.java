package org.embulk.input.marketo;

import org.joda.time.DateTime;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by tai.khuu on 10/7/17.
 */
public class MarketoUtilsTest
{

    @Test
    public void sliceRange() throws Exception
    {
        DateTime startDate = new DateTime(1507369760000L);
        List<MarketoUtils.DateRange> dateRanges1 = MarketoUtils.sliceRange(startDate, startDate.plusDays(7), 2);
        assertEquals(4, dateRanges1.size());
        assertEquals(startDate.plusDays(7), dateRanges1.get(3).toDate);

        List<MarketoUtils.DateRange> dateRanges2 = MarketoUtils.sliceRange(startDate, startDate.plusDays(1), 2);
        assertEquals(1, dateRanges2.size());
        assertEquals(startDate.plusDays(1), dateRanges2.get(0).toDate);
    }

}