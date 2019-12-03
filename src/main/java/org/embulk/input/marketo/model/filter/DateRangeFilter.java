package org.embulk.input.marketo.model.filter;

/**
 * Created by tai.khuu on 8/27/17.
 */
public class DateRangeFilter
{
    private String startAt;

    private String endAt;

    public String getStartAt()
    {
        return startAt;
    }

    public void setStartAt(String startAt)
    {
        this.startAt = startAt;
    }

    public String getEndAt()
    {
        return endAt;
    }

    public void setEndAt(String endAt)
    {
        this.endAt = endAt;
    }

    @Override
    public String toString()
    {
        return "DateRangeFilter{" +
                "startAt='" + startAt + '\'' +
                ", endAt='" + endAt + '\'' +
                '}';
    }
}
