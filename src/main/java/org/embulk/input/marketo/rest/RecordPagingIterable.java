package org.embulk.input.marketo.rest;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Record Iterable class that will go through Marketo Paging
 * Warning this iterator implementation do not cached page due to reduce memory usage. So iterate through a
 * RecordIterate multiple time is not recommended since it will sent query to Marketo on every call.
 * Created by tai.khuu on 9/5/17.
 */
public class RecordPagingIterable<T> implements Iterable<T>
{
    private final PagingFunction<Page<T>> pagingFunction;

    public RecordPagingIterable(PagingFunction pagingFunction)
    {
        this.pagingFunction = pagingFunction;
    }

    @Override
    public Iterator<T> iterator()
    {
        return this.new RecordIterator();
    }

    private class RecordIterator implements Iterator<T>
    {
        Page<T> currentPage;
        private Iterator<T> currentIterator;

        public RecordIterator()
        {
        }

        @Override
        public boolean hasNext()
        {
            if (currentPage == null) {
                currentPage = pagingFunction.getFirstPage();
                this.currentIterator = currentPage.getRecordsIter();
            }
            if (currentIterator.hasNext()) {
                return true;
            }
            if (!currentPage.hasNext) {
                return false;
            }
            Page<T> nextPage = pagingFunction.getNextPage(currentPage);
            currentIterator = nextPage.getRecordsIter();
            currentPage = nextPage;
            return currentIterator.hasNext();
        }

        @Override
        public T next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException("Call next on an empty iterator");
            }
            return currentIterator.next();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("RecordIterator not support remove");
        }
    }

    public interface PagingFunction<P extends Page>
    {
        P getNextPage(P currentPage);
        /**
         * All implementation must make sure calling get first page multiple time should always return.
         * @return P
         */
        P getFirstPage();
    }

    public static class Page<T>
    {
        private Iterable<T> records;

        private boolean hasNext;

        public Page(Iterable<T> records, boolean hasNext)
        {
            this.records = records;
            this.hasNext = hasNext;
        }

        public Iterable<T> getRecords()
        {
            return records;
        }
        public Iterator<T> getRecordsIter()
        {
            return records.iterator();
        }
        public void setRecords(List<T> records)
        {
            this.records = records;
        }

        public boolean isHasNext()
        {
            return hasNext;
        }

        public void setHasNext(boolean hasNext)
        {
            this.hasNext = hasNext;
        }
    }

    public static class TokenPage<T> extends Page<T>
    {
        private String nextPageToken;

        public TokenPage(Iterable<T> records, String nextPageToken, boolean moreResult)
        {
            super(records, moreResult);
            this.nextPageToken = nextPageToken;
        }

        public String getNextPageToken()
        {
            return nextPageToken;
        }

        public void setNextPageToken(String nextPageToken)
        {
            this.nextPageToken = nextPageToken;
        }
    }

    public static class OffsetPage<T> extends Page<T>
    {
        private final int nextOffSet;

        public OffsetPage(Iterable<T> records, int nextOffSet, boolean moreResult)
        {
            super(records, moreResult);
            this.nextOffSet = nextOffSet;
        }

        public int getNextOffSet()
        {
            return nextOffSet;
        }
    }
    public static class OffsetWithTokenPage<T> extends Page<T>
    {
        private final int nextOffSet;
        private String nextPageToken;
        public OffsetWithTokenPage(Iterable<T> records, int nextOffSet, String nextPageToken, boolean moreResult)
        {
            super(records, moreResult);
            this.nextOffSet = nextOffSet;
            this.nextPageToken = nextPageToken;
        }

        public int getNextOffSet()
        {
            return nextOffSet;
        }

        public String getNextPageToken()
        {
            return nextPageToken;
        }

        public void setNextPageToken(String nextPageToken)
        {
            this.nextPageToken = nextPageToken;
        }
    }
}
