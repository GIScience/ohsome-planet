package org.heigit.ohsome.contributions.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Callable;

public abstract class AbstractIterator<T> implements Iterator<T> {

    public static <T> AbstractIterator<T> iterator(Callable<T> compute) {
        return new AbstractIterator<>() {
            @Override
            protected Optional<T> computeNext() throws Exception {
                var call = compute.call();
                if (call != null) {
                    return Optional.of(call);
                }
                return endOfData();
            }
        };
    }


    private T next;
    private Exception exception = null;

    public AbstractIterator() {
        this.next = null;
    }

    protected abstract Optional<T> computeNext() throws Exception;

    public static <T> T endOfData() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return (next != null) || ((next = tryToComputeNext()) != null);
    }

    public boolean hasException() {
        return exception != null;
    }

    public Exception exception() {
        return exception;
    }

    private T tryToComputeNext() {
        try {
            var tryNext = (Optional<T>) null;
            while ((tryNext = computeNext()) != null) {
                if (tryNext.isPresent()) {
                    return tryNext.get();
                }
            }
            return null;
        } catch (Exception e) {
            exception = e;
            throw new RuntimeException(e);
        }
    }

    public T peek() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return next;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException(exception);
        }
        T ret = next;
        next = null;
        return ret;
    }
}
