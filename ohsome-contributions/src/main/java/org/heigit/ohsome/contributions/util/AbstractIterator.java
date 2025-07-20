package org.heigit.ohsome.contributions.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class AbstractIterator<T> implements Iterator<T> {

  private T next;
  private Exception exception = null;

  protected AbstractIterator() {
    this.next = null;
  }

  protected abstract T computeNext() throws Exception;

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
      return computeNext();
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
