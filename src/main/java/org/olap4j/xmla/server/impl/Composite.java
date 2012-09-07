/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho
// All Rights Reserved.
*/
package org.olap4j.xmla.server.impl;

import java.util.*;

/**
 * Composite collections.
 *
 * @author jhyde
 */
public abstract class Composite {

    /**
     * Creates a composite list, inferring the element type from the arguments.
     *
     * @param lists One or more lists
     * @param <T> element type
     * @return composite list
     */
    public static <T> List<T> of(
        List<? extends T>... lists)
    {
        return CompositeList.of(lists);
    }

    /**
     * Creates a composite iterable, inferring the element type from the
     * arguments.
     *
     * @param iterables One or more iterables
     * @param <T> element type
     * @return composite iterable
     */
    public static <T> Iterable<T> of(
        Iterable<? extends T>... iterables)
    {
        return new CompositeIterable<T>(iterables);
    }

    /**
     * Creates a composite list, inferring the element type from the arguments.
     *
     * @param iterators One or more iterators
     * @param <T> element type
     * @return composite list
     */
    public static <T> Iterator<T> of(
        Iterator<? extends T>... iterators)
    {
        //noinspection unchecked
        return new CompositeIterator<T>((Iterator[]) iterators);
    }

    private static class CompositeIterable<T> implements Iterable<T> {
        private final Iterable[] iterables;

        private CompositeIterable(Iterable[] iterables) {
            this.iterables = iterables;
        }

        public Iterator<T> iterator() {
            //noinspection unchecked
            return new CompositeIterator<T>(iterables);
        }
    }

    private static class CompositeIterator<T> implements Iterator<T> {
        private final Iterator<Iterator<T>> iteratorIterator;
        private boolean hasNext;
        private T next;
        private Iterator<T> iterator;

        public CompositeIterator(Iterator<T>[] iterables) {
            this.iteratorIterator = Arrays.asList(iterables).iterator();
            this.iterator = EmptyIterator.instance();
            this.hasNext = true;
            advance();
        }

        public CompositeIterator(Iterable<T>[] iterables) {
            this.iteratorIterator = new IterableIterator<T>(iterables);
            this.iterator = EmptyIterator.instance();
            this.hasNext = true;
            advance();
        }

        private void advance() {
            for (;;) {
                if (iterator.hasNext()) {
                    next = iterator.next();
                    return;
                }
                if (!iteratorIterator.hasNext()) {
                    hasNext = false;
                    break;
                }
                iterator = iteratorIterator.next();
            }
        }

        public boolean hasNext() {
            return hasNext;
        }

        public T next() {
            final T next1 = next;
            advance();
            return next1;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class IterableIterator<T>
        implements Iterator<Iterator<T>>
    {
        private int i;
        private final Iterable<T>[] iterables;

        public IterableIterator(Iterable<T>[] iterables) {
            this.iterables = iterables;
            i = 0;
        }

        public boolean hasNext() {
            return i < iterables.length;
        }

        public Iterator<T> next() {
            return iterables[i++].iterator();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class EmptyIterator implements Iterator {
        private static final Iterator INSTANCE = new EmptyIterator();

        private static <T> Iterator<T> instance() {
            //noinspection unchecked
            return INSTANCE;
        }

        public boolean hasNext() {
            return false;
        }

        public Object next() {
            throw new NoSuchElementException();
        }

        public void remove() {
            throw new IllegalStateException();
        }
    }
}

// End Composite.java
