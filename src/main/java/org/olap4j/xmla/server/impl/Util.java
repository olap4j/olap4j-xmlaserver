/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
package org.olap4j.xmla.server.impl;

import org.apache.commons.collections.Predicate;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Cut-down version of mondrian.olap.Util.
 */
public class Util {

    public static final String nl = System.getProperty("line.separator");

    /**
     * When the compiler is complaining that you are not using a variable, just
     * call one of these routines with it.
     **/
    public static void discard(boolean b) {
    }

    public static void discard(byte b) {
    }

    public static void discard(char c) {
    }

    public static void discard(double d) {
    }

    public static void discard(float d) {
    }

    public static void discard(int i) {
    }

    public static void discard(long l) {
    }

    public static void discard(Object o) {
    }

    public static void discard(short s) {
    }

    /**
     * Appends a double-quoted string to a string builder.
     */
    public static StringBuilder quoteForMdx(StringBuilder buf, String val) {
        buf.append("\"");
        String s0 = replace(val, "\"", "\"\"");
        buf.append(s0);
        buf.append("\"");
        return buf;
    }

    /**
     * Return string quoted in [...].  For example, "San Francisco" becomes
     * "[San Francisco]"; "a [bracketed] string" becomes
     * "[a [bracketed]] string]".
     */
    public static String quoteMdxIdentifier(String id) {
        StringBuilder buf = new StringBuilder(id.length() + 20);
        quoteMdxIdentifier(id, buf);
        return buf.toString();
    }

    public static void quoteMdxIdentifier(String id, StringBuilder buf) {
        buf.append('[');
        int start = buf.length();
        buf.append(id);
        replace(buf, start, "]", "]]");
        buf.append(']');
    }

    /**
     * Returns a string with every occurrence of a seek string replaced with
     * another.
     */
    public static String replace(String s, String find, String replace) {
        // let's be optimistic
        int found = s.indexOf(find);
        if (found == -1) {
            return s;
        }
        StringBuilder sb = new StringBuilder(s.length() + 20);
        int start = 0;
        char[] chars = s.toCharArray();
        final int step = find.length();
        if (step == 0) {
            // Special case where find is "".
            sb.append(s);
            replace(sb, 0, find, replace);
        } else {
            for (;;) {
                sb.append(chars, start, found - start);
                if (found == s.length()) {
                    break;
                }
                sb.append(replace);
                start = found + step;
                found = s.indexOf(find, start);
                if (found == -1) {
                    found = s.length();
                }
            }
        }
        return sb.toString();
    }

    /**
     * Replaces all occurrences of a string in a buffer with another.
     *
     * @param buf String buffer to act on
     * @param start Ordinal within <code>find</code> to start searching
     * @param find String to find
     * @param replace String to replace it with
     * @return The string buffer
     */
    public static StringBuilder replace(
        StringBuilder buf,
        int start,
        String find,
        String replace)
    {
        // Search and replace from the end towards the start, to avoid O(n ^ 2)
        // copying if the string occurs very commonly.
        int findLength = find.length();
        if (findLength == 0) {
            // Special case where the seek string is empty.
            for (int j = buf.length(); j >= 0; --j) {
                buf.insert(j, replace);
            }
            return buf;
        }
        int k = buf.length();
        while (k > 0) {
            int i = buf.lastIndexOf(find, k);
            if (i < start) {
                break;
            }
            buf.replace(i, i + find.length(), replace);
            // Step back far enough to ensure that the beginning of the section
            // we just replaced does not cause a match.
            k = i - findLength;
        }
        return buf;
    }

    /**
     * Converts a list of SQL-style patterns into a Java regular expression.
     *
     * <p>For example, {"Foo_", "Bar%BAZ"} becomes "Foo.|Bar.*BAZ".
     *
     * @param wildcards List of SQL-style wildcard expressions
     * @return Regular expression
     */
    public static String wildcardToRegexp(List<String> wildcards) {
        StringBuilder buf = new StringBuilder();
        for (String value : wildcards) {
            if (buf.length() > 0) {
                buf.append('|');
            }
            int i = 0;
            while (true) {
                int percent = value.indexOf('%', i);
                int underscore = value.indexOf('_', i);
                if (percent == -1 && underscore == -1) {
                    if (i < value.length()) {
                        buf.append(quotePattern(value.substring(i)));
                    }
                    break;
                }
                if (underscore >= 0 && (underscore < percent || percent < 0)) {
                    if (i < underscore) {
                        buf.append(
                            quotePattern(value.substring(i, underscore)));
                    }
                    buf.append('.');
                    i = underscore + 1;
                } else if (percent >= 0
                    && (percent < underscore || underscore < 0))
                {
                    if (i < percent) {
                    buf.append(
                        quotePattern(value.substring(i, percent)));
                    }
                    buf.append(".*");
                    i = percent + 1;
                } else {
                    throw new IllegalArgumentException();
                }
            }
        }
        return buf.toString();
    }

    /**
     * Converts a camel-case name to an upper-case name with underscores.
     *
     * <p>For example, <code>camelToUpper("FooBar")</code> returns "FOO_BAR".
     *
     * @param s Camel-case string
     * @return  Upper-case string
     */
    public static String camelToUpper(String s) {
        StringBuilder buf = new StringBuilder(s.length() + 10);
        int prevUpper = -1;
        for (int i = 0; i < s.length(); ++i) {
            char c = s.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > prevUpper + 1) {
                    buf.append('_');
                }
                prevUpper = i;
            } else {
                c = Character.toUpperCase(c);
            }
            buf.append(c);
        }
        return buf.toString();
    }

    /**
     * Parses a locale string.
     *
     * <p>The inverse operation of {@link java.util.Locale#toString()}.
     *
     * @param localeString Locale string, e.g. "en" or "en_US"
     * @return Java locale object
     */
    public static Locale parseLocale(String localeString) {
        String[] strings = localeString.split("_");
        switch (strings.length) {
        case 1:
            return new Locale(strings[0]);
        case 2:
            return new Locale(strings[0], strings[1]);
        case 3:
            return new Locale(strings[0], strings[1], strings[2]);
        default:
            throw newInternal(
                "bad locale string '" + localeString + "'");
        }
    }

    /**
     * Applies a collection of filters to an iterable.
     *
     * @param iterable Iterable
     * @param conds Zero or more conditions
     * @param <T> element type
     * @return Iterable that returns only members of underlying iterable for
     *     for which all conditions evaluate to true
     */
    public static <T> Iterable<T> filter(
        final Iterable<T> iterable,
        final Predicate1<T>... conds)
    {
        final Predicate1<T>[] conds2 = optimizeConditions(conds);
        if (conds2.length == 0) {
            return iterable;
        }
        return new Iterable<T>() {
            public Iterator<T> iterator() {
                return new Iterator<T>() {
                    final Iterator<T> iterator = iterable.iterator();
                    T next;
                    boolean hasNext = moveToNext();

                    private boolean moveToNext() {
                        outer:
                        while (iterator.hasNext()) {
                            next = iterator.next();
                            for (Predicate1<T> cond : conds2) {
                                if (!cond.test(next)) {
                                    continue outer;
                                }
                            }
                            return true;
                        }
                        return false;
                    }

                    public boolean hasNext() {
                        return hasNext;
                    }

                    public T next() {
                        T t = next;
                        hasNext = moveToNext();
                        return t;
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    private static <T> Predicate1<T>[] optimizeConditions(
        Predicate1<T>[] conds)
    {
        final List<Predicate1<T>> predicateList =
            new ArrayList<Predicate1<T>>(Arrays.asList(conds));
        for (Iterator<Predicate1<T>> funcIter = predicateList.iterator();
            funcIter.hasNext();)
        {
            Predicate1<T> predicate = funcIter.next();
            if (predicate == truePredicate1()) {
                funcIter.remove();
            }
        }
        if (predicateList.size() < conds.length) {
            //noinspection unchecked
            return predicateList.toArray(new Predicate1[predicateList.size()]);
        } else {
            return conds;
        }
    }

    /**
     * Sorts a collection of {@link Comparable} objects and returns a list.
     *
     * @param collection Collection
     * @param <T> Element type
     * @return Sorted list
     */
    public static <T extends Comparable> List<T> sort(
        Collection<T> collection)
    {
        Object[] a = collection.toArray(new Object[collection.size()]);
        Arrays.sort(a);
        return cast(Arrays.asList(a));
    }

    /**
     * Sorts a collection of objects using a {@link java.util.Comparator} and
     * returns a list.
     *
     * @param collection Collection
     * @param comparator Comparator
     * @param <T> Element type
     * @return Sorted list
     */
    public static <T> List<T> sort(
        Collection<T> collection,
        Comparator<T> comparator)
    {
        Object[] a = collection.toArray(new Object[collection.size()]);
        //noinspection unchecked
        Arrays.sort(a, (Comparator<? super Object>) comparator);
        return cast(Arrays.asList(a));
    }

    /**
     * Creates an internal error with a given message.
     */
    public static RuntimeException newInternal(String message) {
        return new RuntimeException("Internal error: " + message);
    }

    /**
     * Creates an internal error with a given message and cause.
     */
    public static RuntimeException newInternal(Throwable e, String message) {
        return new RuntimeException("Internal error: " + message, e);
    }

    /**
     * Creates a non-internal error. Currently implemented in terms of
     * internal errors, but later we will create resourced messages.
     */
    public static RuntimeException newError(String message) {
        return newInternal(message);
    }

    /**
     * Creates a non-internal error. Currently implemented in terms of
     * internal errors, but later we will create resourced messages.
     */
    public static RuntimeException newError(Throwable e, String message) {
        return newInternal(e, message);
    }

    /**
     * Converts a {@link Properties} object to a string-to-string {@link Map}.
     *
     * @param properties Properties
     * @return String-to-string map
     */
    public static Map<String, String> toMap(final Properties properties) {
        return new AbstractMap<String, String>() {
            @SuppressWarnings({"unchecked"})
            public Set<Entry<String, String>> entrySet() {
                return (Set) properties.entrySet();
            }
        };
    }

    public static String printMemory() {
        return printMemory(null);
    }

    public static String printMemory(String msg) {
        final Runtime rt = Runtime.getRuntime();
        final long freeMemory = rt.freeMemory();
        final long totalMemory = rt.totalMemory();
        final StringBuilder buf = new StringBuilder(64);

        buf.append("FREE_MEMORY:");
        if (msg != null) {
            buf.append(msg);
            buf.append(':');
        }
        buf.append(' ');
        buf.append(freeMemory / 1024);
        buf.append("kb ");

        long hundredths = (freeMemory * 10000) / totalMemory;

        buf.append(hundredths / 100);
        hundredths %= 100;
        if (hundredths >= 10) {
            buf.append('.');
        } else {
            buf.append(".0");
        }
        buf.append(hundredths);
        buf.append('%');

        return buf.toString();
    }

    /**
     * Casts a List to a List with a different element type.
     *
     * @param list List
     * @return List of desired type
     */
    @SuppressWarnings({"unchecked"})
    public static <T> List<T> cast(List<?> list) {
        return (List<T>) list;
    }

    /**
     * Looks up an enumeration by name, returning null if null or not valid.
     *
     * @param clazz Enumerated type
     * @param name Name of constant
     */
    public static <E extends Enum<E>> E lookup(Class<E> clazz, String name) {
        return lookup(clazz, name, null);
    }

    /**
     * Looks up an enumeration by name, returning a given default value if null
     * or not valid.
     *
     * @param clazz Enumerated type
     * @param name Name of constant
     * @param defaultValue Default value if constant is not found
     * @return Value, or null if name is null or value does not exist
     */
    public static <E extends Enum<E>> E lookup(
        Class<E> clazz,
        String name,
        E defaultValue)
    {
        if (name == null) {
            return defaultValue;
        }
        try {
            return Enum.valueOf(clazz, name);
        } catch (IllegalArgumentException e) {
            return defaultValue;
        }
    }

    /**
     * Make a BigDecimal from a double. On JDK 1.5 or later, the BigDecimal
     * precision reflects the precision of the double while with JDK 1.4
     * this is not the case.
     *
     * @param d the input double
     * @return the BigDecimal
     */
    public static BigDecimal makeBigDecimalFromDouble(double d) {
        return new BigDecimal(d, MathContext.DECIMAL64);
    }

    /**
     * Returns a literal pattern String for the specified String.
     *
     * @param s The string to be literalized
     * @return A literal string replacement
     */
    public static String quotePattern(String s) {
        return Pattern.quote(s);
    }

    /**
     * Function that takes one argument ({@code PT}) and returns {@code RT}.
     *
     * @param <RT> Return type
     * @param <PT> Parameter type
     */
    public static interface Function1<PT, RT> {
        RT apply(PT param);
    }

    /**
     * Predicate that takes one argument ({@code PT}).
     * Can be used as a {@code Function1&lt;PT&gt;} or as an Apache-collections
     * Predicate.
     *
     * @param <PT> Parameter type
     */
    public static abstract class Predicate1<PT>
        implements Predicate, Function1<PT, Boolean>
    {
        public Boolean apply(PT param) {
            return test(param);
        }

        public boolean evaluate(Object o) {
            //noinspection unchecked
            return test((PT) o);
        }

        public abstract boolean test(PT pt);
    }

    public static <T> Function1<T, T> identityFunction() {
        //noinspection unchecked
        return (Function1) IDENTITY_FUNCTION;
    }

    private static final Function1 IDENTITY_FUNCTION =
        new Function1<Object, Object>() {
            public Object apply(Object param) {
                return param;
            }
        };

    /**
     * Returns a predicate that takes 1 argument and always returns true.
     *
     * @param <PT> Parameter type
     * @return Predicate that always returns true
     */
    public static <PT> Predicate1<PT> truePredicate1() {
        //noinspection unchecked
        return (Predicate1) TRUE_PREDICATE1;
    }

    private static final Predicate1 TRUE_PREDICATE1 =
        new Predicate1<Object>() {
            public boolean test(Object o) {
                return true;
            }
        };
}

// End Util.java
