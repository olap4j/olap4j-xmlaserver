/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.olap4j.xmla.server.impl;

import java.util.AbstractList;
import java.util.List;

/**
 * List composed of several lists.
 *
 * @param <T> element type
 *
 * @author jhyde
 */
public class CompositeList<T> extends AbstractList<T> {
    private final List<? extends T>[] lists;

    /**
     * Creates a composite list.
     *
     * @param lists Component lists
     */
    public CompositeList(
        List<? extends T>... lists)
    {
        this.lists = lists;
    }

    /**
     * Creates a composite list, inferring the element type from the arguments.
     *
     * @param lists One or more lists
     * @param <T> element type
     * @return composite list
     */
    public static <T> CompositeList<T> of(
        List<? extends T>... lists)
    {
        return new CompositeList<T>(lists);
    }

    public T get(int index) {
        int n = 0;
        for (List<? extends T> list : lists) {
            int next = n + list.size();
            if (index < next) {
                return list.get(index - n);
            }
            n = next;
        }
        throw new IndexOutOfBoundsException(
            "index" + index + " out of bounds in list of size " + n);
    }

    public int size() {
        int n = 0;
        for (List<? extends T> array : lists) {
            n += array.size();
        }
        return n;
    }
}

// End CompositeList.java
