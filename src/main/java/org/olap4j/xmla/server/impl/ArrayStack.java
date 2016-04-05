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

import java.util.ArrayList;
import java.util.EmptyStackException;

/**
 * Stack implementation based on {@link java.util.ArrayList}.
 *
 * <p>More efficient than {@link java.util.Stack}, which extends
 * {@link java.util.Vector} and is
 * therefore synchronized whether you like it or not.
 *
 * @param <E> Element type
 *
 * @author jhyde
 */
public class ArrayStack<E> extends ArrayList<E> {
    /**
     * Default constructor.
     */
    public ArrayStack() {
        super();
    }

    /**
     * Copy Constructor
     * @param toCopy Instance of {@link ArrayStack} to copy.
     */
    public ArrayStack(ArrayStack<E> toCopy) {
        super();
        this.addAll(toCopy);
    }

    /**
     * Analogous to {@link java.util.Stack#push}.
     */
    public E push(E item) {
        add(item);
        return item;
    }

    /**
     * Analogous to {@link java.util.Stack#pop}.
     */
    public E pop() {
        int len = size();
        E obj = peek();
        remove(len - 1);
        return obj;
    }

    /**
     * Analogous to {@link java.util.Stack#peek}.
     */
    public E peek() {
        int len = size();
        if (len <= 0) {
            throw new EmptyStackException();
        }
        return get(len - 1);
    }
}

// End ArrayStack.java
