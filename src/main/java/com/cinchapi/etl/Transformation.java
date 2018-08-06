/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.etl;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * A {@link Transformation} is a {@link Map mapping from key to a one or more
 * values wrapped in a {@link Collection}. This class exists purely for
 * semantics and is not extended for instantiation. It contains static factory
 * methods for common transformations.
 * 
 * @author Jeff Nelson
 */
public final class Transformation
        extends AbstractMap<String, Collection<Object>> {

    private Transformation() {/* no-init */}

    /**
     * Return a transformation containing a single key/value pair.
     * 
     * @param key
     * @param value
     * @return the transformation
     */
    public static Map<String, Collection<Object>> of(String key, Object value) {
        return ImmutableMap.of(key, ImmutableList.of(value));
    }

    /**
     * Return a transformation containing mapping from a single key to multiple
     * values.
     * 
     * @param key
     * @param values
     * @return the transformation
     */
    public static Map<String, Collection<Object>> of(String key,
            Object... values) {
        return ImmutableMap.of(key, Lists.newArrayList(values));
    }

    @Override
    public Set<Entry<String, Collection<Object>>> entrySet() {
        throw new UnsupportedOperationException();
    }

}
