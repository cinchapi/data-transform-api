/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
import java.util.Map.Entry;

import javax.annotation.Nullable;

import com.google.common.collect.Iterables;

/**
 * A {@link Transformer} is a routine that takes a key/value pair and
 * potentially alters one or both of them, prior to import.
 * <p>
 * Sometimes, raw data from a source must be modified before being imported into
 * Concourse, for example:
 * <ul>
 * <li>Modifying keys by changing their case or stripping illegal
 * characters</li>
 * <li>Normalizing values, for example, converting strings to a specific case or
 * sanitizing</li>
 * <li>Compacting representation by values from an enumerated set of values to a
 * simple integer</li>
 * <li>Constructing a link or resolvable link</li>
 * </ul>
 * 
 * @author Jeff Nelson
 */
@FunctionalInterface
public interface Transformer {

    /**
     * Potentially transform one or both of the {@code key}/{@code value} pair.
     * If no transformation should occur, it is acceptable to return
     * {@code null} to inform the caller that the import values are acceptable
     * in their passed in state.
     * Otherwise, the preferred pair should be wrapped in an {@link Entry}
     * object.
     * 
     * @param key the raw key to potentially transform
     * @param value the raw value to potentially transform; in which case, it is
     *            acceptable to return any kind of object, but it is recommended
     *            to return an encoded String (i.e. don't return a
     *            {@link com.cinchapi.concourse.Link} object, but return a
     *            string that encodes a link (@record@) instead)
     * @return a {@link Entry} object that contains the transformed
     *         {@code key}/{@code value} pair or {@code null} if no
     *         transformation occurred
     * @deprecated in version 1.1.0; scheduled to be removed in version 2.0.0.
     *             Use {@link #transform(String, Object)} instead.
     */
    @Nullable
    @Deprecated
    public default Entry<String, Object> transform(String key, String value) {
        Map<String, Collection<Object>> transformed = transform(key,
                (Object) value);
        if(transformed != null) {
            Entry<String, Collection<Object>> entry = Iterables
                    .getFirst(transformed.entrySet(), null);
            if(entry != null) {
                return new AbstractMap.SimpleImmutableEntry<>(entry.getKey(),
                        Iterables.get(entry.getValue(), 0));
            }
        }
        return null;
    }

    /**
     * Potentially transform the provided {@code key} and {@code value} pair.
     * <p>
     * There are four possible transformation scenarios:
     * <ol>
     * <li>Neither the key or value changes</li>
     * <li>Only the key changes</li>
     * <li>Only the value changes</li>
     * <li>Both the key and value change</li>
     * </ol>
     * In scenario 1, this method returns {@code null}. In the other scenarios,
     * this method returns a {@link Map} which contains the data that will
     * <em>replace</em> the original {@code key} and {@code value}. So, if the
     * input parameters are part of a larger data map, the caller should
     * {@link AnyMaps#merge(Map)} the data from this map with the source data.
     * </p>
     * <p>
     * Even though the inputs to this method are simple, a {@link Map} is
     * returned to allow for complex transformations. For example,
     * <ul>
     * <li>You can transform a single value into multiple values by including
     * multiple objects in the associated values collection.</li>
     * <li>You can transform a single key/value pair into multiple key/value
     * pairs by including multiple keys in the returned map</li>
     * </ul>
     * For the basic case of transforming a single key/value pair into another
     * single key/value pair, you should use the
     * {@link Transformation#of(Object, Object)} utility.
     * </p>
     * 
     * @param key the raw key to potentially transform
     * @param value the raw value to potentially transform
     * @return the transformation
     */
    @Nullable
    public Map<String, Collection<Object>> transform(String key, Object value);

}
