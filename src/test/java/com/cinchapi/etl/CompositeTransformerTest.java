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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.collect.Collections;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

/**
 * Unit tests for the {@link CompositeTransformer} class.
 *
 * @author Jeff Nelson
 */
public class CompositeTransformerTest {

    @Test
    public void testCase1() {
        Transformer transformer = Transformers.compose(
                Transformers.keyToLowerCase(),
                Transformers.valueSplitOnDelimiter(','),
                Transformers.valueStringToJava(), (key, value) -> {
                    if(value.equals(3)) {
                        return Transformations.singleKeyValuePair("bar", value);
                    }
                    else {
                        return Transformations.none();
                    }
                }, (key, value) -> {
                    if(key.equals("bar")) {
                        return Transformations.singleKeyToMultiValues(key,
                                value, value.toString() + "-Extra");
                    }
                    else {
                        return null;
                    }
                });
        Map<String, Collection<Object>> transformed = transformer
                .transform("Foo", "1,2,3,4,1");
        Assert.assertEquals(ImmutableMap.of("foo", ImmutableList.of(1, 2, 4, 1),
                "bar", ImmutableList.of(3, "3-Extra")), transformed);

    }

    @Test
    public void testCase2() {
        Transformer transformer = Transformers
                .compose(
                        Transformers.keyStripInvalidChars(
                                c -> !Character.isWhitespace(c)),
                        (key, value) -> {
                            switch (key) {
                            case "Foo1":
                                return Transformations.singleKeyValuePair(
                                        "foo.1.name", value);
                            case "Foo1Description":
                                return Transformations.singleKeyValuePair(
                                        "foo.1.description", value);
                            case "Foo2":
                                return Transformations.singleKeyValuePair(
                                        "foo.2.name", value);
                            case "Foo2Description":
                                return Transformations.singleKeyValuePair(
                                        "foo.2.description", value);
                            default:
                                return null;
                            }
                        });
        Map<String, Set<Object>> object = ImmutableMap.of("Foo1",
                ImmutableSet.of("Bar"), "Foo1 Description",
                ImmutableSet.of("Bar Bar"), "Foo2", ImmutableSet.of("Bar 2"),
                "Foo2 Description", ImmutableSet.of("Bar Bar"));
        Map<String, Collection<Object>> transformed = Maps.newLinkedHashMap();
        object.forEach((key, values) -> {
            values.forEach(value -> {
                Map<String, Collection<Object>> t = transformer.transform(key,
                        value);
                if(t != null) {
                    t.forEach((k, vs) -> {
                        transformed.merge(k, vs, Collections::concat);
                    });
                }
                else {
                    transformed.merge(key, ImmutableList.of(value),
                            Collections::concat);
                }

            });
        });
        Assert.assertEquals(
                ImmutableMap.of("foo.1.name", ImmutableList.of("Bar"),
                        "foo.1.description", ImmutableList.of("Bar Bar"),
                        "foo.2.name", ImmutableList.of("Bar 2"),
                        "foo.2.description", ImmutableList.of("Bar Bar")),
                transformed);
    }

}
