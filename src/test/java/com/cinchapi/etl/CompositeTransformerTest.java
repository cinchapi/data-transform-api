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

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

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

}
