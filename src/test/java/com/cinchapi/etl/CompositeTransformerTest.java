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

import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.collect.Association;
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
        Transformer transformer = Transformers.composeForEach(
                Transformers.keyToLowerCase(),
                Transformers.valueSplitOnDelimiter(','),
                Transformers.valueStringToJava(), (key, value) -> {
                    if(value.equals(3)) {
                        return Transformation.to("bar", value);
                    }
                    else {
                        return null;
                    }
                }, (key, value) -> {
                    if(key.equals("bar")) {
                        return Transformation.to(key, value,
                                value.toString() + "-Extra");
                    }
                    else {
                        return null;
                    }
                });
        Map<String, Object> transformed = transformer.transform("Foo",
                (Object) "1,2,3,4,1");
        Assert.assertEquals(ImmutableMap.of("foo", ImmutableList.of(1, 2, 4, 1),
                "bar", ImmutableList.of(3, "3-Extra")), transformed);

    }

    @Test
    public void testTransformObjectWithTransformationsToNavigableKeys() {
        Transformer transformer = Transformers.compose(
                Transformers.keyRemoveInvalidChars(Character::isWhitespace),
                Transformers.keyMap(ImmutableMap.of("Foo1", "foo.1.name",
                        "Foo1Description", "foo.1.description", "Foo2",
                        "foo.2.name", "Foo2Description", "foo.2.description")));
        Map<String, Object> object = ImmutableMap.of("Foo1", "Bar",
                "Foo1 Description", "Bar Bar", "Foo2", "Bar 2",
                "Foo2 Description", "Bar Bar");
        Map<String, Object> transformed = transformer.transform(object);
        System.out.println(transformed);
        Assert.assertEquals(ImmutableMap.of("foo.1.name", "Bar",
                "foo.1.description", "Bar Bar", "foo.2.name", "Bar 2",
                "foo.2.description", "Bar Bar"), transformed);
    }

    @Test
    public void testTransformObjectWithTransformationsToNavigableKeysAndExplode() {
        Transformer transformer = Transformers.compose(
                Transformers.keyRemoveInvalidChars(Character::isWhitespace),
                Transformers.keyMap(ImmutableMap.of("Foo1", "foo.0.name",
                        "Foo1Description", "foo.0.description", "Foo2",
                        "foo.1.name", "Foo2Description", "foo.1.description")),
                Transformers.explode());
        Map<String, Object> object = ImmutableMap.of("Foo1", "Bar",
                "Foo1 Description", "Bar Bar", "Foo2", "Bar 2",
                "Foo2 Description", "Bar Bar");
        Map<String, Object> transformed = transformer.transform(object);
        Assert.assertEquals(Association.of(ImmutableMap.of("foo.0.name", "Bar",
                "foo.0.description", "Bar Bar", "foo.1.name", "Bar 2",
                "foo.1.description", "Bar Bar")), transformed);
    }

}
