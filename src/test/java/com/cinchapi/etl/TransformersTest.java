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

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.collect.AnyMaps;
import com.cinchapi.common.describe.Empty;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.util.Random;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * Unit tests for the built-in {@link Transformers}.
 *
 * @author Jeff Nelson
 */
public class TransformersTest {

    @Test
    public void testValueAsBooleanInvalidIsAlwaysFalse() {
        Map<String, Object> transformed = Transformers.valueAsBoolean()
                .transform("foo", (Object) (Random.getString() + "f"));
        Object value = Iterables.getOnlyElement(transformed.values());
        Assert.assertFalse((boolean) value);
    }

    @Test
    public void testValueAsBooleanTrue() {
        Map<String, Object> transformed = Transformers.valueAsBoolean()
                .transform("foo", (Object) "True");
        Object value = Iterables.getOnlyElement(transformed.values());
        Assert.assertTrue((boolean) value);

        transformed = Transformers.valueAsBoolean().transform("foo",
                (Object) "true");
        value = Iterables.getOnlyElement(transformed.values());
        Assert.assertTrue((boolean) value);

        transformed = Transformers.valueAsBoolean().transform("foo",
                (Object) "TRUE");
        value = Iterables.getOnlyElement(transformed.values());
        Assert.assertTrue((boolean) value);

        transformed = Transformers.valueAsBoolean().transform("foo",
                (Object) "tRuE");
        value = Iterables.getOnlyElement(transformed.values());
        Assert.assertTrue((boolean) value);
    }

    @Test
    public void testValueAsBooleanFalse() {
        Map<String, Object> transformed = Transformers.valueAsBoolean()
                .transform("foo", (Object) "False");
        Object value = Iterables.getOnlyElement(transformed.values());
        Assert.assertFalse((boolean) value);

        transformed = Transformers.valueAsBoolean().transform("foo",
                (Object) "false");
        value = Iterables.getOnlyElement(transformed.values());
        Assert.assertFalse((boolean) value);

        transformed = Transformers.valueAsBoolean().transform("foo",
                (Object) "FALSE");
        value = Iterables.getOnlyElement(transformed.values());
        Assert.assertFalse((boolean) value);

        transformed = Transformers.valueAsBoolean().transform("foo",
                (Object) "FalSe");
        value = Iterables.getOnlyElement(transformed.values());
        Assert.assertFalse((boolean) value);
    }

    @Test
    public void testValueAsNumber() {
        Map<String, Object> transformed = Transformers.valueAsNumber()
                .transform("foo", (Object) "1");
        Object value = Iterables.getOnlyElement(transformed.values());
        Assert.assertEquals(1, value);

        transformed = Transformers.valueAsNumber().transform("foo",
                (Object) "1.0");
        value = Iterables.getOnlyElement(transformed.values());
        Assert.assertEquals((float) 1.0, value);
    }

    @Test
    public void testValueAsResolvableLink() {
        Map<String, Object> transformed = Transformers
                .valueAsResolvableLinkInstruction()
                .transform("foo", (Object) "name = jeff");
        Object value = Iterables.getOnlyElement(transformed.values());
        Assert.assertTrue(value.toString().startsWith("@"));
        Assert.assertTrue(value.toString().endsWith("@"));
    }

    @Test
    public void testValueAsTag() {
        Map<String, Object> transformed = Transformers.valueAsTag()
                .transform("foo", (Object) "name = jeff");
        Object value = Iterables.getOnlyElement(transformed.values());
        Assert.assertTrue(value instanceof Tag);
    }

    @Test
    public void testValueNullifyIfEmpty() {
        Map<String, Object> expected = Maps.newHashMap();
        expected.put("foo", null);
        Empty empty = Empty.is(ImmutableMap.of(String.class,
                str -> StringUtils.isBlank((String) str)));
        Assert.assertEquals(expected, Transformers.valueNullifyIfEmpty(empty)
                .transform("foo", (Object) "   "));
        Assert.assertNull(Transformers.valueNullifyIfEmpty(empty)
                .transform("foo", (Object) null));
    }

    @Test
    public void testValueRemoveIfEmpty() {
        Empty empty = Empty.is(ImmutableMap.of(String.class,
                str -> StringUtils.isBlank((String) str)));
        Assert.assertEquals(ImmutableMap.of(), Transformers.valueRemoveIf(empty)
                .transform("foo", (Object) "   "));
    }

    @Test
    public void testNullValueHandling() {
        Transformer transformer = Transformers.noOp();
        Map<String, Object> expected = AnyMaps.create("foo", null);
        Map<String, Object> actual = transformer.transform(expected);
        Assert.assertEquals(expected, actual);
    }

}
