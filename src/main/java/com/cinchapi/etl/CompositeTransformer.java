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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nullable;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.collect.AnyMaps;
import com.cinchapi.common.collect.MergeStrategies;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.util.ByteBuffers;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A {@link Transformer} that is composed of other {@link Transformer
 * Transformers}.
 * <p>
 * Each of the composing Transformers is applied in declaration order.
 * </p>
 * <h1>Serializability</h1>
 * <p>
 * A {@link CompositedTransformer} can be
 * {@link Transformer#serialize(Transformer) serialized} if an only if all of
 * the transformers being composed can be serialized.
 * </p>
 * 
 * @author Jeff Nelson
 */
class CompositeTransformer implements Transformer, Serializable {

    private static final long serialVersionUID = 1759080269596158582L;

    /**
     * Apply the {@code transformer} to each key/value mapping within the
     * {@code object}. While doing so, use the {@code current} transformation
     * mapping as a merge source.
     * 
     * @param transformer
     * @param object
     * @param next an optional collection that tracks the future state of the
     *            {@code object} after applying the {@code transformer} to each
     *            key/value mapping
     * @return the transformed object after applying the {@code transformer} and
     *         merging with the {@code current}
     */
    private static Map<String, Object> transformAndMerge(
            Transformer transformer, Map<String, Object> object,
            @Nullable Map<String, Object> next) {
        for (Entry<String, Object> entry : object.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            Map<String, Object> transformed = transformer.transform(key, value);
            if(next == null && transformed != null) {
                // The current key/value pair is the first to result in a
                // non-null transformation, so we have to go back and
                // re-transform the entire object to preserve all the
                // non-transformed data alongside the result of transforming the
                // current key/value pair.
                next = transformAndMerge(transformer, object,
                        Maps.newLinkedHashMap());
            }
            else if(next != null) {
                AnyMaps.mergeInPlace(next,
                        transformed == null ? ImmutableMap.of(key, value)
                                : transformed,
                        MergeStrategies::theirs);
            }
        }
        return next;
    }

    /**
     * An ordered list of the {@link Transformer transformers} that are applied
     * in the composite {@link #transform(String, String)} method
     */
    private final List<Transformer> transformers;

    /**
     * Construct a new instance.
     * 
     * @param transformers
     */
    public CompositeTransformer(List<Transformer> transformers) {
        Preconditions.checkArgument(!transformers.isEmpty());
        this.transformers = ImmutableList.copyOf(transformers);
    }

    /**
     * {@inheritDoc}
     * <p>
     * NOTE: This {@link Transformer} users a breadth-first strategy where a
     * composed transformer is applied to the entire {@code object} before the
     * next one.
     * </p>
     */
    @Override
    @Nullable
    public Map<String, Object> transform(Map<String, Object> object) {
        for (Transformer transformer : transformers) {
            object = MoreObjects.firstNonNull(transformer.transform(object),
                    object);
        }
        return object;
    }

    @Override
    @Nullable
    public Map<String, Object> transform(String key, Object value) {
        Map<String, Object> transformed = null;
        for (Transformer transformer : transformers) {
            Map<String, Object> next = null;
            if(transformed == null) {
                // Either this is the first attempt to transform key/value or
                // all other previous attempts have declined to do so.
                next = transformer.transform(key, value);
            }
            else {
                // The original key/value, at some point, has been transformed
                // so we must go through the entire map and apply the
                // transformer in order to get the #next state.
                next = transformAndMerge(transformer, transformed, null);
            }
            if(next != null) {
                // NOTE: We replace #transformed with #next (instead of
                // merging) because the composition is supposed to be a
                // successive operation instead of a cumulative one.
                transformed = next;
            }
        }
        return transformed;
    }

    /**
     * Deserialize this object from the {@code in} stream.
     * 
     * @param in
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        Reflection.set("transformers", Lists.newArrayList(), this);
        int count = in.readInt();
        for (int i = 0; i < count; ++i) {
            int size = in.readInt();
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            Transformer transformer = Transformer
                    .deserialize(ByteBuffer.wrap(bytes));
            transformers.add(transformer);
        }
    }

    /**
     * Serialize this object to the {@code out} stream.
     * 
     * @param out
     * @throws IOException
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeInt(transformers.size());
        transformers.forEach(transformer -> {
            try {
                ByteBuffer bytes = Transformer.serialize(transformer);
                out.writeInt(bytes.remaining());
                out.write(ByteBuffers.toByteArray(bytes));
            }
            catch (IOException e) {
                throw CheckedExceptions.wrapAsRuntimeException(e);
            }
        });
    }

}
