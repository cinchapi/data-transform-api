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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.base.Verify;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.reflect.Reflection;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

/**
 * A factory for serializing and deserializing {@link Transformer transformers}.
 *
 * @author Jeff Nelson
 */
final class TransformerSerializationFactory {

    /**
     * Singleton.
     */
    private static TransformerSerializationFactory INSTANCE = new TransformerSerializationFactory();

    /**
     * Return the instance of the {@link TransformerSerializationFactory}.
     * 
     * @return the instance of TransformerSerializationFactory
     */
    public static TransformerSerializationFactory instance() {
        return INSTANCE;
    }

    /**
     * Serialize the {@code object} into a {@link ByteBuffer}.
     * 
     * @param object
     * @return the serialized form of the {@code object}
     */
    private static ByteBuffer getSerializedBytes(Object object) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutput output = new ObjectOutputStream(
                    new BufferedOutputStream(baos));
            output.writeObject(object);
            output.flush();
            output.close();
            return ByteBuffer.wrap(baos.toByteArray());
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Deserialize the object from {@code bytes}.
     * 
     * @param bytes
     * @return the deserialized object
     */
    @SuppressWarnings("unchecked")
    private static <T> T getSerializedObject(ByteBuffer bytes) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(
                    ByteBuffers.getByteArray(bytes));
            BufferedInputStream bis = new BufferedInputStream(bais);
            ObjectInput input = new ObjectInputStream(bis);
            T object = (T) input.readObject();
            return object;
        }
        catch (IOException | ReflectiveOperationException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * A mapping from a method name in {@link Transformers} to the {@link Class}
     * object from the lamba it provides. This is a cache to facilitate faster
     * lookups in the {@link #serialize(Transformer)} method.
     * <p>
     * NOTE: The class name for lambda is generated at runtime, so this state is
     * intentionally kept local to this instance of the factory.
     * </p>
     */
    private final Map<String, Class<? extends Transformer>> lambdas = Maps
            .newHashMap();

    private TransformerSerializationFactory() {}

    /**
     * Deserialize the {@link Transformer} from the {@code bytes} and return it.
     * 
     * @param bytes
     * @return the {@link Transformer} that was serialized to the {@code bytes}
     */
    public Transformer deserialize(ByteBuffer bytes) {
        Technique technique = Technique.values()[bytes.get()];
        switch (technique) {
        case JAVA_SERIALIZATION:
            return getSerializedObject(
                    ByteBuffers.slice(bytes, bytes.remaining()));
        case AUTO_DETECT_BUILTIN_LAMBDA:
            int nameSize = bytes.getInt();
            byte[] nameBytes = new byte[nameSize];
            bytes.get(nameBytes);
            String name = new String(nameBytes, StandardCharsets.UTF_8);
            int paramCount = bytes.getInt();
            List<Object> params = Lists
                    .newArrayListWithExpectedSize(paramCount);
            while (bytes.hasRemaining()) {
                int paramSize = bytes.getInt();
                byte[] paramBytes = new byte[paramSize];
                bytes.get(paramBytes);
                Object param = getSerializedObject(ByteBuffer.wrap(paramBytes));
                params.add(param);
            }
            return Reflection.callStatic(Transformers.class, name,
                    params.toArray());
        default:
            throw new UnsupportedOperationException(
                    "Cannot handle serialization technique " + technique);
        }
    }

    /**
     * Serialize a {@link Transformer} and return it as a {@link ByteBuffer}.
     * 
     * @param transformer
     * @return the serialized form of the {@link Transformer}
     */
    public ByteBuffer serialize(Transformer transformer) {
        ByteBuffer bytes;
        Technique technique;
        if(transformer instanceof Serializable) {
            technique = Technique.JAVA_SERIALIZATION;
            bytes = getSerializedBytes(transformer);
        }
        else {
            technique = Technique.AUTO_DETECT_BUILTIN_LAMBDA;
            // If the Transformer isn't serializable, but is a factory defined
            // in the Transformers class, hack together a serialization scheme
            // that records the factory method and the provided parameters so
            // that the Transformer can be reconstructed when deserialized.
            try {
                Field[] fields = Reflection
                        .getAllDeclaredFields(transformer.getClass());
                Class<?>[] types = new Class<?>[fields.length];
                Object[] params = new Object[fields.length];
                // Gather all the method parameter types and values
                for (int i = 0; i < types.length; ++i) {
                    Field field = fields[i];
                    field.setAccessible(true);
                    types[i] = field.getType();
                    params[i] = field.get(transformer);
                }
                // Go through each of the Transformers factories and try to
                // guess which method produced the #transformer
                List<Method> candidates = Arrays
                        .stream(Transformers.class.getDeclaredMethods())
                        .filter(method -> Reflection.isCallableWith(method,
                                params))
                        .collect(Collectors.toList());
                Method method = null;
                for (Method candidate : candidates) {
                    Class<? extends Transformer> clazz = lambdas
                            .get(candidate.getName());
                    if(clazz == null) {
                        Transformer t = (Transformer) candidate.invoke(null,
                                params);
                        clazz = t.getClass();
                        lambdas.put(candidate.getName(), clazz);
                    }
                    if(clazz.equals(transformer.getClass())) {
                        method = candidate;
                        break;
                    }
                }
                Verify.that(method != null,
                        "Cannot serialize transformer using technique "
                                + technique);
                String name = method.getName();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                baos.write(Ints.toByteArray(name.length())); // nameSize (4
                                                             // bytes)
                baos.write(name.getBytes(StandardCharsets.UTF_8)); // name
                                                                   // (nameSize
                                                                   // bytes)
                baos.write(Ints.toByteArray(params.length)); // paramCount (4
                                                             // bytes)
                for (Object param : params) {
                    /*
                     * FORMAT:
                     * - objectSize (4 bytes)
                     * - object (objectSize byes)
                     */
                    ByteBuffer buf = getSerializedBytes(param);
                    baos.write(Ints.toByteArray(buf.remaining()));
                    baos.write(ByteBuffers.getByteArray(buf));
                }
                bytes = ByteBuffer.wrap(baos.toByteArray());
            }
            catch (ReflectiveOperationException | IOException e) {
                throw CheckedExceptions.wrapAsRuntimeException(e);
            }
        }
        ByteBuffer serialized = ByteBuffer.allocate(bytes.remaining() + 1);
        serialized.put(technique.code());
        serialized.put(bytes);
        serialized.flip();
        return serialized;
    }

    /**
     * The possible serialization techniques.
     *
     * @author Jeff Nelson
     */
    private enum Technique {
        AUTO_DETECT_BUILTIN_LAMBDA, JAVA_SERIALIZATION;

        /**
         * Returns the {@link #ordinal()} as a {@link byte}.
         * 
         * @return the technique code
         */
        public byte code() {
            return (byte) ordinal();
        }
    }

}
