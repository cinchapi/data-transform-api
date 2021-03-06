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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.CaseFormats;
import com.cinchapi.common.base.SplitOption;
import com.cinchapi.common.base.StringSplitter;
import com.cinchapi.common.base.validate.Check;
import com.cinchapi.common.collect.AnyMaps;
import com.cinchapi.common.collect.Association;
import com.cinchapi.common.collect.MergeStrategies;
import com.cinchapi.common.collect.Sequences;
import com.cinchapi.common.describe.Adjective;
import com.cinchapi.common.describe.Empty;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.util.Convert;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Utilities for import {@link Transformer} objects.
 * 
 * @author Jeff Nelson
 */
@SuppressWarnings("deprecation")
public final class Transformers {

    /**
     * Return a {@link CompositeTransformer} that invokes each of the
     * {@code transformers} in order.
     * 
     * @param transformers the {@link Transformer transformers} to invoke
     * @return a {@link CompositeTransformer}
     */
    public static Transformer compose(Transformer... transformers) {
        return new CompositeTransformer(Arrays.asList(transformers));
    }

    /**
     * Return a {@link CompositeTransformer} that wrapers each of the
     * {@code transformers} in a {@link #forEach(Transformer)} transformer} and
     * invokes each of them in order
     * 
     * @param transformers
     * @return a {@link CompositeTransformer}
     */
    public static Transformer composeForEach(Transformer... transformers) {
        return new CompositeTransformer(Arrays.asList(transformers).stream()
                .map(transformer -> forEach(transformer))
                .collect(Collectors.toList()));
    }

    /**
     * Return a {@link Transformer} that copies that value associated with
     * {@code fromKey} to {@code toKey}.
     * 
     * @param fromKey
     * @param toKey
     * @return the {@link Transformer}
     */
    public static Transformer copy(String fromKey, String toKey) {
        return (key, value) -> {
            if(key.equals(fromKey)) {
                Map<String, Object> transformed = Maps.newLinkedHashMap();
                transformed.put(fromKey, value);
                transformed.put(toKey, value);
                return transformed;
            }
            else {
                return null;
            }
        };
    }

    /**
     * Return a {@link Transformer} that explodes a {@code key}/{@code value}
     * pair into a nested data structure.
     * 
     * @return the {@link Transformer}
     */
    public static Transformer explode() {
        return (key, value) -> Association.of(ImmutableMap.of(key, value));
    }

    /**
     * Return a {@link Transformer} that will apply the provided
     * {@code transformer} to every item in a value that is a
     * {@link Sequences#isSequence(Object) sequence} or to the value itself if
     * the value is not a sequence.
     * <p>
     * NOTE: You should always wrap subsequent {@link Transformer transformers}
     * within this this one when it is likely that the input values will be
     * collections instead of "flat" objects. For instance, if you have a
     * {@link #compose(Transformer...) composite} transformation chain and one
     * of the transformers in the sequence is
     * {@link #valueSplitOnDelimiter(char, SplitOption...)}, you should wrap all
     * the following transformers using this method to ensure that the logic is
     * applied to each item.
     * </p>
     * 
     * @param transformer the {@link Transformer} to apply to every element
     * @return the {@link Transformer}
     */
    public static Transformer forEach(Transformer transformer) {
        return (key, value) -> {
            if(Sequences.isSequence(value)) {
                Map<String, Object> transformed = Maps.newLinkedHashMap();
                Sequences.forEach(value, val -> {
                    Map<String, Object> theirs = transformer.transform(key,
                            val);
                    AnyMaps.mergeInPlace(transformed,
                            theirs != null ? theirs : ImmutableMap.of(key, val),
                            MergeStrategies::concat);
                });
                return transformed.isEmpty() ? null : transformed;
            }
            else {
                return transformer.transform(key, value);
            }
        };
    }

    /**
     * Return a {@link Transformer} that converts keys {@code from} one case
     * format {@code to} another one.
     * 
     * @param from the original {@link CaseFormat}
     * @param to the desired {@link CaseFormat}
     * @return the {@link Transformer}
     * @deprecated use {@link #keyEnsureCaseFormat(CaseFormat)} or
     *             {@link #keyConditionalConvertCaseFormat(CaseFormat, CaseFormat)}
     *             instead depending on the desired transformation
     */
    @Deprecated
    public static Transformer keyCaseFormat(CaseFormat from, CaseFormat to) {
        return (key, value) -> {
            key = from.to(to, key);
            return Transformation.to(key, value);
        };
    }

    /**
     * Return a {@link Transformer} that converts a key to a {@link CaseFormat}
     * if and only if the key currently matches another {@code undesired}
     * format.
     * <p>
     * This transformer should be use when there is only one case format that is
     * undesirable (e.g. it is fine if a key takes on any other case format). If
     * you want to ensure that a key matches a particular case format, use
     * {@link #keyEnsureCaseFormat(CaseFormat)}.
     * </p>
     * 
     * @param undesired
     * @param desired
     * @return the {@link Transformer}
     */
    public static Transformer keyConditionalConvertCaseFormat(
            CaseFormat undesired, CaseFormat desired) {
        return (key, value) -> {
            CaseFormat format = CaseFormats.detect(key);
            return format == undesired
                    ? Transformation.to(undesired.to(desired, key), value)
                    : null;
        };
    }

    /**
     * Return a {@link Transformer} that ensures a key is in the specified case
     * {@link CaseFormat format}
     * 
     * @param format
     * @return the {@link Transformer}
     */
    public static Transformer keyEnsureCaseFormat(CaseFormat format) {
        return (key, value) -> {
            CaseFormat current = CaseFormats.detect(key);
            if(current != format) {
                key = current.to(format, key);
                return Transformation.to(key, value);
            }
            else {
                return null;
            }
        };
    }

    /**
     * Transform keys to other keys using the provided {@code map}ping.
     * 
     * @param map
     * @return the {@link Transformer}
     * @deprecated use {@link #keyRename(Map)} instead
     */
    @Deprecated
    public static Transformer keyMap(Map<String, String> map) {
        return keyRename(map);
    }

    /**
     * Return a {@link Transformer} that removes all the character in the
     * {@code invalid} collection.
     * 
     * @param invalid
     * @return the {@link Transformer}
     */
    public static Transformer keyRemoveInvalidChars(
            Collection<Character> invalid) {
        return keyRemoveInvalidChars(c -> invalid.contains(c));
    }

    /**
     * Return a {@link Transformer} that removes all characters that match the
     * {@code invalid} {@link Predicate} from a key.
     * 
     * @param invalid
     * @return the {@link Transformer}
     */
    public static Transformer keyRemoveInvalidChars(
            Predicate<Character> invalid) {
        return (key, value) -> {
            boolean modified = false;
            for (char c : key.toCharArray()) {
                if(invalid.test(c)) {
                    modified = true;
                    break;
                }
                else {
                    continue;
                }
            }
            if(modified) {
                StringBuilder sb = new StringBuilder();
                for (char c : key.toCharArray()) {
                    if(invalid.negate().test(c)) {
                        sb.append(c);
                    }
                }
                return Transformation.to(sb.toString(), value);
            }
            else {
                return null;
            }
        };
    }

    /**
     * Return a {@link Transformer} that removes all whitespace characters from
     * a key.
     * 
     * @return the {@link Transformer}
     */
    public static Transformer keyRemoveWhitespace() {
        return keyRemoveInvalidChars(Character::isWhitespace);
    }

    /**
     * Return a {@link Transformer} that renames keys. The renaming rules are
     * that each key in the provided {@code map} is renamed to its associated
     * value.
     * 
     * @param map
     * @return the {@link Transformer}
     */
    public static Transformer keyRename(Map<String, String> map) {
        return (key, value) -> {
            key = map.get(key);
            if(key != null) {
                return Transformation.to(key, value);
            }
            else {
                return null;
            }
        };
    }

    /**
     * Return a {@link Transformer} that renames key {@code from} to the
     * specified {@code to} key.
     * 
     * @param from
     * @param to
     * @return the {@link Transformer}
     */
    public static Transformer keyRename(String from, String to) {
        return keyRename(ImmutableMap.of(from, to));
    }

    /**
     * Return a {@link Transformer} that replaces all the character keys in the
     * {@code replacements} mapping with their associated character values in
     * each data key.
     * 
     * @param replacements a mapping from characters to another character with
     *            which it should be replaced
     * @return the {@link Transformer}
     */
    public static Transformer keyReplaceChars(
            Map<Character, Character> replacements) {
        return (key, value) -> {
            char[] chars = key.toCharArray();
            boolean modified = false;
            for (int i = 0; i < chars.length; ++i) {
                Character replacement = replacements.get(chars[i]);
                if(replacement != null) {
                    chars[i] = replacement;
                    modified = true;
                }
            }
            return modified ? Transformation.to(new String(chars), value)
                    : null;
        };
    }

    /**
     * Return a {@link Transformer} that strips invalid characters from the key.
     * Some of those characters are replaced with valid stand-ins.
     * 
     * @return the {@link Transformer}
     * @deprecated use {@link #keyRemoveInvalidChars(Predicate)} which takes a
     *             {@link Predicate} that tests whether a character is
     *             <strong>invalid</strong>. Please note that said functionality
     *             is the inverse of this method, which takes a {@link Check}
     *             that tests whether a character is valid.
     */
    @Deprecated
    public static Transformer keyStripInvalidChars(Check<Character> validator) {
        return (key, value) -> {
            StringBuilder sb = new StringBuilder();
            boolean modified = false;
            for (char c : key.toCharArray()) {
                if(validator.passes(c)) {
                    sb.append(c);
                }
                else {
                    modified = true;
                }
            }
            return modified ? Transformation.to(sb.toString(), value) : null;
        };
    }

    /**
     * Return a {@link Transformer} that converts all the characters in a key to
     * lowercase. No other modifications to the key are made.
     * 
     * @return the {@link Transformer}
     */
    public static Transformer keyToLowerCase() {
        return (key, value) -> Transformation.to(key.toLowerCase(), value);
    }

    /**
     * Return a {@link Transformer} that will remove single and double quotes
     * from the beginning and end of both the key and value.
     */
    public static Transformer keyValueRemoveQuotes() {
        return (key, value) -> {
            boolean modified = false;
            if(AnyStrings.isWithinQuotes(key)) {
                key = key.substring(1, key.length() - 1);
                modified = true;
            }
            if(value instanceof String
                    && AnyStrings.isWithinQuotes((String) value)) {
                String str = (String) value;
                value = str.substring(1, str.length() - 1);
                modified = true;
            }
            return modified ? Transformation.to(key, value) : null;
        };
    }

    /**
     * Return a {@link Transformer} that will strip single and double quotes
     * from the beginning and end of both the key and value.
     * 
     * @deprecated use {@link #keyValueRemoveQuotes()} which has the exact same
     *             functionality
     */
    @Deprecated
    public static Transformer keyValueStripQuotes() {
        return keyValueRemoveQuotes();
    }

    /**
     * Return a {@link Transformer} that replaces whitespace characters with an
     * underscore in keys.
     * 
     * @return the {@link Transformer}
     */
    public static Transformer keyWhitespaceToUnderscore() {
        return keyReplaceChars(ImmutableMap.of(' ', '_'));
    }

    /**
     * Return a {@link Transformer} that applies the provided
     * {@code transformer} throughout the entirety of a nested {@link Map} or to
     * the value itself if it is not a {@link Map}.
     * <p>
     * NOTE: You should always wrap subsequent {@link Transformer transformers}
     * within this this one when it is likely that the input values will be
     * maps instead of "flat" objects. For instance, if you have a
     * {@link #compose(Transformer...) composite} transformation chain and one
     * of the transformers in the sequence is
     * {@link #explode()}, you should wrap all
     * the following transformers using this method to ensure that the logic is
     * applied to each nested map.
     * </p>
     * 
     * @param transformer
     * @return the {@link Transformer}
     */
    @SuppressWarnings("unchecked")
    public static Transformer nest(Transformer transformer) {
        return (key, value) -> {
            Map<String, Object> initial = transformer.transform(key, value);
            initial = initial == null ? AnyMaps.create(key, value) : initial;
            Map<String, Object> nested = Maps.newLinkedHashMap();
            initial.forEach((k, v) -> {
                if(v instanceof Map) {
                    Map<String, Object> map = (Map<String, Object>) v;
                    map.forEach((_k, _v) -> {
                        Map<String, Object> inner = nest(transformer)
                                .transform(_k, _v);
                        inner = Maps.newLinkedHashMap(inner);
                        AnyMaps.mergeInPlace(nested,
                                inner != null ? ImmutableMap.of(k, inner)
                                        : ImmutableMap.of(k,
                                                ImmutableMap.of(_k, _v)),
                                MergeStrategies::upsert);
                    });
                }
                else if(Sequences.isSequence(v)) {
                    Map<String, Object> forEach = forEach(nest(transformer))
                            .transform(key, v);
                    AnyMaps.mergeInPlace(nested,
                            forEach != null ? forEach : ImmutableMap.of(key, v),
                            MergeStrategies::concat);
                }
            });
            return nested.isEmpty() ? initial : nested;
        };
    }

    /**
     * Return a {@link Transformer} that does not perform any key or value
     * transformations.
     * 
     * @return a no-op {@link Transformer}
     */
    public static Transformer noOp() {
        return (key, value) -> null;
    }

    /**
     * Return a {@link Transformer} that only attempts transformation if the
     * value is not {@code null}.
     * 
     * @param transformer
     * @return the {@link Transformer}
     */
    public static Transformer nullSafe(Transformer transformer) {
        return (key, value) -> value != null ? transformer.transform(key, value)
                : null;
    }

    /**
     * Return a {@link Transformer} that will cause a key/value pair to be
     * "removed" if the value is described by the provided {@code adjective}.
     * <p>
     * Removal is accomplished by returning an empty map for the transformation.
     * </p>
     * 
     * @param adjective
     * @return the {@link Transformer}
     * @deprecated use {@link #valueRemoveIf(Adjective) instead}
     */
    @Deprecated
    public static Transformer removeValuesThatAre(Adjective adjective) {
        return valueRemoveIf(adjective);
    }

    /**
     * Return a {@link Transformer} that, For EVERY key, transform values to a
     * {@link Boolean} if possible. If the value cannot be transformed, an
     * exception is thrown.
     * 
     * @return the {@link Transformer}
     */
    public static Transformer valueAsBoolean() {
        return (key, value) -> {
            if(value instanceof Boolean) {
                return null;
            }
            else {
                return Transformation.to(key,
                        Boolean.parseBoolean(value.toString()));
            }
        };
    }

    /**
     * Return a {@link Transformer} that, for each of the {@code keys},
     * transform values to a {@link Boolean} if possible. If the value cannot
     * be transformed, an exception is thrown. If {@code keys} is an empty
     * array, this transformation is applied to EVERY key (a la
     * {@link #valueAsBoolean()}.
     * 
     * @param key the keys for which the transformer is applied
     * @return the {@link Transformer}
     */
    public static Transformer valueAsBoolean(String... keys) {
        if(keys.length == 0) {
            return valueAsBoolean();
        }
        else {
            Set<String> _keys = Arrays.stream(keys).collect(Collectors.toSet());
            return (key, value) -> {
                if(_keys.contains(key)) {
                    return valueAsBoolean().transform(key, value);
                }
                else {
                    return null;
                }
            };
        }
    }

    /**
     * Return a {@link Transformer} that, For EVERY key, transform values to a
     * {@link Number} if possible. If the value cannot be transformed, an
     * exception is thrown.
     * 
     * @return the {@link Transformer}
     */
    public static Transformer valueAsNumber() {
        return (key, value) -> {
            if(value instanceof Number) {
                return null;
            }
            else {
                Number number = AnyStrings.tryParseNumber(value.toString());
                if(number != null) {
                    return Transformation.to(key, number);
                }
                else {
                    throw new IllegalArgumentException(
                            value + " cannot be transformed to a Number");
                }
            }
        };
    }

    /**
     * Return a {@link Transformer} that, for each of the {@code keys},
     * transform values to a {@link Number} if possible. If the value cannot
     * be transformed, an exception is thrown. If {@code keys} is an empty
     * array, this transformation is applied to EVERY key (a la
     * {@link #valueAsNumber()}.
     * 
     * @param key the keys for which the transformer is applied
     * @return the {@link Transformer}
     */
    public static Transformer valueAsNumber(String... keys) {
        if(keys.length == 0) {
            return valueAsNumber();
        }
        else {
            Set<String> _keys = Arrays.stream(keys).collect(Collectors.toSet());
            return (key, value) -> {
                if(_keys.contains(key)) {
                    return valueAsNumber().transform(key, value);
                }
                else {
                    return null;
                }
            };
        }
    }

    /**
     * Return a {@link Transformer} that, For EVERY key, transform values to a
     * {@link Convert#stringToResolvableLinkInstruction(String) resolvable link
     * instruction} if possible. If the value cannot be transformed, an
     * exception is thrown.
     * 
     * @return the {@link Transformer}
     */
    public static Transformer valueAsResolvableLinkInstruction() {
        return (key, value) -> Transformation.to(key,
                Convert.stringToResolvableLinkInstruction(value.toString()));
    }

    /**
     * Return a {@link Transformer} that, for each of the {@code keys},
     * transform values to a
     * {@link Convert#stringToResolvableLinkInstruction(String) resolvable link
     * instruction} if possible. If the value cannot be transformed, an
     * exception is thrown. If {@code keys} is an empty array, this
     * transformation is applied to EVERY key (a la
     * {@link #valueAsResolvableLinkInstruction()}.
     * 
     * @param key the keys for which the transformer is applied
     * @return the {@link Transformer}
     */
    public static Transformer valueAsResolvableLinkInstruction(String... keys) {
        if(keys.length == 0) {
            return valueAsResolvableLinkInstruction();
        }
        else {
            Set<String> _keys = Arrays.stream(keys).collect(Collectors.toSet());
            return (key, value) -> {
                if(_keys.contains(key)) {
                    return valueAsResolvableLinkInstruction().transform(key,
                            value);
                }
                else {
                    return null;
                }
            };
        }
    }

    /**
     * Return a {@link Transformer} that, for EVERY key, transforms values to a
     * {@link String}.
     * 
     * @return the {@link Transformer}
     */
    public static Transformer valueAsString() {
        return (key, value) -> value instanceof String ? null
                : Transformation.to(key, value.toString());
    }

    /**
     * Return a {@link Transformer} that, for the specified {@code key}as,
     * transforms values to a {@link String}.
     * 
     * @param keys
     * @return the {@link Transformer}
     */
    public static Transformer valueAsString(String... keys) {
        if(keys.length == 0) {
            return valueAsString();
        }
        else {
            Set<String> _keys = Arrays.stream(keys).collect(Collectors.toSet());
            return (key, value) -> {
                if(_keys.contains(key)) {
                    return valueAsString().transform(key, value);
                }
                else {
                    return null;
                }
            };
        }
    }

    /**
     * Return a {@link Transformer} that, For EVERY key, transform values to a
     * {@link Number} if possible. If the value cannot be transformed, an
     * exception is thrown.
     * 
     * @return the {@link Transformer}
     */
    public static Transformer valueAsTag() {
        return (key, value) -> {
            if(value instanceof Tag) {
                return null;
            }
            else {
                return Transformation.to(key, Tag.create(value.toString()));
            }
        };
    }

    /**
     * Return a {@link Transformer} that, for each of the {@code keys},
     * transform values to a {@link Tag} if possible. If the value cannot
     * be transformed, an exception is thrown. If {@code keys} is an empty
     * array, this transformation is applied to EVERY key (a la
     * {@link #valueAsTag()}.
     * 
     * @param key the keys for which the transformer is applied
     * @return the {@link Transformer}
     */
    public static Transformer valueAsTag(String... keys) {
        if(keys.length == 0) {
            return valueAsTag();
        }
        else {
            Set<String> _keys = Arrays.stream(keys).collect(Collectors.toSet());
            return (key, value) -> {
                if(_keys.contains(key)) {
                    return valueAsTag().transform(key, value);
                }
                else {
                    return null;
                }
            };
        }
    }

    /**
     * Return a {@link Transformer} that, For EVERY key, transform values to a
     * {@link Timestamp} if possible. If the value cannot be transformed, an
     * exception is thrown.
     * 
     * @return the {@link Transformer}
     */
    public static Transformer valueAsTimestamp() {
        return (key, value) -> {
            if(value instanceof Timestamp) {
                return null;
            }
            else if(value instanceof Number) {
                return Transformation.to(key,
                        Timestamp.fromMicros(((Number) value).longValue()));
            }
            else {
                return Transformation.to(key,
                        Timestamp.fromString(value.toString()));
            }
        };
    }

    /**
     * Return a {@link Transformer} that, for each of the {@code keys},
     * transform values to a {@link Timestamp} if possible. If the value cannot
     * be transformed, an exception is thrown. If {@code keys} is an empty
     * array, this transformation is applied to EVERY key (a la
     * {@link #valueAsTimestamp()}.
     * 
     * @param key the keys for which the transformer is applied
     * @return the {@link Transformer}
     */
    public static Transformer valueAsTimestamp(String... keys) {
        if(keys.length == 0) {
            return valueAsTimestamp();
        }
        else {
            Set<String> _keys = Arrays.stream(keys).collect(Collectors.toSet());
            return (key, value) -> {
                if(_keys.contains(key)) {
                    return valueAsTimestamp().transform(key, value);
                }
                else {
                    return null;
                }
            };
        }
    }

    /**
     * Return a {@link Transformer} that replaces a value with {@code null} it
     * meets the default definition of {@link Empty#ness()}.
     * 
     * @return the {@link Transformer}
     */
    public static Transformer valueNullifyIfEmpty() {
        return valueNullifyIfEmpty(Empty.ness());
    }

    /**
     * Return a {@link Transformer} that replaces a value with {@code null} it
     * meets the provided definition of {@link Empty empty}.
     * 
     * @param empty
     * @return the {@link Transformer}
     */
    public static Transformer valueNullifyIfEmpty(Empty empty) {
        return (key, value) -> {
            if(value != null && empty.describes(value)) {
                return Transformation.to(key, (Object) null);
            }
            else {
                return null;
            }
        };
    }

    /**
     * Return a {@link Transformer} that will cause a key/value pair to be
     * "removed" if the value is described by the provided {@code adjective}.
     * <p>
     * Removal is accomplished by returning an empty map for the transformation.
     * </p>
     * 
     * @param adjective
     * @return the {@link Transformer}
     */
    public static Transformer valueRemoveIf(Adjective adjective) {
        return (key, value) -> adjective.describes(value) ? ImmutableMap.of()
                : null;
    }

    /**
     * Return a {@link Transformer} that will caused a key/value pair to be
     * "removed" if the value is described by the default definition of
     * {@link Empty#ness() Empty}.
     * <p>
     * Removal is accomplished by returning an empty map for the transformation.
     * </p>
     * <p>
     * NOTE: If the default definition of {@link Empty} changes, the behaviour
     * of this {@link Transformer} will also change accordingly.
     * </p>
     * 
     * @return the {@link Transformer}
     */
    public static Transformer valueRemoveIfEmpty() {
        return (key, value) -> Empty.ness().describes(value) ? ImmutableMap.of()
                : null;
    }

    /**
     * Return a {@link Transformer} that splits a String value into multiple
     * strings that are all mapped from the original key.
     * 
     * @param delimiter the character on which to split the String
     * @param options the optional {@link SplitOption SplitOptions}
     * @return the {@link Transformer}
     * @deprecated use
     *             {@link #valueStringSplitOnDelimiter(char, SplitOption...)}
     *             instead
     */
    @Deprecated
    public static Transformer valueSplitOnDelimiter(char delimiter,
            SplitOption... options) {
        return valueStringSplitOnDelimiter(delimiter, options);
    }

    /**
     * Return a {@link Transformer} that splits a String value into multiple
     * strings that are all mapped from the original key.
     * 
     * @param delimiter the character on which to split the String
     * @param options the optional {@link SplitOption SplitOptions}
     * @return the {@link Transformer}
     */
    public static Transformer valueStringSplitOnDelimiter(char delimiter,
            SplitOption... options) {
        return (key, value) -> {
            if(value instanceof String) {
                StringSplitter splitter = new StringSplitter((String) value,
                        delimiter, options);
                Object[] values = splitter.toArray();
                return values.length > 1 ? Transformation.to(key, values)
                        : null;
            }
            else {
                return null;
            }
        };
    }

    /**
     * Return a {@link Transformer} that uses the
     * {@link Convert#stringToJava(String)} method to convert String values to
     * the preferred java type. If the value is not a String, this transformer
     * has no effect.
     * 
     * @return the {@link Transformer}
     */
    public static Transformer valueStringToJava() {
        return (key, value) -> {
            if(value instanceof String) {
                Object converted = Convert.stringToJava((String) value);
                return !converted.equals(value)
                        ? Transformation.to(key, converted)
                        : null;
            }
            else {
                return null;
            }
        };
    }

    private Transformers() {/* no init */}

}
