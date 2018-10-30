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
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.cinchapi.common.base.CaseFormats;
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
import com.cinchapi.concourse.util.SplitOption;
import com.cinchapi.concourse.util.StringSplitter;
import com.cinchapi.concourse.util.Strings;
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
     * Return a {@link Transformer} that explodes a {@code key}/{@code value}
     * pair into a nested data structure.
     * 
     * @return the transformer
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
     * @return the transformer
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
     * @return the transformer
     */
    public static Transformer keyMap(Map<String, String> map) {
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
     * Return a {@link Transformer} that replaces whitespace characters with an
     * underscore in keys.
     * 
     * @return the {@link Transformer}
     */
    public static Transformer keyWhitespaceToUnderscore() {
        return keyReplaceChars(ImmutableMap.of(' ', '_'));
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
            if(Strings.isWithinQuotes(key)) {
                key = key.substring(1, key.length() - 1);
                modified = true;
            }
            if(value instanceof String
                    && Strings.isWithinQuotes((String) value)) {
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
     * @return the transformer
     */
    public static Transformer nullSafe(Transformer transformer) {
        return (key, value) -> value != null ? transformer.transform(key, value)
                : null;
    }

    public static Transformer valueRemoveIfEmpty() {
        return valueRemoveIf(Empty.ness());
    }

    /**
     * Return a {@link Transformer} that will cause a key/value pair to be
     * "removed" if the value is described by the provided {@code adjective}.
     * <p>
     * Removal is accomplished by returning an empty map for the transformation.
     * </p>
     * 
     * @param adjective
     * @return the transformer
     * @deprecated use {@link #valueRemoveIf(Adjective) instead}
     */
    @Deprecated
    public static Transformer removeValuesThatAre(Adjective adjective) {
        return valueRemoveIf(adjective);
    }

    /**
     * Return a {@link Transformer} that will cause a key/value pair to be
     * "removed" if the value is described by the provided {@code adjective}.
     * <p>
     * Removal is accomplished by returning an empty map for the transformation.
     * </p>
     * 
     * @param adjective
     * @return the transformer
     */
    public static Transformer valueRemoveIf(Adjective adjective) {
        return (key, value) -> adjective.describes(value) ? ImmutableMap.of()
                : null;
    }

    /**
     * Transform values to a {@link Boolean} if possible. If the value cannot be
     * transformed, an exception is thrown.
     * 
     * @return the transformer
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
     * Transform values to a {@link Number} if possible. If the value cannot be
     * transformed, an exception is thrown.
     * 
     * @return the transformer
     */
    public static Transformer valueAsNumber() {
        return (key, value) -> {
            if(value instanceof Number) {
                return null;
            }
            else {
                Number number = Strings.tryParseNumber(value.toString());
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
     * Transform values to a
     * {@link Convert#stringToResolvableLinkInstruction(String) resolvable link
     * instruction}.
     * 
     * @return the transformer
     */
    public static Transformer valueAsResolvableLinkInstruction() {
        return (key, value) -> Transformation.to(key,
                Convert.stringToResolvableLinkInstruction(value.toString()));
    }

    /**
     * Transform values to a {@link Tag} if possible. If the value cannot be
     * transformed, an exception is thrown.
     * 
     * @return the transformer
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
     * Transform values to a {@link Timestamp} if possible. If the value cannot
     * be transformed, an exception is thrown.
     * 
     * @return the transformer
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

    public static Transformer valueNullifyIfEmpty() {
        return valueNullifyIfEmpty(Empty.ness());
    }

    /**
     * Transform a value that is considered to be {@link Empty empty} to
     * {@code null}.
     * 
     * @param empty
     * @return the transformer
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
     * Return a {@link Transformer} that splits a String value into multiple
     * strings that are all mapped from the original key.
     * 
     * @param delimiter the character on which to split the String
     * @param options the optional {@link SplitOption SplitOptions}
     * @return the transformer
     */
    public static Transformer valueSplitOnDelimiter(char delimiter,
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
     * Transform a string value to the proper java type using
     * {@link Convert#stringToJava(String)}.
     * 
     * @return the transformer
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
