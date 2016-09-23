package org.apache.mesos.offer;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.StringTokenizer;

import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.Value;

/**
 * Tools for manipulating
 * @author nick
 *
 */
public class AttributeUtils {

    private static final String ATTRIBUTE_LIST_SEPARATOR = ";";
    private static final char ATTRIBUTE_KEYVAL_SEPARATOR = ':';

    private AttributeUtils() {
        // do not instantiate
    }

    public static List<String> toStringList(String joinedAttributes) {
        StringTokenizer tokenizer = new StringTokenizer(joinedAttributes, ATTRIBUTE_LIST_SEPARATOR);
        List<String> tokens = new ArrayList<>();
        while (tokenizer.hasMoreTokens()) {
            tokens.add(tokenizer.nextToken());
        }
        return tokens;
    }

    /**
     * Converts the provided list of zero or more attributes into a string suitable for parsing by
     * {@link #parseString(String)}.
     *
     * @throws IllegalArgumentException if some part of the provided attributes couldn't be
     * serialized
     */
    public static String toString(List<Attribute> attributes) throws IllegalArgumentException {
        StringJoiner joiner = new StringJoiner(ATTRIBUTE_LIST_SEPARATOR);
        for (Attribute attribute : attributes) {
            joiner.add(toString(attribute));
        }
        return joiner.toString();
    }

    /**
     * Converts the provided attribute into a string which follows the format defined by Mesos:
     * <code>
     * attributes : attribute ( ";" attribute )*
     * attribute : text ":" ( scalar | range | text )
     * text : [a-zA-Z0-9_/.-]
     * scalar : floatValue
     * floatValue : ( intValue ( "." intValue )? ) | ...
     * intValue : [0-9]+
     * range : "[" rangeValue ( "," rangeValue )* "]"
     * rangeValue : scalar "-" scalar
     * set : "{" text ( "," text )* "}"
     * </code>
     *
     * NOTE that it is difficult if not impossible to consistently perform the inverse of this
     * operation. For example, how can you tell if something is supposed to be a SCALAR value or a
     * TEXT value? [0-9.]+ is valid in both cases! Your best hope is to consistently convert to
     * string, and then convert strings...
     *
     * @throws IllegalArgumentException if some part of the provided attributes couldn't be
     * serialized
     */
    public static String toString(Attribute attribute) throws IllegalArgumentException {
        StringBuffer buf = new StringBuffer();
        buf.append(attribute.getName());
        buf.append(ATTRIBUTE_KEYVAL_SEPARATOR);
        switch (attribute.getType()) {
        case RANGES: {
            // "ports:[21000-24000,30000-34000]"
            buf.append('[');
            StringJoiner joiner = new StringJoiner(",");
            for (Value.Range range : attribute.getRanges().getRangeList()) {
                joiner.add(String.format("%d-%d", range.getBegin(), range.getEnd()));
            }
            buf.append(joiner.toString());
            buf.append(']');
            break;
        }
        case SCALAR:
            // mesos.proto: "Mesos keeps three decimal digits of precision ..."
            buf.append(String.format("%.3f", attribute.getScalar().getValue()));
            break;
        case SET:
            // "bugs(debug_role):{a,b,c}"
            buf.append('{');
            StringJoiner joiner = new StringJoiner(",");
            for (String item : attribute.getSet().getItemList()) {
                joiner.add(item);
            }
            buf.append('}');
            break;
        case TEXT:
            buf.append(attribute.getText().getValue());
            break;
        default:
            throw new IllegalArgumentException("Unsupported attribute value type: " + attribute);
        }
        return buf.toString();
    }
}
