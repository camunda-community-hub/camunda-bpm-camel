package org.camunda.bpm.camel.converter;

import org.apache.camel.converter.DurationConverter;

/**
 * This class has been removed in camel-base > 3.4.0, so we emulate the behavior here.
 */
public class TimePatternConverter {

  private TimePatternConverter() {
    throw new UnsupportedOperationException("This class cannot be instantiated");
  }

  public static long toMilliSeconds(String value) {
    return stringToLong(value);
  }

  public static long[] stringToLongs(String values) {
    String[] strings = values.split(",");
    long[] longValues = new long[strings.length];

    for (int i = 0; i < strings.length; ++i) {
      var longValue = stringToLong(strings[i]);
      longValues[i] = longValue;
    }

    return longValues;
  }

  public static long stringToLong(String value) {
    var duration = DurationConverter.toDuration(value);
    return DurationConverter.toMilliSeconds(duration);
  }

}
