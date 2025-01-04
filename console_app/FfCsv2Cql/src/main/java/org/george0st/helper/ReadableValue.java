package org.george0st.helper;

import java.util.Locale;

/**
 * Transform value to readable forms e.g. duration in milliseconds to the readable
 * units (number of hours, minutes, seconds, milliseconds, etc.)
 */
public class ReadableValue {

    /**
     * Transformation from seconds to days, hours, minutes and seconds.
     * @param durationSeconds value in seconds
     * @return readable value
     */
    public static String fromSeconds(long durationSeconds){
        if (durationSeconds<0)
            return "n/a";

        StringBuilder details = new StringBuilder();
        long calc;

        calc = durationSeconds / (24 * 60 * 60);
        if (calc > 0)
            details.append(String.format("%d day ", calc));
        durationSeconds %= (24 * 60 * 60);

        calc = durationSeconds / (60 * 60);
        if (calc > 0)
            details.append(String.format("%d hour ", calc));
        durationSeconds %= (60 * 60);

        calc = durationSeconds / 60;
        if (calc > 0)
            details.append(String.format("%d min ", calc));

        durationSeconds %= 60;
        if (durationSeconds > 0)
            details.append(String.format("%d sec ", durationSeconds));

        return details.toString();
    }

    /**
     * Transformation from milliseconds to days, hours, minutes, seconds and milliseconds.
     * @param durationMillisecond value for transformation
     * @return readable value
     */
    public static String fromMillisecond(long durationMillisecond){

        if (durationMillisecond <0)
            return "n/a";

        StringBuilder details = new StringBuilder();
        long calc;

        calc = durationMillisecond / (24 * 60 * 60 * 1000);
        if (calc > 0)
            details.append(String.format("%d day ", calc));
        durationMillisecond %= (24 * 60 * 60 * 1000);

        calc = durationMillisecond / (60 * 60 * 1000);
        if (calc > 0)
            details.append(String.format("%d hour ", calc));
        durationMillisecond %= (60 * 60 * 1000);

        calc = durationMillisecond / (60 * 1000);
        if (calc > 0)
            details.append(String.format("%d min ", calc));
        durationMillisecond %= (60 * 1000);

        calc = durationMillisecond / 1000;
        if (calc > 0)
            details.append(String.format("%d sec ", calc));

        durationMillisecond %= 1000;
        if (durationMillisecond > 0)
            details.append(String.format("%d ms ", durationMillisecond));

        return details.toString();
    }

    /**
     * Transformation from number to value in Giga (G), Mega (M), Kilo (K) and original value.
     * @param number value for transformation
     * @return readable value
     */
    public static String fromNumber(long number) {
        return ReadableValue.fromNumber(number,0);
    }

    /**
     * Transformation from number to value in Giga (G), Mega (M), Kilo (K) and original value.
     * @param number value for transformation
     * @param precision requested precision
     * @return readable value
     */
    public static String fromNumber(long number, int precision){
        if (number >= 1_000_000_000) {
            return String.format(Locale.ENGLISH, "%." + precision + "fG", number / 1_000_000_000.0);
        } else if (number >= 1_000_000) {
            return String.format(Locale.ENGLISH, "%." + precision + "fM", number / 1_000_000.0);
        } else if (number >= 1_000) {
            return String.format(Locale.ENGLISH, "%." + precision + "fK", number / 1_000.0);
        } else {
            return String.valueOf(number);
        }
    }

    /**
     * Transformation from number in bytes to value in Giga (GB), Mega (MB), Kilo (KB) and original value.
     * @param number value for transformation
     * @return readable value
     */
    public static String fromNumberBytes(long number) {
        return ReadableValue.fromNumberBytes(number,0);
    }

    /**
     * Transformation from number in bytes to value in Giga (GB), Mega (MB), Kilo (KB) and original value.
     * @param number value for transformation
     * @param precision requested precision
     * @return readable value
     */
    public static String fromNumberBytes(long number, int precision) {
        if (number >= 1_073_741_824) {
            return String.format(Locale.ENGLISH, "%." + precision + "fGB", number / 1_073_741_824.0);
        } else if (number >= 1_048_576) {
            return String.format(Locale.ENGLISH, "%." + precision + "fMB", number / 1_048_576.0);
        } else if (number >= 1_024) {
            return String.format(Locale.ENGLISH, "%." + precision + "fKB", number / 1_024.0);
        } else {
            return String.format("%dB", number);
        }
    }
}
