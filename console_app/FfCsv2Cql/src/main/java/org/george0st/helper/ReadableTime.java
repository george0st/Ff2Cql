package org.george0st.helper;

public class ReadableTime {

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
}
