package org.george0st.helper;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * The pseudo-random number generator with extra seed (local datetime, cpu speed, UUID version 4)
 */
public class RndGenerator {


    private static String allCandidates = "abcdefghijklmnopqrstuvwxyz" +
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890"+
            " ()_-,.";
    private static String stringCandidates = "abcdefghijklmnopqrstuvwxyz" +
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
            "1234567890";
    private static String numberCandidates = "1234567890";

    private SecureRandom rnd=null;

    public RndGenerator() throws InterruptedException {

        // calc based on current CPU speed
        long startTime = System.nanoTime();
        Thread.sleep(3);
        long calcClock = System. nanoTime() - startTime;

        // define sequence for seed init
        byte[] init = String.format("%s,%d,%s", LocalDateTime.now().toString(), calcClock, UUID.randomUUID().toString())
                .getBytes();

        try {
            rnd = SecureRandom.getInstance("SHA1PRNG");
        }
        catch (NoSuchAlgorithmException ex) {
            rnd = new SecureRandom();
        }
        rnd.setSeed(init);
    }

    public String getStringSequence(int length){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i ++)
            sb.append(stringCandidates.charAt(rnd.nextInt(stringCandidates.length())));
        return sb.toString();
    }

    public String getNumberSequence(int length){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i ++)
            sb.append(numberCandidates.charAt(rnd.nextInt(numberCandidates.length())));
        return sb.toString();
    }

    public String getAllSequence(int length){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i ++)
            sb.append(allCandidates.charAt(rnd.nextInt(allCandidates.length())));
        return sb.toString();
    }

    public int getInt(int toNumber){
        return getInt(0, toNumber);
    }


    public int getInt(int fromNumber, int toNumber){
        return rnd.nextInt(fromNumber, toNumber);
    }

    public float getFloat(float toNumber){
        return getFloat(0, toNumber);
    }

    public float getFloat(float fromNumber, float toNumber){
        return rnd.nextFloat(fromNumber, toNumber);
    }

    public double getDouble(double toNumber){
        return getDouble(0, toNumber);
    }

    public double getDouble(double fromNumber, double toNumber){
        return rnd.nextDouble(fromNumber, toNumber);
    }

}
