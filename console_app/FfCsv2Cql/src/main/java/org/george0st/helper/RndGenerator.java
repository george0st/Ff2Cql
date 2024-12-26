package org.george0st.helper;

import com.fasterxml.uuid.Generators;

import javax.xml.crypto.Data;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.*;
import java.util.Date;
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

    private static long dateTimeEpochMin = LocalDateTime.parse("1970-01-01T00:00:00").toEpochSecond(ZoneOffset.UTC);
    private static long dateTimeEpochMax = LocalDateTime.parse("2030-12-31T23:59:59").toEpochSecond(ZoneOffset.UTC);

    private static long instantEpochMin= Instant.parse("1970-01-01T00:00:00Z").getEpochSecond();
    private static long instantEpochMax= Instant.parse("2030-12-31T23:59:59Z").getEpochSecond();

    private SecureRandom rnd=null;

    public RndGenerator() throws InterruptedException {
        init();
    }

    private void init() throws InterruptedException {
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

    public UUID getUUID(boolean timeBased){
        return timeBased ? Generators.timeBasedGenerator().generate() : UUID.randomUUID();
    }

    public Boolean getBoolean() {
        return rnd.nextBoolean();
    }

    public Date getDate(Date fromDate, Date toDate){
        return new Date(rnd.nextLong(fromDate.getTime(), toDate.getTime()));
    }

    public LocalTime getLocalTime(){
        //return getLocalTime(LocalTime.parse("00:00:00"),LocalTime.parse("23:59:59"));
        return getLocalTime(LocalTime.MIN,LocalTime.MAX);
    }

    public LocalTime getLocalTime(LocalTime fromTime, LocalTime toTime){
        return LocalTime.ofSecondOfDay(rnd.nextInt(fromTime.toSecondOfDay(), toTime.toSecondOfDay()));
    }

    public LocalDateTime getLocalDateTime(){
        return LocalDateTime.ofEpochSecond(rnd.nextLong(dateTimeEpochMin, dateTimeEpochMax), 0, ZoneOffset.UTC);
    }

    public Instant getInstant(){
        return Instant.ofEpochSecond(rnd.nextLong(instantEpochMin, instantEpochMax));
    }

    public LocalDateTime getLocalDateTime(LocalDateTime fromDateTime, LocalDateTime toDateTime){
        return LocalDateTime.ofEpochSecond(rnd.nextLong(fromDateTime.toEpochSecond(ZoneOffset.UTC), toDateTime.toEpochSecond(ZoneOffset.UTC)), 0,ZoneOffset.UTC);
    }

}
