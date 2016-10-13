package core.parser;

import org.apache.hadoop.io.Text;

public class NcdcRecordParserMax {

    private static final int MISSING_TEMPERATURE = 9999;

    private String year;
    private int airTemp;
    private boolean airTempMalformed;
    private String quality;

    public void parse(String record) {
        year = record.substring(15, 19);
        airTempMalformed = false;
        // Remove leading plus sign as parseInt doesn't like them (pre-Java 7)
        if (record.charAt(87) == '+') {
            airTemp = Integer.parseInt(record.substring(88, 92));
        } else if (record.charAt(87) == '-') {
            airTemp = Integer.parseInt(record.substring(87, 92));
        } else {
            airTempMalformed = true;
        }
        quality = record.substring(92, 93);
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValidTemp() {
        return !airTempMalformed && airTemp != MISSING_TEMPERATURE
                && quality.matches("[01459]");
    }

    public boolean isMalformedTemp() {
        return airTempMalformed;
    }

    public String getYear() {
        return year;
    }

    public int getAirTemp() {
        return airTemp;
    }

}
