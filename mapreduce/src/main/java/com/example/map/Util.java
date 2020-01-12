package com.example.map;

import java.util.HashMap;
import java.util.Map;

public class Util {

    public static Map<String, String> transformCsvToMapForAvailability(String csv) {
        Map<String, String> map = new HashMap<String, String>();
        try {
            String valueString = csv.toString();
            String[] SingleCountryData = valueString.split(",");
            map.put("availability", SingleCountryData[SingleCountryData.length-1].toString());

        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(csv);
        }

        return map;
    }

    public static Map<String, String> transformCsvToMapForNaiberhood(String csv) {
        Map<String, String> map = new HashMap<String, String>();
        try {
            String valueString = csv.toString();
            String[] SingleCountryData = valueString.split(",");
            if(SingleCountryData.length>=5){
                map.put("region", SingleCountryData[4].toString());
            }


        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(csv);
        }

        return map;
    }
}