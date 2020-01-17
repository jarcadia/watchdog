package com.jarcadia.watchdog;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.junit.jupiter.api.Test;

public class UnitTest {

    @Test
    public void test() {
        String value = "2020-01-11T00:39:30+0000";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssxxxx");
        System.out.println(formatter.format(ZonedDateTime.now()));
        System.out.println(ZonedDateTime.parse(value, formatter));
    }

}