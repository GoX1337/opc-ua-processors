package org.gox.nifi.opcua.processors.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

public class DateUtils {

    public static LocalDateTime convert(Date date) {
        return new Date(date.getTime()).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
    }
}
