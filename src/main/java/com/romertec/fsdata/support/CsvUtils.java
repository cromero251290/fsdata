package com.romertec.fsdata.support;

import org.apache.commons.text.StringEscapeUtils;

import java.util.Locale;

public final class CsvUtils {

    private CsvUtils() {}

    public static String clean(String s) {
        if (s == null) return null;
        String v = StringEscapeUtils.unescapeHtml4(s).trim();
        return v.isEmpty() ? null : v;
    }

    public record AddressParts(String street, String city, String state, String zip, String unit) {}

    /**
     * Espera formatos tipo:
     *   "224 Daniel Payne Drive, Birmingham, AL, 35207"
     *   "1024 20th Street South Unit 101, Birmingham, AL, 35205"
     */
    public static AddressParts parseFullAddress(String fullAddressRaw, String zipFallbackRaw) {
        String fullAddress = clean(fullAddressRaw);
        String zipFallback = clean(zipFallbackRaw);

        if (fullAddress == null) {
            return new AddressParts(null, null, null, zipFallback, null);
        }

        String[] parts = fullAddress.split(",");
        String street = parts.length > 0 ? clean(parts[0]) : null;
        String city   = parts.length > 1 ? clean(parts[1]) : null;
        String state  = parts.length > 2 ? clean(parts[2]) : null;
        String zip    = parts.length > 3 ? extractZip(clean(parts[3])) : zipFallback;

        // Unit parsing básico desde street (si viene embebido)
        String unit = null;
        if (street != null) {
            String lower = street.toLowerCase(Locale.ROOT);
            int idx = lower.indexOf(" unit ");
            if (idx >= 0) {
                unit = clean(street.substring(idx + " unit ".length()));
                street = clean(street.substring(0, idx));
            }
        }

        return new AddressParts(street, city, state, zip, unit);
    }

    private static String extractZip(String s) {
        if (s == null) return null;
        // keep digits only (ej: "35207" o "35207-1234")
        String v = s.trim();
        if (v.isEmpty()) return null;
        return v;
    }
}
