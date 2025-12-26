package com.romertec.fsdata.support;

import java.util.Locale;

public final class PriceParser {

    private PriceParser() {}

    /**
     * Normaliza precios tipo:
     *  "15.99 USD"
     *  "3.99 USD"
     * Si viene null/blank, retorna null.
     */
    public static String normalize(String priceRaw) {
        String v = CsvUtils.clean(priceRaw);
        if (v == null) return null;

        // Normalización mínima: espacios simples y moneda en mayúscula si existe
        v = v.replaceAll("\\s+", " ").trim();

        // Si termina en una moneda, la sube a uppercase (USD, COP, etc.)
        String[] parts = v.split(" ");
        if (parts.length >= 2) {
            String amount = parts[0];
            String currency = parts[1].toUpperCase(Locale.ROOT);
            return amount + " " + currency;
        }
        return v;
    }
}
