package com.romertec.fsdata.policy;


import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;

public class QuoteBalancedRecordSeparatorPolicy implements RecordSeparatorPolicy {

    private final char quoteChar;

    public QuoteBalancedRecordSeparatorPolicy() {
        this('"');
    }

    public QuoteBalancedRecordSeparatorPolicy(char quoteChar) {
        this.quoteChar = quoteChar;
    }

    @Override
    public boolean isEndOfRecord(String record) {
        // Fin de record cuando NO estamos dentro de comillas al terminar el texto acumulado
        boolean inQuotes = false;

        for (int i = 0; i < record.length(); i++) {
            char c = record.charAt(i);

            if (c == quoteChar) {
                // Manejo de comillas escapadas en CSV: "" dentro de un campo quoted
                if (inQuotes && i + 1 < record.length() && record.charAt(i + 1) == quoteChar) {
                    i++; // saltar la comilla escapada
                } else {
                    inQuotes = !inQuotes;
                }
            }
        }
        return !inQuotes;
    }

    @Override
    public String preProcess(String record) {
        return record;
    }

    @Override
    public String postProcess(String record) {
        return record;
    }
}

