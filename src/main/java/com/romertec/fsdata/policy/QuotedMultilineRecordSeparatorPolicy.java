package com.romertec.fsdata.policy;

import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;

public  class QuotedMultilineRecordSeparatorPolicy implements RecordSeparatorPolicy {

    @Override
    public boolean isEndOfRecord(String record) {
        return countQuotes(record) % 2 == 0;
    }

    @Override
    public String postProcess(String record) {
        // opcional: normalizar saltos de línea internos
        return record;
    }

    @Override
    public String preProcess(String record) {
        return record;
    }

    private int countQuotes(String s) {
        if (s == null) return 0;
        int count = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == '"') count++;
        }
        return count;
    }
}