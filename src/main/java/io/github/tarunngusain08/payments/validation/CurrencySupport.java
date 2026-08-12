package io.github.tarunngusain08.payments.validation;

import java.util.Currency;
import java.util.Locale;

public final class CurrencySupport {

    private CurrencySupport() {
    }

    public static Currency resolve(String rawCode) {
        if (rawCode == null) {
            throw new IllegalArgumentException("Currency code is required");
        }
        return Currency.getInstance(rawCode.trim().toUpperCase(Locale.ROOT));
    }

    public static boolean isSupported(String rawCode) {
        try {
            resolve(rawCode);
            return true;
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }
}
