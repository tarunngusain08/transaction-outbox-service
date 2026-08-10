package io.github.tarunngusain08.payments.validation;

import java.util.Locale;

import static io.github.tarunngusain08.payments.transaction.TransactionContract.INR;

public final class CurrencySupport {

    private CurrencySupport() {
    }

    public static String canonicalizeLegacy(String rawCode) {
        if (rawCode == null || !INR.equals(rawCode.trim().toUpperCase(Locale.ROOT))) {
            throw new IllegalArgumentException("Version 1 supports only INR");
        }
        return INR;
    }

    public static boolean isSupported(String rawCode) {
        try {
            return INR.equals(canonicalizeLegacy(rawCode));
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }
}
