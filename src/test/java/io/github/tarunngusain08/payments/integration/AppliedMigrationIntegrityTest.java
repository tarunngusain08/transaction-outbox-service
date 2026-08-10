package io.github.tarunngusain08.payments.integration;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AppliedMigrationIntegrityTest {

    private static final String MIGRATION_ROOT = "db/migration/";

    /*
     * SHA-256 digests from artifact commit 1d9d97c. These migrations may already
     * be recorded in flyway_schema_history and therefore must remain byte exact.
     */
    private static final Map<String, String> APPLIED_M08_MIGRATIONS = Map.of(
            "V1__create_transactions.sql",
            "bd08293f3ae9bc04228f9a91438687b41afe35abd827550be88cbe087a27a470",
            "V2__create_outbox_events.sql",
            "b2b6bf6788fd467a82007907733b36726d70802addfe593d546c7b58a2bb1ebc",
            "V3__harden_transaction_invariants.sql",
            "227efedf8358e14b36fd32ae4a65ab3ff78388ab94952ec6d4e0a9ba3d4e768b",
            "V4__add_outbox_claim_leases.sql",
            "2085cd412b78470f01e313d037054b54c3a52ad4fc71e43b84c97298eb900bb2",
            "V5__optimize_outbox_claim_indexes.sql",
            "bee450f8b0bf0cabad793a13ea68b14a26feec32a1f8e4511d9f9a20775a02c1",
            "V6__recover_terminal_outbox_events.sql",
            "1a8b4bfc821651b171d949792a62e13b84980c4205434d45c60ae3b492e6d608"
    );

    @Test
    void appliedM08MigrationsRemainByteForByteIdentical() throws Exception {
        for (var migration : APPLIED_M08_MIGRATIONS.entrySet()) {
            assertThat(sha256(MIGRATION_ROOT + migration.getKey()))
                    .as("SHA-256 of applied migration %s", migration.getKey())
                    .isEqualTo(migration.getValue());
        }
    }

    private String sha256(String resource) throws IOException, NoSuchAlgorithmException {
        try (var input = getClass().getClassLoader().getResourceAsStream(resource)) {
            assertThat(input).as("migration resource %s", resource).isNotNull();
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(input.readAllBytes())
            );
        }
    }
}
