package org.apache.kafka.common.security.plain;

import java.security.Provider;
import java.security.Security;

import org.apache.kafka.common.security.plain.PlainSaslServer.PlainSaslServerFactory;

public class PlainSaslServerProvider extends Provider {

    private static final long serialVersionUID = 1L;

    protected PlainSaslServerProvider() {
        super("Simple SASL/PLAIN Server Provider", 1.0, "Simple SASL/PLAIN Server Provider for Kafka");
        super.put("SaslServerFactory." + PlainSaslServer.PLAIN_MECHANISM, PlainSaslServerFactory.class.getName());
    }

    public static void initialize() {
        Security.addProvider(new PlainSaslServerProvider());
    }
}
