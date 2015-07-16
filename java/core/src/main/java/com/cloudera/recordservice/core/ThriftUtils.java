// Confidential Cloudera Information: Covered by NDA.
// Copyright 2014 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.recordservice.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;

import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.thrift.TDelegationToken;
import com.cloudera.recordservice.thrift.TProtocolVersion;

/**
 * Utility class to convert from thrift classes to client classes. We should
 * never be returning thrift classes as part of the client API.
 */
public class ThriftUtils {
  private final static Logger LOG = LoggerFactory.getLogger(ThriftUtils.class);

  private final static String KERBEROS_MECHANISM = "GSSAPI";
  private final static String TOKEN_MECHANISM = "DIGEST-MD5";

  static {
    // This is required to allow clients to connect via kerberos. This is called
    // when the kerberos connection is being opened. The option we need is to
    // use the ticketCache.
    // TODO: is this the best way to do this?
    Configuration.setConfiguration(new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        final Map<String, String> options = new HashMap<String, String>();
        options.put("useTicketCache", "true");
        return new AppConfigurationEntry[]{
                new AppConfigurationEntry(
                        "com.sun.security.auth.module.Krb5LoginModule",
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        options)};
      }
    });
  }

  // Callback for DIGEST-MD5 to provide additional client information. We need to
  // implement all 4 of the callbacks used for DIGEST-MD5 although we are only
  // interested in the user and password.
  private static final class DigestHandler implements CallbackHandler {
    private final TDelegationToken token_;

    public DigestHandler(TDelegationToken token) {
      assert token != null;
      token_ = token;
    }

    @Override
    public void handle(Callback[] callbacks)
        throws IOException, UnsupportedCallbackException {
      for (Callback cb : callbacks) {
        if (cb instanceof RealmChoiceCallback) {
          continue; // Ignore.
        } else if (cb instanceof NameCallback) {
          ((NameCallback)cb).setName(token_.identifier);
        } else if (cb instanceof PasswordCallback) {
          PasswordCallback pcb = ((PasswordCallback)cb);
          if (token_.password == null) {
            pcb.setPassword(null);
          } else {
            pcb.setPassword(token_.password.toCharArray());
          }
        } else if (cb instanceof RealmCallback) {
          RealmCallback rcb = (RealmCallback)cb;
          rcb.setText(rcb.getDefaultText());
        } else {
          throw new UnsupportedCallbackException(cb, "Unexpected DIGEST-MD5 callback");
        }
      }
    }
  }

  /**
   * Connects to a thrift service running at hostname/port, returning a TTransport
   * object to that service. If kerberosPrincipal is not null, the connection will
   * be kerberized. If delegationToken is not null, we will authenticate using
   * delegation tokens.
   */
  protected static TTransport createTransport(String service, String hostname, int port,
      String kerberosPrincipal, TDelegationToken token, int timeoutMs) throws IOException {
    if (kerberosPrincipal != null && token != null) {
      throw new IllegalArgumentException(
          "Cannot specify both kerberos principal and delegation token.");
    }

    TTransport transport = new TSocket(hostname, port, timeoutMs);

    if (kerberosPrincipal != null) {
      LOG.info(String.format(
          "Connecting to %s at %s:%d with kerberos principal: %s, with timeout: %sms",
          service, hostname, port, kerberosPrincipal, timeoutMs));

      // Kerberized, wrap the transport in a sasl transport.
      String[] names = kerberosPrincipal.split("[/@]");
      if (names.length != 3) {
        throw new IllegalArgumentException("Kerberos principal should have 3 parts: "
            + kerberosPrincipal);
      }
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
      Map<String, String> saslProps = new HashMap<String, String>();
      saslProps.put(Sasl.SERVER_AUTH, "true");
      transport = new TSaslClientTransport(KERBEROS_MECHANISM, null,
          names[0], names[1], saslProps, null, transport);
    } else if (token != null) {
      LOG.info(String.format(
          "Connecting to %s at %s:%d using delegation token, with timeout: %sms",
          service, hostname, port, timeoutMs));

      // Delegation token, wrap the transport in a sasl transport.
      CallbackHandler callback = new DigestHandler(token);
      transport = new TSaslClientTransport(TOKEN_MECHANISM, null,
          "impala", "default",  new HashMap<String, String>(), callback, transport);
    } else {
      LOG.info(String.format("Connecting to %s at %s:%d, with timeout: %sms",
          service, hostname, port, timeoutMs));
    }

    try {
      transport.open();
    } catch (TTransportException e) {
      String msg = String.format("Could not connect to %s: %s:%d",
          service, hostname, port);
      LOG.warn(String.format("%s: error: %s", msg, e));
      if (e.getMessage() != null && e.getMessage().contains("SocketTimeoutException") &&
          (kerberosPrincipal != null || token != null)) {
        // If connecting from a secure connection to a non-secure server, the
        // connection seems to just hang and the attempt fails with a timeout.
        // This is because the client is expecting the server to participate
        // in the handshake which it is not.
        // This is a heuristic (there are other reasons it can time out) but
        // likely helpful.
        // TODO: is it possible to deal with this in the server?
        msg += " Attempting to connect with a secure connection. " +
            "Ensure the server has security enabled.";
      }
      throw new IOException(msg, e);
    }

    LOG.info(String.format("Connected to %s at %s:%d", service, hostname, port));
    return transport;
  }

  protected static ProtocolVersion fromThrift(TProtocolVersion v) {
    switch (v) {
    case V1: return ProtocolVersion.V1;
    default:
      // TODO: is this right for mismatched versions? Default to a lower
      // version?
      throw new RuntimeException("Unrecognized version: " + v);
    }
  }
}
