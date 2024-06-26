/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cip.server;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.util.Unsafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Locale;

/**
 * An Avatica server for arbitrary JDBC drivers.
 */
public class CIPStandaloneServer {
  private static final Logger LOG = LoggerFactory.getLogger(CIPStandaloneServer.class);

  @Parameter(names = { "-u", "--url" }, required = true,
      description = "JDBC driver url for the server")
  private String url;

  @Parameter(names = { "-p", "--port" }, required = false,
      description = "Port the server should bind")
  private int port = 0;

  @Parameter(names = { "-s", "--serialization" }, required = false,
      description = "Serialization method to use", converter = SerializationConverter.class)
  private Serialization serialization = Serialization.PROTOBUF;

  @Parameter(names = { "-h", "-help", "--help" }, required = false, help = true,
      description = "Print the help message")
  private boolean help = false;

  @Parameter(names = { "--principal" }, required = false,
      description = "Kerberos principal (optiona)")
  private String kerberosPrincipal = null;

  @Parameter(names = { "--keytab" }, required = false,
      description = "Kerberos keytab (optional)", converter = ToFileConverter.class)
  private File kerberosKeytab = null;

  private HttpServer server;

  public void start() {
    if (null != server) {
      LOG.error("The server was already started");
      Unsafe.systemExit(ExitCodes.ALREADY_STARTED.ordinal());
      return;
    }

    try {

      LOG.info("Starting Avatica server with URL {}", url);
      JdbcMeta meta = new JdbcMeta(url, "admin", "admin");
      LocalService service = new LocalService(meta);

      // Construct the server
      HttpServer.Builder builder = new HttpServer.Builder()
          .withHandler(service, serialization)
          .withPort(9787);

      if (kerberosPrincipal != null && kerberosKeytab != null) {
        System.out.println("Configuring Avatica to use SPENGO");
        builder.withSpnego(kerberosPrincipal)
            .withAutomaticLogin(kerberosKeytab);
      } else {
        System.out.println("Not configuring Avatica authentication");
      }

      server = builder.build();

      // Then start it
      server.start();
      LOG.info("Started Avatica server on port {} with serialization {}", server.getPort(),
          serialization);

    } catch (Exception e) {
      LOG.error("Failed to start Avatica server", e);
      Unsafe.systemExit(ExitCodes.START_FAILED.ordinal());
    }
  }

  public void stop() {
    if (null != server) {
      server.stop();
      server = null;
    }
  }

  public void join() throws InterruptedException {
    server.join();
  }

  public static void main(String[] args) {
    final CIPStandaloneServer server = new CIPStandaloneServer();
    JCommander jc = new JCommander(server);
    jc.parse(args);
    if (server.help) {
      jc.usage();
      Unsafe.systemExit(ExitCodes.USAGE.ordinal());
      return;
    }

    server.start();

    // Try to clean up when the server is stopped.
    Runtime.getRuntime().addShutdownHook(
        new Thread(new Runnable() {
          @Override public void run() {
            LOG.info("Stopping server");
            server.stop();
            LOG.info("Server stopped");
          }
        }));

    try {
      server.join();
    } catch (InterruptedException e) {
      // Reset interruption
      Thread.currentThread().interrupt();
      // And exit now.
      return;
    }
  }

  /**
   * Converter from String to Serialization. Must be public for JCommander.
   */
  public static class SerializationConverter implements IStringConverter<Serialization> {
    @Override public Serialization convert(String value) {
      return Serialization.valueOf(value.toUpperCase(Locale.ROOT));
    }
  }

  /**
   * Converter from String to a File.
   */
  public static class ToFileConverter implements IStringConverter<File> {
    @Override public File convert(String value) {
      File f = new File(value);
      if (!f.isFile()) {
        String msg = "Kerberos keytab '" + value + "' appears to not be a file";
        throw new IllegalArgumentException(msg);
      }
      return f;
    }
  }

  /**
   * Codes for exit conditions
   */
  private enum ExitCodes {
    NORMAL,
    ALREADY_STARTED, // 1
    START_FAILED,    // 2
    USAGE;           // 3
  }
}

// End StandaloneServer.java
