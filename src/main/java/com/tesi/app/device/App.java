package com.tesi.app.device;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.DittoClients;
import org.eclipse.ditto.client.configuration.BasicAuthenticationConfiguration;
import org.eclipse.ditto.client.configuration.ClientCredentialsAuthenticationConfiguration;
import org.eclipse.ditto.client.configuration.DummyAuthenticationConfiguration;
import org.eclipse.ditto.client.configuration.MessagingConfiguration;
import org.eclipse.ditto.client.configuration.ProxyConfiguration;
import org.eclipse.ditto.client.configuration.TrustStoreConfiguration;
import org.eclipse.ditto.client.configuration.WebSocketMessagingConfiguration;
import org.eclipse.ditto.client.messaging.AuthenticationProvider;
import org.eclipse.ditto.client.messaging.AuthenticationProviders;
import org.eclipse.ditto.client.messaging.MessagingProvider;
import org.eclipse.ditto.client.messaging.MessagingProviders;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.Payload;
import org.eclipse.ditto.protocol.TopicPath;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.neovisionaries.ws.client.WebSocket;


/**
 * Hello world!
 *
 */
public class App 
{
	private static final String PROPERTIES_FILE = "ditto-client-starter-local.properties";
	private static final Logger LOGGER = LogManager.getLogger(App.class);
	 	
	private static final String PROXY_HOST;
	private static final String PROXY_PORT;
	private static final String DITTO_ENDPOINT_URL;

	private static final URL DITTO_TRUSTSTORE_LOCATION;
	private static final String DITTO_TRUSTSTORE_PASSWORD;
	private static final String DITTO_DUMMY_AUTH_USER;
	private static final String DITTO_USERNAME;
	private static final String DITTO_PASSWORD;
	private static final String DITTO_OAUTH_CLIENT_ID;
	private static final String DITTO_OAUTH_CLIENT_SECRET;
	private static final Collection<String> DITTO_OAUTH_SCOPES;
	private static final String DITTO_OAUTH_TOKEN_ENDPOINT;
	private static final String NAMESPACE;
	private static final Properties CONFIG;
	
    public static void main( String[] args )  throws ExecutionException, InterruptedException, IOException 
    {
    	Scripts mqtt = new Scripts();
        LOGGER.info("Avvio applicazione");
        final DittoClient client = DittoClients.newInstance(createMessagingProvider()).connect().toCompletableFuture()
				.join();
    	client.live().startConsumption().toCompletableFuture().join();
        LOGGER.info("Creazione istanza client ditto avvenuta con successo!");
        
    	final String namespace = CONFIG.getProperty("ditto.search.namespace", CONFIG.getProperty("ditto.namespace"));
		final String thing = CONFIG.getProperty("ditto.thing.id");
		final String id = namespace + ":" + thing;
        getDigitalThing(client, id);
		LOGGER.info("Caricamento digitalTwin completata!");
		
		Status status = new Status(client, id);
		status.run();
		
		useLiveMessages(client, mqtt, id);
		
		System.out.println("\n\nFinished with LIVE messages demo");
		Thread.sleep(500);
		
		client.destroy();
		System.out.println("\n\nDittoClientUsageExamples successfully completed!");
		System.exit(0);
    }
    
	private static void promptEnterKey() throws InterruptedException {
		if (promptToContinue()) {
			Thread.sleep(500);
			System.out.println("Press \"ENTER\" to continue...");
			final Scanner scanner = new Scanner(System.in);
			scanner.nextLine();
		}
	}
	

	private static void useLiveMessages(final DittoClient clientAtDevice, Scripts mqtt, String id)
			throws InterruptedException, ExecutionException, IOException {
		
		ObjectMapper objectMapper = new ObjectMapper();
		LOGGER.info("[AT DEVICE] Registrazione in corso per ricevere messaggi...");

		clientAtDevice.live().registerForMessage("globalMessageHandler", "*", message -> {
			LOGGER.info("[AT DEVICE] Ricevuto messaggio con soggetto '{}' dal client: {}", message.getSubject(),
					message.getExtra());
			Optional<?> op = message.getPayload();
			
			LOGGER.info("MESSAGGIO: " + op.orElse(null));
			String messaggioParser = op.orElse(null).toString().replace("\\", "");
			
			try {
				JsonNode messagePayload = null;
				if (!message.getSubject().equals("fileUpload")) {
					messagePayload = objectMapper.readTree(messaggioParser.substring(1, messaggioParser.length() - 1));
				}
				JsonNode thingRappresentation = objectMapper.readTree(CONFIG.getProperty("ditto.digital.thing"));
				int countCoffee = thingRappresentation.findValue("brewed-coffees").asInt();
				if (message.getSubject().equals("makeCoffee")) {
					if (messagePayload.get("cups") != null) {
						int cups = messagePayload.get("cups").asInt();
						final Adaptable modifyFeatureDesiredProperties = Adaptable
								.newBuilder(TopicPath.newBuilder(ThingId.of(id)).things()
										.twin().commands().modify().build())
								.withPayload(
										Payload.newBuilder(JsonPointer.of("/features/coffee-brewer/properties"))
												.withValue(JsonObject.newBuilder()
														.set("brewed-coffees", countCoffee + cups).build())
												.build())
								.build();
						clientAtDevice.sendDittoProtocol(modifyFeatureDesiredProperties)
								.whenComplete(((adaptable, throwable) -> {
									if (throwable != null) {
										LOGGER.error(
												"Ricevuto un errore mentre si modificava la proprietà desiderata: '{}' ",
												throwable.toString());
									} else {
										LOGGER.info("proprietà modificata con successo: '{}'", adaptable);
										getDigitalThing(clientAtDevice, id);
									}
								}));
					}
				}
				if (message.getSubject().equals("fileUpload")) {
					System.out.println( mqtt.isSkip());
					if (mqtt.p != null && mqtt.p.isAlive() && !mqtt.isSkip()) {
						mqtt.setSkip(true);
						mqtt.stop();
					}
					Optional<ByteBuffer> buffer = message.getRawPayload();
					File file = new File("FirmwareNodeJs.zip");

					FileOutputStream fos = null;
					boolean append = false;

					FileChannel channel;
					try {
						channel = new FileOutputStream(file, append).getChannel();
						// Writes a sequence of bytes to this channel from the given buffer.
						channel.write(buffer.get());
						// close the channel
						channel.close();
						if (!channel.isOpen()) {
							String baseDir = CONFIG.getProperty("ditto.base.firmware.dir");
							if (mqtt.estractFileTar(baseDir)) {
								LOGGER.info("Estrazione del file avvenuta con successo!");
								int frequencyUpdate = thingRappresentation.findValue("frequencyUpdate").asInt();
								String urlBroker = thingRappresentation.findValue("urlBroker").asText();
								if (mqtt.p != null && mqtt.p.isAlive()) {
									mqtt.setSkip(false);
									final Adaptable modifyFrequencyProperties = setMqttProperty(id,urlBroker,frequencyUpdate, mqtt.getPid());
									clientAtDevice.sendDittoProtocol(modifyFrequencyProperties)
											.whenComplete(((adaptable, throwable) -> {
												if (throwable != null) {
													LOGGER.error(
															"Ricevuto un errore mentre si modificava la proprietà desiderata: '{}' ",
															throwable.toString());
												} else {
													LOGGER.info("proprietà modificata con successo: '{}'", adaptable);
													getDigitalThing(clientAtDevice, id);
													message.reply().httpStatus(HttpStatus.ACCEPTED)
															.payload("{\"status\":200}").send();
												}
											}));
								} else {
									if (mqtt.exec(urlBroker, frequencyUpdate)) {
										message.reply().httpStatus(HttpStatus.ACCEPTED).payload("{\"status\":200}")
												.send();
									}

								}
							}
						}
					} catch (FileNotFoundException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				if (message.getSubject().equals("mqttStart")) {
					if (mqtt.p != null && mqtt.p.isAlive()) {
						System.out.println("processo già in esecuzione cpn pid:");
						System.out.println("PID: " + mqtt.getPid());
						int frequencyUpdate = thingRappresentation.findValue("frequencyUpdate").asInt();
						String urlBroker = thingRappresentation.findValue("urlBroker").asText();
						final Adaptable modifyFrequencyProperties = setMqttProperty(id,urlBroker,frequencyUpdate, mqtt.getPid());
						clientAtDevice.sendDittoProtocol(modifyFrequencyProperties)
								.whenComplete(((adaptable, throwable) -> {
									if (throwable != null) {
										LOGGER.error(
												"Ricevuto un errore mentre si modificava la proprietà desiderata: '{}' ",
												throwable.toString());
									} else {
										LOGGER.info("proprietà modificata con successo: '{}'", adaptable);
										getDigitalThing(clientAtDevice, id);
										message.reply().httpStatus(HttpStatus.ACCEPTED).payload("{\"status\":200}")
												.send();
									}
								}));
						message.reply().httpStatus(HttpStatus.ACCEPTED)
								.payload("{\"status\":200,\"pid\":" + mqtt.getPid() + "}").send();
					} else {
						int frequencyUpdate = thingRappresentation.findValue("frequencyUpdate").asInt();
						String urlBroker = thingRappresentation.findValue("urlBroker").asText();
						if (mqtt.exec(urlBroker, frequencyUpdate)) {
							message.reply().httpStatus(HttpStatus.ACCEPTED).payload("{\"status\":200}").send();
						}
					}
				}
				if (message.getSubject().equals("mqttUpdate")) {
					if (mqtt.p != null && mqtt.p.isAlive()) {
						mqtt.stop();
					}
					int frequencyUpdate = messagePayload.get("frequencyUpdate").asInt();
					String urlBroker = messagePayload.get("urlBroker").asText();
					String pid = messagePayload.get("pid") != null ? messagePayload.get("pid").asText() : "-1";
					ObjectNode jsonData = objectMapper.createObjectNode();
					jsonData.put("frequencyUpdate", frequencyUpdate);
					jsonData.put("urlBroker", urlBroker);
					final Adaptable modifyFrequencyProperties = setMqttProperty(id,urlBroker,frequencyUpdate, pid);
					clientAtDevice.sendDittoProtocol(modifyFrequencyProperties)
							.whenComplete(((adaptable, throwable) -> {
								if (throwable != null) {
									LOGGER.error(
											"Ricevuto un errore mentre si modificava la proprietà desiderata: '{}' ",
											throwable.toString());
								} else {
									LOGGER.info("proprietà modificata con successo: '{}'", adaptable);
									getDigitalThing(clientAtDevice, id);
									message.reply().httpStatus(HttpStatus.ACCEPTED).payload("{\"status\":200}").send();
								}
							}));
					// mqtt.exec(urlBroker, frequencyUpdate);
				}
				if (message.getSubject().equals("mqttStop")) {
					if (mqtt.stop()) {
						int frequencyUpdate = thingRappresentation.findValue("frequencyUpdate").asInt();
						String urlBroker = thingRappresentation.findValue("urlBroker").asText();
						final Adaptable modifyFrequencyProperties = setMqttProperty(id,urlBroker,frequencyUpdate, mqtt.getPid());
						clientAtDevice.sendDittoProtocol(modifyFrequencyProperties)
								.whenComplete(((adaptable, throwable) -> {
									if (throwable != null) {
										LOGGER.error(
												"Ricevuto un errore mentre si modificava la proprietà desiderata: '{}' ",
												throwable.toString());
									} else {
										LOGGER.info("proprietà modificata con successo: '{}'", adaptable);
										getDigitalThing(clientAtDevice, id);
										message.reply().httpStatus(HttpStatus.ACCEPTED).payload("{\"status\":200}")
												.send();
									}
								}));
						message.reply().httpStatus(HttpStatus.ACCEPTED).payload("{\"status\":200}").send();
					}
				}
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			message.reply().httpStatus(HttpStatus.ACCEPTED).payload("{\"status\":200}").send();
		});

		LOGGER.info("[AT DEVICE] registrato per ricevere messaggi...");
		promptEnterKey();

	}

	
	private static Adaptable setMqttProperty(String thingId, String urlBroker, int frequencyUpdate, String pid) {
		Adaptable modifyFrequencyProperties = Adaptable
				.newBuilder(TopicPath.newBuilder(ThingId.of(thingId))
					.things()
					.twin()
					.commands()
					.modify()
					.build())
				.withPayload(Payload
						.newBuilder(JsonPointer.of("/attributes/mqttConfiguration"))
						.withValue(JsonObject.newBuilder()
								.set("urlBroker", urlBroker)
								.set("frequencyUpdate", frequencyUpdate)
								.set("pid", pid).build())
						.build())
				.build();
		return modifyFrequencyProperties;
	}
	
	private static Adaptable setStatusProperty(String thingId, String status) {
		
	    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
	    System.out.println(timestamp.getTime());
	    
		Adaptable modifyFrequencyProperties = Adaptable
				.newBuilder(TopicPath.newBuilder(ThingId.of(thingId))
					.things()
					.twin()
					.commands()
					.modify()
					.build())
				.withPayload(Payload
						.newBuilder(JsonPointer.of("/attributes/status"))
						.withValue(JsonObject.newBuilder()
								.set("status", status)
								.set("lastUpdate", timestamp.getTime()).build())
						.build())
				.build();
		return modifyFrequencyProperties;
	}
	
	private static boolean promptToContinue() {
		return Boolean.parseBoolean(CONFIG.getProperty("prompt.to.continue", "true"));
	}
	
	public static void getDigitalThing(final DittoClient client, String id) {
		final String namespace = CONFIG.getProperty("ditto.search.namespace", CONFIG.getProperty("ditto.namespace"));
		final String rql1 = "like(thingId,\"" + id + "\")";
		final String options1 = "sort(-thingId),size(1)";
		LOGGER.info("Avvio ricerca thing <{}> con opzioni <{}>...", rql1, options1);
		client.twin().search().stream(searchQueryBuilder -> searchQueryBuilder.namespace(namespace).filter(rql1)
				.options(builder -> builder.sort(s -> s.desc("thingId")).size(1)).initialDemand(1).demand(1)
		).forEach(thing -> {
			LOGGER.info("Ricervuto Thing: <{}>", thing.toJson());
			CONFIG.setProperty("ditto.digital.thing", thing.toJson().toString());
		});
		LOGGER.info("Thing correttamente caricato.\n");
	}
	
	/**
	 * Create a messaging provider according to the configuration.
	 *
	 * @return the messaging provider.
	 */
	public static MessagingProvider createMessagingProvider() {
		final MessagingConfiguration.Builder builder = WebSocketMessagingConfiguration.newBuilder()
				.endpoint(DITTO_ENDPOINT_URL).jsonSchemaVersion(JsonSchemaVersion.V_2).reconnectEnabled(false);

		final ProxyConfiguration proxyConfiguration;
		if (PROXY_HOST != null && !PROXY_HOST.isEmpty()) {
			proxyConfiguration = ProxyConfiguration.newBuilder().proxyHost(PROXY_HOST)
					.proxyPort(Integer.parseInt(PROXY_PORT)).build();
			builder.proxyConfiguration(proxyConfiguration);
		} else {
			proxyConfiguration = null;
		}

		if (DITTO_TRUSTSTORE_LOCATION != null) {
			builder.trustStoreConfiguration(TrustStoreConfiguration.newBuilder().location(DITTO_TRUSTSTORE_LOCATION)
					.password(DITTO_TRUSTSTORE_PASSWORD).build());
		}

		final AuthenticationProvider<WebSocket> authenticationProvider;
		if (DITTO_DUMMY_AUTH_USER != null) {
			authenticationProvider = AuthenticationProviders
					.dummy(DummyAuthenticationConfiguration.newBuilder().dummyUsername(DITTO_DUMMY_AUTH_USER).build());
		} else if (DITTO_OAUTH_CLIENT_ID != null && !DITTO_OAUTH_CLIENT_ID.isEmpty()) {
			final ClientCredentialsAuthenticationConfiguration.ClientCredentialsAuthenticationConfigurationBuilder authenticationConfigurationBuilder = ClientCredentialsAuthenticationConfiguration
					.newBuilder().clientId(DITTO_OAUTH_CLIENT_ID).clientSecret(DITTO_OAUTH_CLIENT_SECRET)
					.scopes(DITTO_OAUTH_SCOPES).tokenEndpoint(DITTO_OAUTH_TOKEN_ENDPOINT);
			if (proxyConfiguration != null) {
				authenticationConfigurationBuilder.proxyConfiguration(proxyConfiguration);
			}
			authenticationProvider = AuthenticationProviders
					.clientCredentials(authenticationConfigurationBuilder.build());
		} else {
			authenticationProvider = AuthenticationProviders.basic(BasicAuthenticationConfiguration.newBuilder()
					.username(DITTO_USERNAME).password(DITTO_PASSWORD).build());
		}

		return MessagingProviders.webSocket(builder.build(), authenticationProvider);
	}
    
    static {
		try {
			final Properties config = new Properties();
			if (new File(PROPERTIES_FILE).exists()) {
				config.load(new FileReader(PROPERTIES_FILE));
			} else {
				final InputStream i = Thread.currentThread().getContextClassLoader()
						.getResourceAsStream(PROPERTIES_FILE);
				config.load(i);
				i.close();
			}

			PROXY_HOST = config.getProperty("proxy.host");
			PROXY_PORT = config.getProperty("proxy.port");

			DITTO_ENDPOINT_URL = config.getProperty("ditto.endpoint");

			if (!config.getProperty("ditto.truststore.location").isEmpty()) {
				DITTO_TRUSTSTORE_LOCATION = App.class
						.getResource(config.getProperty("ditto.truststore.location"));
			} else {
				DITTO_TRUSTSTORE_LOCATION = null;
			}
			DITTO_TRUSTSTORE_PASSWORD = config.getProperty("ditto.truststore.password");
			DITTO_DUMMY_AUTH_USER = config.getProperty("ditto.dummy-auth-user");
			DITTO_USERNAME = config.getProperty("ditto.username");
			DITTO_PASSWORD = config.getProperty("ditto.password");
			DITTO_OAUTH_CLIENT_ID = config.getProperty("ditto.oauth.client-id");
			DITTO_OAUTH_CLIENT_SECRET = config.getProperty("ditto.oauth.client-secret");
			DITTO_OAUTH_SCOPES = Arrays.stream(config.getProperty("ditto.oauth.scope").split(" "))
					.collect(Collectors.toSet());
			DITTO_OAUTH_TOKEN_ENDPOINT = config.getProperty("ditto.oauth.token-endpoint");
			NAMESPACE = config.getProperty("ditto.namespace");
			CONFIG = config;
		} catch (final IOException e) {
			throw new IllegalStateException(e);
		}
	}
}
