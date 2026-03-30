package com.fabiodm.pat;

import com.fabiodm.pat.api.PatClient;
import com.fabiodm.pat.api.event.PatEvent;
import com.fabiodm.pat.codec.ByteArrayCodec;
import com.fabiodm.pat.handler.PatHandler;
import com.fabiodm.pat.handler.impl.ConsumerSubscription;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.CompressionCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * This class implements the PatClient interface and provides methods for connecting to a Redis server,
 * sending and receiving messages, and managing listeners for messages.
 */
public final class Pat implements PatClient {

    public static final Logger LOGGER = LoggerFactory.getLogger(Pat.class);

    // The Redis client for connecting to the Redis server.
    private final RedisClient redisClient;
    // The connection to the Redis server.
    private StatefulRedisPubSubConnection<String, byte[]> connection;

    // The compression type for the messages.
    private final CompressionCodec.CompressionType compressionType;

    // The listener for Redis pub/sub messages.
    private final PatListener patListener;

    // The set of listeners for PatEvents.
    private final Map<Class<?>, PatHandler> listeners = new ConcurrentHashMap<>();

    private final boolean isPatRedisClient;

    /**
     * Constructs a Pat object with the given RedisURI and ClientOptions.
     *
     * @param URI     the RedisURI for the Redis server
     * @param options the ClientOptions for the Redis client
     */
    Pat(final RedisURI URI, final ClientOptions options, final CompressionCodec.CompressionType compressionType) {
        this.isPatRedisClient = true;

        this.redisClient = RedisClient.create(URI);
        this.redisClient.setOptions(options);

        this.compressionType = compressionType;

        this.patListener = new PatListener(this);
    }

    Pat(final RedisClient redisClient, final CompressionCodec.CompressionType compressionType) {
        this.isPatRedisClient = false;

        this.redisClient = redisClient;

        this.compressionType = compressionType;

        this.patListener = new PatListener(this);
    }

    @Override
    public void connect() {
        if (!this.isConnected()) {
            final RedisCodec<String, byte[]> codec;
            if (this.compressionType != null) {
                codec = CompressionCodec.valueCompressor(new ByteArrayCodec(), this.compressionType);
            } else {
                codec = new ByteArrayCodec();
            }

            this.connection = this.redisClient.connectPubSub(codec);
            this.connection.addListener(this.patListener);
        }
    }

    @Override
    public void disconnect() {
        if (this.isConnected()) {
            this.connection.removeListener(this.patListener);
            this.connection.close();
        }
    }

    @Override
    public void shutdown() {
        this.disconnect();

        if (this.isPatRedisClient) {
            this.redisClient.shutdown();
        }
    }

    @Override
    public void register(final Object object) {
        final PatHandler patHandler = new PatHandler(object);
        this.listeners.put(object.getClass(), patHandler);
        if (!patHandler.isEmpty()) {
            patHandler.getChannels().forEach(this::subscribe);
        }
    }

    @Override
    public void unregister(final Object object) {
        final PatHandler handler = this.listeners.get(object.getClass());
        if (handler == null) {
            return;
        }

        this.listeners.remove(object.getClass());
        handler.getChannels().forEach(this::unsubscribe);
    }

    @Override
    public void subscribeToChannel(final Object listener,
                                   final String channel,
                                   final Consumer<PatEvent> consumer) {
        final PatHandler handler = this.listeners.get(listener.getClass());
        if (handler == null) {
            return;
        }

        this.subscribe(channel);

        handler.registerSubscription(channel, new ConsumerSubscription(consumer));
    }

    @Override
    public void send(final String channel, final byte[] message) {
        this.connection.sync().publish(channel, message);
    }

    @Override
    public void send(final String channel, final String message) {
        this.send(channel, message.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public RedisFuture<Long> sendAsync(String channel, byte[] message) {
        return this.connection.async().publish(channel, message);
    }

    @Override
    public RedisFuture<Long> sendAsync(String channel, String message) {
        return this.sendAsync(channel, message.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public StatefulRedisPubSubConnection<String, byte[]> getConnection() {
        return connection;
    }

    @Override
    public boolean isConnected() {
        return this.connection != null && this.connection.isOpen();
    }

    /**
     * Subscribes to a Redis pub/sub channel.
     *
     * @param channel the channel to subscribe to
     */
    private void subscribe(final String channel) {
        this.connection.sync().subscribe(channel);
    }

    /**
     * Unsubscribes from a Redis pub/sub channel.
     *
     * @param channel the channel to unsubscribe from
     */
    private void unsubscribe(final String channel) {
        this.connection.sync().unsubscribe(channel);
    }

    /**
     * Broadcasts a PatEvent to all listeners.
     *
     * @param event the PatEvent to broadcast
     */
    public void broadcast(final PatEvent event) {
        this.listeners.values().forEach(handler -> handler.handle(event));
    }
}