package com.baeldung

import io.nats.client.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import com.sun.xml.internal.ws.api.message.AddressingUtils.getReplyTo
import io.nats.client.SyncSubscription




class NatsClient {

    private var serverURI: String = "jnats://localhost:4222"

    private var natsConnection: Connection? = null

    private var subscriptions: Map<String, Subscription> = HashMap()

    private val log: Logger = LoggerFactory.getLogger(NatsClient::class.java)

    init {
        natsConnection = initConnection(serverURI)
    }

    fun closeConnection() {
        natsConnection!!.close()
    }

    private fun initConnection(uri: String): Connection? {



        return try {
            val options = Options.Builder()
                .errorCb { ex -> log.error("Connection Exception: ", ex) }
                .disconnectedCb { event -> log.error("Channel disconnected: {}", event.connection) }
                .reconnectedCb { event -> log.error("Reconnected to server: {}", event.connection) }
                .build()

            Nats.connect(uri, options)
        } catch (ex: IOException) {
            log.error("Error connection to NATs! ", ex)
            null
        }
    }

    fun publishMessage(topic: String, replyTo: String, message: String) {
        try {
            natsConnection!!.publish(topic, replyTo, message.toByteArray())
        } catch (ioe: IOException) {
            log.error("Error publishing message: {} to {} ", message, topic, ioe)
        }
    }

    fun subscribeAsync(topic: String) {
        val subscription = natsConnection!!.subscribe(topic)
//        log.info("Received message on {}", msg.getSubject())
        if (subscription == null) {
            log.error("Error subscribing to {}", topic)
        } else {
            subscriptions to (topic to subscription)
        }
    }

    fun subscribeSync(topic: String): SyncSubscription {
        return natsConnection!!.subscribe(topic)
    }

    fun unsubscribe(topic: String) {
        try {
            val subscription = subscriptions[topic]

            if( subscription !== null) {
                subscription.unsubscribe()
            } else {
                log.error("{} not found. Unable to unsubscribe.", topic)
            }
        } catch(ioe: IOException) {
            log.error("Error unsubscribing from {} ", topic, ioe)
        }
    }

    fun installReply(topic: String, reply: String) {
        natsConnection!!.subscribe(topic) { message ->
            try {
                natsConnection!!.publish(message.replyTo, reply.toByteArray())
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }

    fun makeRequest(topic: String, request: String): Message? {
        return try {
            natsConnection!!.request(topic, request.toByteArray())
        } catch (ex: Exception) {
            when(ex) {
                is IOException,
                is InterruptedException -> {
                    log.error("Error making request {} to {} ", topic, request, ex)
                }
            }
            null
        }
    }

    fun joinQueueGroup(topic: String, queue: String): SyncSubscription {
        return natsConnection!!.subscribe(topic, queue)
    }

}