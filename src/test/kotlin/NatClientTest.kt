import com.baeldung.NatsClient
import org.junit.Test
import java.util.ArrayList
import io.nats.client.Message
import junit.framework.Assert.*


class NatClientTest {

    @Test
    fun givenMessageExchange_MessagesReceived() {
        val natsClient = connectClient()

        val fooSubscription = natsClient.subscribeSync("foo.bar")
        val barSubscription = natsClient.subscribeSync("bar.foo")
        natsClient.publishMessage("foo.bar", "bar.foo", "hello there")

        var message = fooSubscription.nextMessage(200)
        assertNotNull("No message!", message)
        assertEquals("hello there", String(message.data))

        natsClient.publishMessage(message.replyTo, message.subject, "hello back")

        message = barSubscription.nextMessage(200)
        assertNotNull("No message!", message)
        assertEquals("hello back", String(message.data))
    }

    private fun connectClient(): NatsClient {
        return NatsClient()
    }

    @Test
    fun whenWildCardSubscription_andMatchTopic_MessageReceived() {
        val natsClient = connectClient()

        val fooSubscription = natsClient.subscribeSync("foo.*")

        natsClient.publishMessage("foo.bar", "bar.foo", "hello there")

        val message = fooSubscription.nextMessage(200)
        assertNotNull("No message!", message);
        assertEquals("hello there", String(message.data))
    }


    @Test
    @Throws(Exception::class)
    fun whenWildCardSubscription_andNotMatchTopic_NoMessageReceived() {

        val client = connectClient()

        val fooSubscription = client.subscribeSync("foo.*")

        client.publishMessage("foo.bar.plop", "bar.foo", "hello there")

        var message = fooSubscription.nextMessage(200)
        assertNull("Got message!", message)


        val barSubscription = client.subscribeSync("foo.>")

        client.publishMessage("foo.bar.plop", "bar.foo", "hello there")

        message = barSubscription.nextMessage(200)
        assertNotNull("No message!", message)
        assertEquals("hello there", String(message.data))


    }


    @Test
    fun givenRequest_ReplyReceived() {

        val client = connectClient()
        client.installReply("salary.requests", "denied!")

        val reply = client.makeRequest("salary.requests", "I need a raise.")
        assertNotNull("No message!", reply)
        assertEquals("denied!", String(reply!!.data))

    }

    @Test
    @Throws(Exception::class)
    fun givenQueueMessage_OnlyOneReceived() {

        val client = connectClient()

        val queue1 = client.joinQueueGroup("foo.bar.requests", "queue1")
        val queue2 = client.joinQueueGroup("foo.bar.requests", "queue1")

        client.publishMessage("foo.bar.requests", "queuerequestor", "foobar")

        val messages = ArrayList<Message>()

        var message: Message? = queue1.nextMessage(200)

        if (message != null) messages.add(message)
        message = queue2.nextMessage(200)

        if (message != null) messages.add(message)
        assertEquals(1, messages.size)

    }
}