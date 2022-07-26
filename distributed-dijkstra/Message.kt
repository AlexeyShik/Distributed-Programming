package dijkstra.messages

import dijkstra.messages.MessageFieldType.*
import java.io.*

/**
 * Inter-process communication message.
 * Copy-paste from distributed-mutex hw
 */
class Message(val bytes: ByteArray) {
    override fun toString(): String = parse {
        generateSequence {
            when (cur) {
                END -> null
                INT -> readInt().toString()
                LONG -> readLong().toString()
                ENUM -> readEnumName()
            }
        }.joinToString(separator = ", ", prefix = "Message(", postfix = ")")
    }
}

/**
 * Builder class for env messages.
 */
class MessageBuilder {
    private val out = ByteArrayOutputStream()
    private val data = DataOutputStream(out)

    fun writeInt(value: Int): MessageBuilder {
        writeField(INT)
        data.writeInt(value)
        return this
    }

    fun writeLong(value: Long): MessageBuilder {
        writeField(LONG)
        data.writeLong(value)
        return this
    }

    fun writeEnum(enum: Enum<*>): MessageBuilder {
        writeField(ENUM)
        data.writeUTF(enum.name)
        return this
    }

    fun build(): Message = Message(out.toByteArray())

    private fun writeField(t: MessageFieldType) {
        data.writeByte(t.ordinal)
    }
}

/**
 * Parser class for messages.
 */
class MessageParser(message: Message) {
    private val data = DataInputStream(ByteArrayInputStream(message.bytes))
    private var curField: MessageFieldType? = null

    fun readInt(): Int {
        check(cur == INT) { "Expected int field, but $cur found" }
        return data.readInt().also { done() }
    }

    fun readLong(): Long {
        check(cur == LONG) { "Expected long field, but $cur found" }
        return data.readLong().also { done() }
    }

    fun readEnumName(): String {
        check(cur == ENUM) { "Expected enum field, but $cur found" }
        return data.readUTF().also { done() }
    }

    inline fun <reified T : Enum<T>> readEnum(): T = enumValueOf(readEnumName())

    val cur: MessageFieldType
        get() = curField ?: run {
            val b = data.read()
            when (b) {
                -1 -> END
                in 1 until FIELD_TYPES.size -> FIELD_TYPES[b]
                else -> error("Unexpected field type $b")
            }
        }.also {
            curField = it
        }

    private fun done() {
        curField = null
    }
}

/**
 * Type of the message field.
 */
enum class MessageFieldType { END, INT, LONG, ENUM }

private val FIELD_TYPES = enumValues<MessageFieldType>()

/**
 * Builds the message.
 */
@Suppress("FunctionName")
inline fun Message(builder: MessageBuilder.() -> Unit): Message =
    MessageBuilder().apply { builder() }.build()

/**
 * Parses the message.
 */
inline fun <T> Message.parse(parser: MessageParser.() -> T): T =
    MessageParser(this).run { parser() }

