package dijkstra

import dijkstra.messages.*
import dijkstra.system.environment.Environment

class ProcessImpl(private val env: Environment) : Process {
    private var myDist: Long? = null
    private var isStart: Boolean = false

    private var requesterId: Int = -1
    private var qBalance: Int = 0

    override fun onMessage(srcId: Int, message: Message) {
        message.parse {
            val type = readEnum<MsgType>()
            val senderId = readInt()
            when (type) {
                MsgType.REQ_UPD -> {
                    val newDist = readLong()
                    if (myDist == null || myDist!! > newDist) {
                        myDist = newDist
                        if (requesterId != -1) {
                            send(MsgType.RESP_UPD, requesterId)
                        }
                        requesterId = senderId
                        broadcast()
                    } else {
                        send(MsgType.RESP_UPD, senderId)
                    }
                }
                MsgType.RESP_UPD -> {
                    --qBalance
                    checkBalance()
                }
            }
        }
    }

    override fun getDistance(): Long? {
        return myDist
    }

    override fun startComputation() {
        isStart = true
        myDist = 0
        broadcast()
    }

    private fun broadcast() {
        for (entry in env.neighbours.entries) {
            if (entry.key != env.pid) {
                ++qBalance
                send(MsgType.REQ_UPD, entry.key) { builder -> builder.writeLong(myDist!! + entry.value) }
            }
        }
        checkBalance()
    }

    private fun checkBalance() {
        if (qBalance == 0) {
            if (isStart) {
                env.finishExecution()
            } else {
                send(MsgType.RESP_UPD, requesterId)
                requesterId = -1
            }
        }
    }

    private enum class MsgType  { REQ_UPD, RESP_UPD }

    private fun send(type: MsgType, destId: Int, builder: (MessageBuilder) -> MessageBuilder = {x -> x}) {
         val msg: Message = builder(MessageBuilder()
             .writeEnum(type)
             .writeInt(env.pid))
             .build()
        env.send(destId, msg)
    }
}
