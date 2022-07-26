package raft

import raft.Message.*
import java.util.*
import kotlin.collections.ArrayDeque
import kotlin.collections.ArrayList
import kotlin.math.min

/**
 * Raft algorithm implementation.
 * All functions are called from the single main thread.
 *
 * @author Alexey Shik
 */
class ProcessImpl(private val env: Environment) : Process {
    private val storage = env.storage
    private val machine = env.machine
    private val clientRequestsQueue = ArrayDeque<Command>()
    private val nextIndex = ArrayList<Int>()
    private val matchIndex = ArrayList<Int>()

    private var leaderId = -1  // -1: follower, -2: candidate, otherwise: id of known leader
    private var commitIndex = 0
    private var lastApplied = 0
    private var nVotes = 0
    private var isReplicating = false

    init {
        val lastEntryIndex = storage.readLastLogId().index
        for (i in 0 .. env.nProcesses) {
            nextIndex.add(lastEntryIndex + 1)
            matchIndex.add(0)
        }
        env.startTimeout(Timeout.ELECTION_TIMEOUT)
    }

    override fun onTimeout() {
        if (leaderId == env.processId) {
            // leader should send heartbeat
            broadcastHeartbeat(storage.readPersistentState().currentTerm, commitIndex)
            env.startTimeout(Timeout.LEADER_HEARTBEAT_PERIOD)
        } else {
            // we start new term as candidate
            leaderId = -2
            val state = storage.readPersistentState()
            val newTerm = state.currentTerm + 1
            val votedFor = env.processId
            storage.writePersistentState(PersistentState(newTerm, votedFor))
            nVotes = 1
            for (i in 1 .. env.nProcesses) {
                if (i != env.processId) {
                    if (newTerm == 1) {
                        // initial requestVote, when no entries in storage
                        env.send(i, RequestVoteRpc(newTerm, START_LOG_ID))
                    } else {
                        env.send(i, RequestVoteRpc(newTerm, storage.readLastLogId()))
                    }

                }
            }
            env.startTimeout(Timeout.ELECTION_TIMEOUT)
        }
    }

    override fun onMessage(srcId: Int, message: Message) {
        checkClientQueries()
        var state = storage.readPersistentState()

        if (message is AppendEntryRpc) {
            // append log request - either from real new leader or from outdated leader, that has restarted
            if (state.currentTerm <= message.term) {
                // message from real new leader, this node become follower
                if (state.currentTerm < message.term) {
                    storage.writePersistentState(PersistentState(message.term, null))
                    state = storage.readPersistentState()
                }
                processAppendEntry(state, message, srcId)
            } else {
                // send our term to outdated leader and reject his request
                env.send(srcId, AppendEntryResult(state.currentTerm, null))
            }
        } else if (message is AppendEntryResult) {
            if (state.currentTerm == message.term && leaderId == env.processId) {
                if (message.lastIndex == null) {
                    // entries don't match, send prev entry
                    --nextIndex[srcId]
                    env.send(srcId, AppendEntryRpc(state.currentTerm,
                        getPrevLogId(nextIndex[srcId]), commitIndex, storage.readLog(nextIndex[srcId])))
                } else {
                    // this log entry replicated successfully, try next one
                    processSuccessfulAppendEntry(state, message, srcId)
                }
            } else if (state.currentTerm < message.term) {
                storage.writePersistentState(PersistentState(message.term, null))
            }
        } else if (message is RequestVoteRpc) {
            if (state.currentTerm < message.term) {
                leaderId = -1
                if (storage.readLastLogId() <= message.lastLogId) {
                    // should vote for this candidate
                    storage.writePersistentState(PersistentState(message.term, srcId))
                    env.send(srcId, RequestVoteResult(message.term, true))
                } else {
                    // reject candidate with outdated log
                    storage.writePersistentState(PersistentState(message.term, null))
                    env.send(srcId, RequestVoteResult(message.term, false))
                }
                env.startTimeout(Timeout.ELECTION_TIMEOUT)
            } else if (state.currentTerm == message.term && lastApplied <= message.lastLogId.index) {
                if (state.votedFor == null) {
                    // first time vote
                    leaderId = -1
                    storage.writePersistentState(PersistentState(message.term, srcId))
                    state = storage.readPersistentState()
                    env.send(srcId, RequestVoteResult(state.currentTerm, true))
                } else if (state.votedFor == srcId) {
                    // second time vote for this node
                    env.send(srcId, RequestVoteResult(state.currentTerm, true))
                } else {
                    // already voted in this round for other node
                    env.send(srcId, RequestVoteResult(state.currentTerm, false))
                }
            } else {
                // outdated candidate
                env.send(srcId, RequestVoteResult(state.currentTerm, false))
            }
        } else if (message is RequestVoteResult) {
            if (message.term == state.currentTerm && message.voteGranted) {
                // somebody in this term voted for us
                ++nVotes
                if (nVotes == (env.nProcesses + 1) / 2 && leaderId != env.processId) {
                    // majority of votes for us
                    leaderId = env.processId
                    broadcastHeartbeat(state.currentTerm, commitIndex)
                    env.startTimeout(Timeout.LEADER_HEARTBEAT_PERIOD)
                }
            }
        } else if (message is ClientCommandRpc) {
            onClientCommand(message.command)
        } else if (message is ClientCommandResult) {
            env.onClientCommandResult(message.result)
            if (state.currentTerm < message.term) {
                // message from new leader
                storage.writePersistentState(PersistentState(message.term, state.votedFor))
                env.startTimeout(Timeout.ELECTION_TIMEOUT)
                leaderId = srcId
                checkClientQueries()
            }
        }

        while (commitIndex > lastApplied) {
            machine.apply(storage.readLog(++lastApplied)!!.command)
        }
    }

    private fun processAppendEntry(state: PersistentState, message: AppendEntryRpc, srcId: Int) {
        leaderId = srcId
        env.send(srcId, AppendEntryResult(state.currentTerm, appendLog(message, state.currentTerm)))

        if (message.leaderCommit > commitIndex) {
            commitIndex = min(message.leaderCommit, storage.readLastLogId().index)
        }
        while (commitIndex > lastApplied) {
            machine.apply(storage.readLog(++lastApplied)!!.command)
        }

        checkClientQueries()
        env.startTimeout(Timeout.ELECTION_TIMEOUT)
    }

    private fun appendLog(message: AppendEntryRpc, term: Int): Int? {
        val prevLogEntry = storage.readLog(message.prevLogId.index)
        if (prevLogEntry == null && message.prevLogId.index == 0 && message.entry != null) {
            // first entry for this node
            val newEntry = LogEntry(LogId(1, term), message.entry.command)
            storage.appendLogEntry(newEntry)
            // return new entry index
            return storage.readLastLogId().index
        }
        if (prevLogEntry == null && message.entry == null) {
            // no entry with such id found
            return storage.readLastLogId().index
        }
        if (prevLogEntry == null) {
            return null
        }
        if (prevLogEntry.id != message.prevLogId) {
            // entries don't match, reject request
            return null
        }
        if (message.entry != null) {
            val logEntry = storage.readLog(message.entry.id.index)
            if (logEntry == null || logEntry.id != message.entry.id) {
                // no such entry found, append new (append would delete all incorrect suffix after this index)
                val newEntry = LogEntry(LogId(message.entry.id.index, message.entry.id.term), message.entry.command)
                storage.appendLogEntry(newEntry)
                // return new entry index
                return storage.readLastLogId().index
            } else {
                return message.entry.id.index
            }
        }
        // return prev entry index
        return prevLogEntry.id.index
    }

    private fun processSuccessfulAppendEntry(state: PersistentState, message: AppendEntryResult, srcId: Int) {
        if (message.lastIndex!! >= nextIndex[srcId]) {
            nextIndex[srcId] = message.lastIndex + 1
            matchIndex[srcId] = message.lastIndex
        } else if (message.lastIndex < storage.readLastLogId().index) {
            env.send(srcId, AppendEntryRpc(state.currentTerm, getPrevLogId(message.lastIndex + 1),
                commitIndex, storage.readLog(message.lastIndex + 1)))
        }

        // calculate number of replicas of this entry
        var nReplicated = 1
        for (i in 1 .. env.nProcesses) {
            if (i != env.processId && matchIndex[i] == message.lastIndex) {
                ++nReplicated
            }
        }
        if (nReplicated >= (env.nProcesses + 1) / 2 && commitIndex < message.lastIndex) {
            // successfully replicated operation, send requested node result
            while (commitIndex < message.lastIndex) {
                val logEntry = storage.readLog(commitIndex + 1)!!
                val currCommand = logEntry.command
                val result = machine.apply(currCommand)
                if (logEntry.id.term == state.currentTerm) {
                    if (env.processId == currCommand.processId) {
                        env.onClientCommandResult(result)
                    } else {
                        env.send(currCommand.processId, ClientCommandResult(state.currentTerm, result))
                    }
                }
                isReplicating = false
                ++commitIndex
                ++lastApplied
            }
        }
    }

    private fun getPrevLogId(index: Int): LogId {
        var prevLogId = START_LOG_ID
        if (index > 0) {
            val prevLogEntry = storage.readLog(index - 1)
            if (prevLogEntry != null) {
                prevLogId = prevLogEntry.id
            }
        }
        return prevLogId
    }

    private fun broadcastHeartbeat(term: Int, commitIndex: Int) {
        for (i in 1 .. env.nProcesses) {
            if (i != env.processId) {
                if (nextIndex[i] > storage.readLastLogId().index) {
                    env.send(i, AppendEntryRpc(term, storage.readLastLogId(), commitIndex, null))
                } else {
                    env.send(i, AppendEntryRpc(term, getPrevLogId(nextIndex[i]),
                        commitIndex, storage.readLog(nextIndex[i])))
                }
            }
        }
    }

    private fun checkClientQueries() {
        while (!clientRequestsQueue.isEmpty() && leaderId > 0) {
            onClientCommand(clientRequestsQueue.removeFirst())
        }
    }

    override fun onClientCommand(command: Command) {
        val state = storage.readPersistentState()

        if (leaderId == env.processId) {
            // this node is leader, should process client query, create new entry and start replication
            val lastLogId = storage.readLastLogId()
            val newLogEntry = LogEntry(LogId(lastLogId.index + 1, state.currentTerm), command)
            if (commitIndex == lastLogId.index || !isReplicating) {
                for (i in 1..env.nProcesses) {
                    if (i != env.processId) {
                        if (nextIndex[i] > storage.readLastLogId().index) {
                            env.send(i, AppendEntryRpc(state.currentTerm, lastLogId, commitIndex, newLogEntry))
                        } else {
                            env.send(i, AppendEntryRpc(state.currentTerm, getPrevLogId(nextIndex[i]),
                                commitIndex, storage.readLog(nextIndex[i])))
                        }
                    }
                }
                isReplicating = true
            }
            storage.appendLogEntry(newLogEntry)
        } else if (leaderId > 0) {
            // this node is not a leader, but we can fallback to (maybe) leader
            env.send(leaderId, ClientCommandRpc(state.currentTerm, command))
        } else {
            // post request to waiting queue
            clientRequestsQueue.add(command)
        }
    }
}
