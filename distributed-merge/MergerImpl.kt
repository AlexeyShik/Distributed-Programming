import system.MergerEnvironment
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

class MergerImpl<T : Comparable<T>>(
    private val mergerEnvironment: MergerEnvironment<T>,
    prevStepBatches: Map<Int, List<T>>?
) : Merger<T> {

    private class Entry<T>(val value: T, val pid: Int) : Comparable<Entry<T>> {
        override fun compareTo(other: Entry<T>): Int {
            return (value as Comparable<T>).compareTo(other.value)
        }

    }

    private var queue : PriorityQueue<Entry<T>> = PriorityQueue()
    private var indices : MutableList<Int> = ArrayList(mergerEnvironment.dataHoldersCount)
    private var batches : MutableMap<Int, List<T>> = HashMap()

    init {
        if (prevStepBatches != null) {
            batches = prevStepBatches.toMutableMap()
        }
        for (i in 0 until mergerEnvironment.dataHoldersCount) {
            if (!batches.containsKey(i)) {
                batches[i] = mergerEnvironment.requestBatch(i)
            }
            if (batches[i]?.isNotEmpty()!!) {
                queue.add(Entry(batches[i]?.get(0)!!, i))
            }
            indices.add(0)
        }
    }

    override fun mergeStep(): T? {
        if (queue.isEmpty()) {
            return null
        }
        val entry = queue.poll()
        ++indices[entry.pid]
        if (indices[entry.pid] == batches[entry.pid]!!.size) {
            batches[entry.pid] = mergerEnvironment.requestBatch(entry.pid)
            indices[entry.pid] = 0
        }
        if (indices[entry.pid] < batches[entry.pid]!!.size) {
            queue.add(Entry(batches[entry.pid]!![indices[entry.pid]], entry.pid))
        }
        return entry.value
    }

    override fun getRemainingBatches(): Map<Int, List<T>> {
        val remain = HashMap<Int, MutableList<T>>()
        val it = queue.iterator()
        while (it.hasNext()) {
            val entry : Entry<T> = it.next()
            if (!remain.containsKey(entry.pid)) {
                remain[entry.pid] = ArrayList()
            }
            remain[entry.pid]!!.add(entry.value)
        }
        for (pid in 0 until mergerEnvironment.dataHoldersCount) {
            if (batches.containsKey(pid)) {
                val prevStepList = batches[pid]
                if (!remain.containsKey(pid) && indices[pid] + 1 < prevStepList!!.size) {
                    remain[pid] = ArrayList()
                }
                if (indices[pid] + 1 < prevStepList!!.size) {
                    remain[pid]!!.addAll(prevStepList.subList(indices[pid] + 1, prevStepList.size))
                }
            }
        }
        return remain
    }
}
