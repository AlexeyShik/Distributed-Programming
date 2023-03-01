import java.util.*
import kotlin.collections.ArrayList

class ConsistentHashImpl<K> : ConsistentHash<K> {
    val bounds : SortedMap<Int, Shard> = TreeMap()

    override fun getShardByKey(key: K): Shard {
        val hash = key.hashCode()
        for (e in bounds.entries) {
            if (hash <= e.key) {
                return e.value
            }
        }
        return bounds[bounds.firstKey()]!!
    }

    private fun getBoundaries(hash: Int): List<Map.Entry<Int, Shard>> {
        val entries = bounds.entries
        var prevEntry = entries.last()
        var l: Map.Entry<Int, Shard> = entries.last()
        var r: Map.Entry<Int, Shard> = entries.first()
        for (i in 0 until entries.size) {
            val curEntry = entries.elementAt(i)
            if (hash < curEntry.key) {
                l = prevEntry
                r = curEntry
                break
            }
            prevEntry = curEntry
        }
        return listOf(l, r)
    }

    private fun compactRanges(ranges: MutableSet<HashRange>): MutableSet<HashRange> {
        val listRanges: MutableList<HashRange> = ArrayList(ranges)
        val sortedRanges = listRanges.sortedBy { x -> x.leftBorder }
        val ans: MutableList<HashRange> = ArrayList()
        for (range in sortedRanges) {
            if (ans.isEmpty()) {
                ans.add(range)
                continue
            }
            val last = ans.last()
            if (last.rightBorder + 1 == range.leftBorder) {
                ans[ans.size - 1] = HashRange(last.leftBorder, range.rightBorder)
            } else {
                ans.add(range)
            }
        }
        if (ans.last().rightBorder + 1 == ans.first().leftBorder) {
            ans[0] = HashRange(ans.last().leftBorder, ans.first().rightBorder)
            ans.removeLast()
        }
        return LinkedHashSet(ans)
    }

    override fun addShard(newShard: Shard, vnodeHashes: Set<Int>): Map<Shard, Set<HashRange>> {
        val ans : MutableMap<Shard, MutableSet<HashRange>> = HashMap()
        for (hash in TreeSet(vnodeHashes)) {
            if (bounds.isEmpty()) {
                bounds[hash] = newShard
                continue
            }
            val boundaries = getBoundaries(hash)
            val l = boundaries[0]
            val r = boundaries[1]
            bounds[hash] = newShard
            if (newShard == r.value) {
                continue
            }
            if (!ans.containsKey(r.value)) {
                ans[r.value] = HashSet()
            }
            ans[r.value]!!.add(HashRange(l.key + 1, hash))

        }
        for (key in ans.keys) {
            ans[key] = compactRanges(ans[key]!!)
        }
        return ans
    }

    override fun removeShard(shard: Shard): Map<Shard, Set<HashRange>> {
        val ans: MutableMap<Shard, MutableSet<HashRange>> = HashMap()
        val keys: MutableList<Int> = ArrayList()
        for (entry in bounds.entries) {
            if (entry.value == shard) {
                keys.add(entry.key)
            }
        }
        for (key in keys) {
            bounds.remove(key)
            val boundaries = getBoundaries(key)
            val l = boundaries[0]
            val r = boundaries[1]
            if (shard == r.value) {
                continue
            }
            if (!ans.containsKey(r.value)) {
                ans[r.value] = HashSet()
            }
            ans[r.value]!!.add(HashRange(l.key + 1, key))
        }
        for (key in ans.keys) {
            ans[key] = compactRanges(ans[key]!!)
        }
        return ans
    }
}
