import system.DataHolderEnvironment
import kotlin.math.min

class DataHolderImpl<T : Comparable<T>>(
    private val keys: List<T>,
    private val dataHolderEnvironment: DataHolderEnvironment
) : DataHolder<T> {

    private var processedPrefix = 0
    private var checkpointPrefix = 0

    override fun checkpoint() {
        checkpointPrefix = processedPrefix
    }

    override fun rollBack() {
        processedPrefix = checkpointPrefix
    }

    override fun getBatch(): List<T> {
        if (processedPrefix < keys.size) {
            val res = keys.subList(processedPrefix, min(keys.size, processedPrefix + dataHolderEnvironment.batchSize))
            processedPrefix += min(dataHolderEnvironment.batchSize, keys.size - processedPrefix)
            return res
        }
        return emptyList()
    }
}