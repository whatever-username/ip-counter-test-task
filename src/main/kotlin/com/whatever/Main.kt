package com.whatever

import java.io.File
import java.lang.Runtime.getRuntime
import java.util.BitSet
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

fun main(args: Array<String>) = runBlocking {
    val start = System.currentTimeMillis()
    val file = File(args[0])
    val threads = getRuntime().availableProcessors()
    val threadPool = Executors.newFixedThreadPool(threads)
    val consumersDispatcher = threadPool.asCoroutineDispatcher()
    val positiveBitSet = BitSet(Math.pow(2.0, 16.0).toInt())
    val negativeBitSet = BitSet(Math.pow(2.0, 16.0).toInt())
    val positiveLock = ReentrantLock()
    val negativeLock = ReentrantLock()
    val channel = Channel<Array<String>>(capacity = Channel.UNLIMITED)

    val producerJob = launch(Dispatchers.IO) {
        parseFile(file, channel, 2000)
    }

    val consumerJobs = (1..threads).map {
        launch(consumersDispatcher) {
            consumeData(channel, positiveBitSet, negativeBitSet, positiveLock, negativeLock)
        }
    }

    producerJob.join()
    channel.close()
    consumerJobs.joinAll()
    threadPool.shutdown()

    println("IPs count: ${negativeBitSet.cardinality() + positiveBitSet.cardinality()}")
    println("Duration: ${System.currentTimeMillis() - start} ms")
}

suspend fun CoroutineScope.consumeData(
    channel: Channel<Array<String>>,
    positiveBitSet: BitSet,
    negativeBitSet: BitSet,
    positiveLock: ReentrantLock,
    negativeLock: ReentrantLock
) {
    for (bufferedIps in channel) {
        bufferedIps.asSequence()
            .map { it.convertToInt() }
            .partition { it >= 0 }
            .let {
                positiveLock.withLock {
                    it.first.forEach { ip -> positiveBitSet.set(ip) }
                }
                negativeLock.withLock {
                    it.second.forEach { ip -> negativeBitSet.set(ip.inv()) }
                }
            }
    }
}

fun String.convertToInt(): Int {
    val parts = this.split(".")
    return parts[0].toInt() shl 24 or
            (parts[1].toInt() shl 16) or
            (parts[2].toInt() shl 8) or
            parts[3].toInt()
}

suspend fun parseFile(file: File, channel: Channel<Array<String>>, bufferSize: Int) {
    val start = System.currentTimeMillis()
    val buffer = Array<String>(bufferSize) { "" }
    var iter = 0

    file.bufferedReader().use { reader ->
        var line = reader.readLine()
        while (line != null) {
            buffer[iter] = line
            if (iter == bufferSize - 1) {
                channel.send(buffer.copyOf())
                iter = 0
            } else {
                iter++
            }
            line = reader.readLine()
        }
        if (iter > 0) {
            channel.send(buffer.copyOfRange(0, iter))
        }
    }
    println("Producer finished in ${System.currentTimeMillis() - start} ms")
}


