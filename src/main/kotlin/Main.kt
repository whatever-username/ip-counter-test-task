package com.whatever

import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.lang.Runtime.getRuntime
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import kotlin.collections.mutableListOf
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

fun main() {
    /*generateTestFile()
    return*/
    val start = System.currentTimeMillis()
    val file = File("test.txt")
    val threads = /*getRuntime().availableProcessors()*/ 12
    val threadPool = Executors.newFixedThreadPool(threads)
    val consumersDispatcher = threadPool.asCoroutineDispatcher()
    val channel = Channel<Array<String>>(capacity = 10000, onBufferOverflow = BufferOverflow.SUSPEND)

    CoroutineScope(Dispatchers.IO).async() {
        parseFile(file, channel, 1500)
    }

    val sets = ThreadLocal.withInitial { mutableSetOf<Int>() }
    val consumerJobs = (1..threads).map {
        println("consumer $it started: ${System.currentTimeMillis() - start}")
        CoroutineScope(consumersDispatcher).async {
            for (bufferedIps in channel) {
                val hex = bufferedIps.map { it.split(".").map { it.toInt() }.fold(0) { acc, i -> (acc shl 8) or i } }
                sets.get().addAll(hex.toSet())
            }
            return@async sets.get()
        }
    }
    val result = runBlocking{ consumerJobs.awaitAll().flatten().toSet() }

    println("count ${result.size}")
    println("total: ${System.currentTimeMillis() - start}")
    threadPool.shutdown()

    /*
    Unique IPs count: 22784640
    12063
    * */
}

suspend fun CoroutineScope.parseFile(file: File, channel: Channel<Array<String>>, bufferSize: Int) {

    val start = System.currentTimeMillis()
    val reader = BufferedReader(FileReader(file))
    val buffer = Array<String>(size = bufferSize) { "" }
    var iter = 0
    file.bufferedReader().use {
        var line = reader.readLine()
        while (line != null) {
            buffer[iter] = line
            if (iter == bufferSize - 1) {
                channel.send(buffer.copyOfRange(0, bufferSize))
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
    channel.close()
    println("producer finished in ${System.currentTimeMillis() - start}")
}

fun generateTestFile() {
    val file = File("test1m.txt")
    val res = mutableListOf<String>()
    for (i in 1..1_000_000) {
        val ip = "${(0..255).random()}.${(0..255).random()}.${(0..255).random()}.${(0..255).random()}"
        res.add(ip)
        if (i % 10000 == 0) {
            file.appendText(res.joinToString("\n") + "\n")
            res.clear()
        }
    }
}