/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import java.nio.ByteBuffer
import java.util.LinkedHashMap

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashSet}

import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.util.collection.SizeTrackingVector

private case class MemoryEntry(value: Any, size: Long, deserialized: Boolean)

/**
 * Stores blocks in memory, either as Arrays of deserialized Java objects or as
 * serialized ByteBuffers.
 */
private[spark] class MemoryStore(blockManager: BlockManager, maxMemory: Long)
  extends BlockStore(blockManager) {

  private val conf = blockManager.conf
  private val entries = new LinkedHashMap[BlockId, MemoryEntry](32, 0.75f, true)
  private val tobeDroppedBlocksSet = new HashSet[BlockId]

  // currentMemory is actually memory that is already used for caching blocks
  @volatile private var currentMemory = 0L
  @volatile private var safeCurrentMemory = 0L

  // Ensure only one thread is putting, and if necessary, dropping blocks at any given time
  private val accountingLock = new Object

  // A mapping from thread ID to amount of memory used for unrolling a block (in bytes)
  // All accesses of this map are assumed to have manually synchronized on `accountingLock`
  private val unrollMemoryMap = mutable.HashMap[Long, Long]()
  
    // A mapping from thread ID to amount of memory used for unrolling a block (in bytes)
  // All accesses of this map are assumed to have manually synchronized on `accountingLock`
  private val toDropMemoryMap = mutable.HashMap[Long, Long]()
  
  private val tryToPutMemoryMap = mutable.HashMap[Long, Long]()
  
  private val droppedMemoryMap = mutable.HashMap[Long, Long]()

  /**
   * The amount of space ensured for unrolling values in memory, shared across all cores.
   * This space is not reserved in advance, but allocated dynamically by dropping existing blocks.
   */
  private val maxUnrollMemory: Long = {
    val unrollFraction = conf.getDouble("spark.storage.unrollFraction", 0.2)
    (maxMemory * unrollFraction).toLong
  }

  logInfo("MemoryStore started with capacity %s".format(Utils.bytesToString(maxMemory)))

  /**
   *  Free memory not occupied by existing blocks. Note that this does not include reserved 
   *  memory for new blocks.
   */
  def freeMemory: Long = maxMemory - (
      safeCurrentMemory + currentTryToPutMemory - currentToDropMemory)
  
  def freeMemoryForUnroll: Long = maxMemory - (
      safeCurrentMemory + currentTryToPutMemory + currentUnrollMemory - currentToDropMemory)
  


  override def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  override def putBytes(blockId: BlockId, _bytes: ByteBuffer, level: StorageLevel): PutResult = {
    // Work on a duplicate - since the original input might be used elsewhere.
    val bytes = _bytes.duplicate()
    bytes.rewind()
    if (level.deserialized) {
      val values = blockManager.dataDeserialize(blockId, bytes)
      putIterator(blockId, values, level, returnValues = true)
    } else {
      val putAttempt = tryToPut(blockId, bytes, bytes.limit, deserialized = false)
      PutResult(bytes.limit(), Right(bytes.duplicate()), putAttempt.droppedBlocks)
    }
  }

  override def putArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    if (level.deserialized) {
      val sizeEstimate = SizeEstimator.estimate(values.asInstanceOf[AnyRef])
      val putAttempt = tryToPut(blockId, values, sizeEstimate, deserialized = true)
      PutResult(sizeEstimate, Left(values.iterator), putAttempt.droppedBlocks)
    } else {
      val bytes = blockManager.dataSerialize(blockId, values.iterator)
      val putAttempt = tryToPut(blockId, bytes, bytes.limit, deserialized = false)
      PutResult(bytes.limit(), Right(bytes.duplicate()), putAttempt.droppedBlocks)
    }
  }

  override def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    putIterator(blockId, values, level, returnValues, allowPersistToDisk = true)
  }

  /**
   * Attempt to put the given block in memory store.
   *
   * There may not be enough space to fully unroll the iterator in memory, in which case we
   * optionally drop the values to disk if
   *   (1) the block's storage level specifies useDisk, and
   *   (2) `allowPersistToDisk` is true.
   *
   * One scenario in which `allowPersistToDisk` is false is when the BlockManager reads a block
   * back from disk and attempts to cache it in memory. In this case, we should not persist the
   * block back on disk again, as it is already in disk store.
   */
  private[storage] def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean,
      allowPersistToDisk: Boolean): PutResult = {
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    val unrolledValues = preUnrollSafely(blockId, values, droppedBlocks)
    unrolledValues match {
      case Left(arrayValues) =>
        // Values are fully unrolled in memory, so store them as an array
        val res = putArray(blockId, arrayValues, level, returnValues)
        droppedBlocks ++= res.droppedBlocks
        PutResult(res.size, res.data, droppedBlocks)
      case Right(iteratorValues) =>
        // Not enough space to unroll this block; drop to disk if applicable
        logWarning(s"Not enough space to store block $blockId in memory! " +
          s"Free memory is $freeMemory bytes.")
        if (level.useDisk && allowPersistToDisk) {
          logWarning(s"Persisting block $blockId to disk instead.")
          val res = blockManager.diskStore.putIterator(blockId, iteratorValues, level, returnValues)
          PutResult(res.size, res.data, droppedBlocks)
        } else {
          PutResult(0, Left(iteratorValues), droppedBlocks)
        }
    }
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(blockManager.dataSerialize(blockId, entry.value.asInstanceOf[Array[Any]].iterator))
    } else {
      Some(entry.value.asInstanceOf[ByteBuffer].duplicate()) // Doesn't actually copy the data
    }
  }

  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(entry.value.asInstanceOf[Array[Any]].iterator)
    } else {
      val buffer = entry.value.asInstanceOf[ByteBuffer].duplicate() // Doesn't actually copy data
      Some(blockManager.dataDeserialize(blockId, buffer))
    }
  }

  override def remove(blockId: BlockId): Boolean = {
    entries.synchronized {
      tobeDroppedBlocksSet.remove(blockId)
      val entry = entries.remove(blockId)
      if (entry != null) {
        increaseDroppedMemoryForThisThread(entry.size)
        currentMemory -= entry.size
        logInfo(s"Block $blockId of size ${entry.size} dropped from memory (free $freeMemory)")
        true
      } else {
        false
      }
    }
  }

  override def clear() {
    entries.synchronized {
      entries.clear()
      tobeDroppedBlocksSet.clear()
      currentMemory = 0
      safeCurrentMemory = currentMemory
    }
    logInfo("MemoryStore cleared")
  }

  /**
   * Unroll the given block in memory safely.
   *
   * The safety of this operation refers to avoiding potential OOM exceptions caused by
   * unrolling the entirety of the block in memory at once. This is achieved by periodically
   * checking whether the memory restrictions for unrolling blocks are still satisfied,
   * stopping immediately if not. This check is a safeguard against the scenario in which
   * there is not enough free memory to accommodate the entirety of a single block.
   *
   * This method returns either an array with the contents of the entire block or an iterator
   * containing the values of the block (if the array would have exceeded available memory).
   */

  def preUnrollSafely(
    blockId: BlockId,
    values: Iterator[Any],
    droppedBlocks: ArrayBuffer[(BlockId, BlockStatus)]): Either[Array[Any], Iterator[Any]] = {

    // Number of elements unrolled so far
    var elementsUnrolled = 0L
    // Whether there is still enough memory for us to continue unrolling this block
    var keepUnrolling = true
    // How often to check whether we need to request more memory
    val memoryCheckPeriod = 16
    // Memory currently reserved by this thread for this particular unrolling operation
    // Initial value is 0 means don't reserve memory originally, only reserve dynamically
    var memoryThreshold = 0L
    // Memory to request as a multiple of current vector size
    val memoryGrowthFactor = 1.5
    // Underlying vector for unrolling the block
    var vector = new SizeTrackingVector[Any]

    // preUnroll this block safely, checking whether we have exceeded our threshold periodically
    try {
      logInfo(s"%%%%%%%%safeCurrentMemory is ${safeCurrentMemory}, currentMemory is ${currentMemory},	currentTryToPutMemory is ${currentTryToPutMemory}, currentUnrollMemory is ${currentUnrollMemory}, currentToDropMemory is ${currentToDropMemory}")
      while (values.hasNext && keepUnrolling) {
        vector += values.next()
        if (elementsUnrolled % memoryCheckPeriod == 0 || !values.hasNext) {

          // If our vector's size has exceeded the threshold, request more memory
          val currentSize = vector.estimateSize()
          if (currentSize > memoryThreshold) {
            //            logInfo(s"***vector size is ${currentSize} ")
            val amountToRequest = (currentSize * memoryGrowthFactor - memoryThreshold).toLong

            accountingLock.synchronized {
              logInfo(s"freeMemoryForUnroll is ${freeMemoryForUnroll}, amountToRequest is ${amountToRequest}")
              if (freeMemoryForUnroll < amountToRequest) {
                var selectedMemory = 0L
                val selectedBlocks = new ArrayBuffer[BlockId]()

                val ensureSpaceResult = ensureFreeSpaceForUnroll(blockId, amountToRequest)

                val enoughFreeSpace = ensureSpaceResult.success
                if (enoughFreeSpace) {
                  selectedBlocks ++= ensureSpaceResult.toDropBlocksId
                  selectedMemory = ensureSpaceResult.selectedMemory
                  if (!selectedBlocks.isEmpty) {
                    for (selectedblockId <- selectedBlocks) {
                      val entry = entries.synchronized { entries.get(selectedblockId) }
                      // This should never be null as only one thread should be dropping
                      // blocks and removing entries. However the check is still here for
                      // future safety.
                      if (entry != null) {
                        val data = if (entry.deserialized) {
                          Left(entry.value.asInstanceOf[Array[Any]])
                        } else {
                          Right(entry.value.asInstanceOf[ByteBuffer].duplicate())
                        }
                        val droppedBlockStatus = blockManager.dropFromMemory(selectedblockId, data)
                        droppedBlockStatus.foreach { status => droppedBlocks += ((selectedblockId, status)) }
                      }
                    }
                  }
                  accountingLock.synchronized {
                    updateToDropMemory
                    increaseUnrollMemoryForThisThread(amountToRequest)
                    safeCurrentMemory = currentMemory
                  }
                } else {
                  keepUnrolling = false
                }
              } else {
                increaseUnrollMemoryForThisThread(amountToRequest)
              }
            }
            if (keepUnrolling) {
              memoryThreshold += amountToRequest
            }
          }
        }
        elementsUnrolled += 1
      }
      logInfo(s"going to out of the func %%%%%%%%safeCurrentMemory is ${safeCurrentMemory}, currentMemory is ${currentMemory},	currentTryToPutMemory is ${currentTryToPutMemory}, currentUnrollMemory is ${currentUnrollMemory}, currentToDropMemory is ${currentToDropMemory}")

      if (keepUnrolling) {
        // to free up memory that requested more than needed
        decreaseUnrollMemoryForThisThread(memoryThreshold - SizeEstimator.estimate(vector.toArray.asInstanceOf[AnyRef]))
        logInfo(s"successfully unrolloing the block, blockID is ${blockId}, size is ${SizeEstimator.estimate(vector.toArray.asInstanceOf[AnyRef])}")
        // We successfully unrolled the entirety of this block
        Left(vector.toArray)
      } else {
        // We ran out of space while unrolling the values for this block
        logInfo(s"failed unrolloing the block, blockID is ${blockId}")
        Right(vector.iterator ++ values)
      }
    } finally {
      accountingLock.synchronized {
        if (!keepUnrolling) {
          removeUnrollMemoryForThisThread
          removeToDropMemoryForThisThread
        }

      }
    }
  }

  /**
   * Return the RDD ID that a given block ID is from, or None if it is not an RDD block.
   */
  private def getRddId(blockId: BlockId): Option[Int] = {
    blockId.asRDDId.map(_.rddId)
  }

  /**
   * Try to put in a set of values, if we can free up enough space. The value should either be
   * an Array if deserialized is true or a ByteBuffer otherwise. Its (possibly estimated) size
   * must also be passed by the caller.
   *
   * Synchronize on `accountingLock` to ensure that all the put requests and its associated block
   * dropping is done by only on thread at a time. Otherwise while one thread is dropping
   * blocks to free memory for one block, another thread may use up the freed space for
   * another block.
   *
   * Return whether put was successful, along with the blocks dropped in the process.
   */

  private def tryToPut(
    blockId: BlockId,
    value: Any,
    size: Long,
    deserialized: Boolean): ResultWithDroppedBlocks = {


    /* TODO: Its possible to optimize the locking by locking entries only when selecting blocks
     * to be dropped. Once the to-be-dropped blocks have been selected, and lock on entries has
     * been released, it must be ensured that those to-be-dropped blocks are not double counted
     * for freeing up more space for another block that needs to be put. Only then the actually
     * dropping of blocks (and writing to disk if necessary) can proceed in parallel. */

    var putSuccess = false
    var enoughFreeSpace = false
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    var selectedMemory = 0L
    val selectedBlocks = new ArrayBuffer[BlockId]()

    val freeSpaceResult = ensureFreeSpaceForTryToPut(blockId, size)
    enoughFreeSpace = freeSpaceResult.success
    if (enoughFreeSpace) {
      selectedBlocks ++= freeSpaceResult.toDropBlocksId
      selectedMemory = freeSpaceResult.selectedMemory
      if (!selectedBlocks.isEmpty) {
        for (selectedblockId <- selectedBlocks) {
          val entry = entries.synchronized { entries.get(selectedblockId) }
          // This should never be null as only one thread should be dropping
          // blocks and removing entries. However the check is still here for
          // future safety.
          if (entry != null) {
            val data = if (entry.deserialized) {
              Left(entry.value.asInstanceOf[Array[Any]])
            } else {
              Right(entry.value.asInstanceOf[ByteBuffer].duplicate())
            }
            val droppedBlockStatus = blockManager.dropFromMemory(selectedblockId, data)
            droppedBlockStatus.foreach { status => droppedBlocks += ((selectedblockId, status)) }
          }
        }
      }

      val entry = new MemoryEntry(value, size, deserialized)
      entries.synchronized {
        entries.put(blockId, entry)

        decreaseTryToPutMemoryForThisThread(size)
        updateToDropMemory

        currentMemory += size
        safeCurrentMemory = currentMemory
      }
      val valuesOrBytes = if (deserialized) "values" else "bytes"
      logInfo("Block %s stored as %s in memory (estimated size %s, free %s)".format(
        blockId, valuesOrBytes, Utils.bytesToString(size), Utils.bytesToString(freeMemory)))
      putSuccess = true
    } else {
      logInfo(s"failing to puth block id : ${blockId} to memory, will drop to disk")
      logInfo(s"current block to be dropped size is ${size}")
      removeUnrollMemoryForThisThread
      // Tell the block manager that we couldn't put it in memory so that it can drop it to
      // disk if the block allows disk storage.
      val data = if (deserialized) {
        Left(value.asInstanceOf[Array[Any]])
      } else {
        Right(value.asInstanceOf[ByteBuffer].duplicate())
      }
      val droppedBlockStatus = blockManager.dropFromMemory(blockId, data)
      droppedBlockStatus.foreach { status => droppedBlocks += ((blockId, status)) }
    }

    ResultWithDroppedBlocks(putSuccess, droppedBlocks)
  }

  /**
   * Try to free up a given amount of space to store a particular block, but can fail if
   * either the block is bigger than our memory or it would require replacing another block
   * from the same RDD (which leads to a wasteful cyclic replacement pattern for RDDs that
   * don't fit into memory that we want to avoid).
   *
   * Assume that `accountingLock` is held by the caller to ensure only one thread is dropping
   * blocks. Otherwise, the freed space may fill up before the caller puts in their new value.
   *
   * Return whether there is enough free space, along with the blocks dropped in the process.
   */

  /**
   * Assume that `accountingLock` is held by the caller to ensure only one thread is dropping
   * blocks. Otherwise, the freed space may fill up before the caller puts in their new value.
   */
  private def ensureFreeSpaceForTryToPut(
    blockIdToAdd: BlockId,
    size: Long): ResultBlocksIdMemory = {
    logInfo(s"ensureFreeSpace($size) called with curMem=$currentMemory, maxMem=$maxMemory")

    var putSuccess = false
    var enoughFreeSpace = false
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    var selectedMemory = 0L
    val selectedBlocks = new ArrayBuffer[BlockId]()

    if (size > maxMemory) {
      logInfo(s"Will not store $blockIdToAdd as it is larger than our memory limit")
      ResultBlocksIdMemory(success = false, selectedBlocks.toSeq, selectedMemory)
    } else {
      logInfo(s"tobe dropped blockset is ${tobeDroppedBlocksSet.toArray}")
      logInfo(s",,,,,,,freeMemory is ${freeMemory}, safeCurrentMemory is ${safeCurrentMemory}, currentMemory is ${currentMemory}")
      // This is synchronized to ensure that the set of entries is not changed
      // (because of getValue or getBytes) while traversing the iterator, as that
      // can lead to exceptions.
      entries.synchronized {
        if (freeMemory < size) {
          val rddToAdd = getRddId(blockIdToAdd)
          val iterator = entries.entrySet().iterator()
          while (freeMemory + selectedMemory < size && iterator.hasNext) {
            val pair = iterator.next()
            val blockId = pair.getKey
            if (!tobeDroppedBlocksSet.contains(blockId)) {
              if (rddToAdd.isEmpty || rddToAdd != getRddId(blockId)) {
                selectedBlocks += blockId
                selectedMemory += pair.getValue.size
              }
            }
          }
        }
        if (freeMemory + selectedMemory >= size) {
          tobeDroppedBlocksSet ++= selectedBlocks
          increaseTryToPutMemoryForThisThread(size)
          decreaseUnrollMemoryForThisThread(size)
          increaseToDropMemoryForThisThread(selectedMemory)
          enoughFreeSpace = true
          logInfo(selectedBlocks.size + " blocks selected for dropping")
          ResultBlocksIdMemory(success = true, selectedBlocks.toSeq, selectedMemory)

        } else {
          logInfo(s"ensureFreeSpaceForTryToPut Will not store $blockIdToAdd as it would require dropping another block " +
            "from the same RDD")
          ResultBlocksIdMemory(success = false, selectedBlocks.toSeq, selectedMemory)
        }
      }
    }

  }

  /**
   * Assume that `accountingLock` is held by the caller to ensure only one thread is dropping
   * blocks. Otherwise, the freed space may fill up before the caller puts in their new value.
   */
  private def ensureFreeSpaceForUnroll(
    blockIdToAdd: BlockId,
    size: Long): ResultBlocksIdMemory = {
    logInfo(s"ensureFreeSpace($size) called with curMem=$currentMemory, maxMem=$maxMemory")

    var putSuccess = false
    var enoughFreeSpace = false
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    var selectedMemory = 0L
    val selectedBlocks = new ArrayBuffer[BlockId]()

    if (size > maxMemory) {
      logInfo(s"Will not store $blockIdToAdd as it is larger than our memory limit")
      ResultBlocksIdMemory(success = false, selectedBlocks.toSeq, selectedMemory)
    } else {
      // This is synchronized to ensure that the set of entries is not changed
      // (because of getValue or getBytes) while traversing the iterator, as that
      // can lead to exceptions.
      entries.synchronized {
        if (freeMemoryForUnroll < size) {
          val rddToAdd = getRddId(blockIdToAdd)
          val iterator = entries.entrySet().iterator()
          while (freeMemory + selectedMemory < size && iterator.hasNext) {
            val pair = iterator.next()
            val blockId = pair.getKey
            if (!tobeDroppedBlocksSet.contains(blockId)) {
              if (rddToAdd.isEmpty || rddToAdd != getRddId(blockId)) {
                selectedBlocks += blockId
                selectedMemory += pair.getValue.size
              }
            }
          }
        }
        if (freeMemoryForUnroll + selectedMemory >= size) {
logInfo(s"in ensureFreeSpaceForUnroll, ensuresize is ${size}, selected Memory is ${selectedMemory} freeMemoryForUnroll is ${freeMemoryForUnroll}")
          tobeDroppedBlocksSet ++= selectedBlocks
          increaseUnrollMemoryForThisThread(size)
          increaseToDropMemoryForThisThread(selectedMemory)
          enoughFreeSpace = true
          logInfo(selectedBlocks.size + " blocks selected for dropping")
          ResultBlocksIdMemory(success = true, selectedBlocks.toSeq, selectedMemory)
        } else {
          logInfo(s"Will not store $blockIdToAdd as it would require dropping another block " +
            "from the same RDD")
          ResultBlocksIdMemory(success = false, selectedBlocks.toSeq, selectedMemory)
        }
      }
    }
  }


  override def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }

  /**
   * Reserve additional memory for unrolling blocks used by this thread.
   * Return whether the request is granted.
   */
  private[spark] def increaseToDropMemoryForThisThread(memory: Long): Unit = {
    val threadId = Thread.currentThread().getId
    accountingLock.synchronized {
      toDropMemoryMap(threadId) = toDropMemoryMap.getOrElse(threadId, 0L) + memory
    }
  }

  /**
   * Release memory used by this thread for unrolling blocks.
   * If the amount is not specified, remove the current thread's allocation altogether.
   */
  private[spark] def decreaseToDropMemoryForThisThread(memory: Long = -1L): Unit = {
    val threadId = Thread.currentThread().getId
    accountingLock.synchronized {
      toDropMemoryMap(threadId) = toDropMemoryMap.getOrElse(threadId, 0L) - memory
      // If this thread claims no more unroll memory, release it completely
      if (toDropMemoryMap(threadId) <= 0) {
        toDropMemoryMap.remove(threadId)
      }
    }
  }

  private[spark] def decreaseToDropMemory(threadId: Long, memory: Long = -1L): Unit = {
    accountingLock.synchronized {
      toDropMemoryMap(threadId) = toDropMemoryMap.getOrElse(threadId, 0L) - memory
      // If this thread claims no more unroll memory, release it completely
      if (toDropMemoryMap(threadId) <= 0) {
        toDropMemoryMap.remove(threadId)
      }
    }
  }
  private[spark] def updateToDropMemory: Unit = {
    accountingLock.synchronized {
      val threadIds = droppedMemoryMap.keySet
      for (threadId <- threadIds) {
        decreaseToDropMemory(threadId, droppedMemoryMap.getOrElse(threadId, 0L))
        droppedMemoryMap.remove(threadId)
      }
    }
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks across all threads.
   */
  private[spark] def currentToDropMemory: Long = accountingLock.synchronized {
    toDropMemoryMap.values.sum
  }

  /**
   *
   */
  private[spark] def currentToDropMemoryForThisThread: Long = accountingLock.synchronized {
    toDropMemoryMap.getOrElse(Thread.currentThread().getId, 0L)
  }

  private[spark] def removeToDropMemoryForThisThread: Unit = accountingLock.synchronized {
    toDropMemoryMap.remove(Thread.currentThread().getId)
  }

  /**
   * Reserve additional memory for unrolling blocks used by this thread.
   * Return whether the request is granted.
   */
  private[spark] def increaseUnrollMemoryForThisThread(memory: Long): Unit = {
    accountingLock.synchronized {
      val threadId = Thread.currentThread().getId
      unrollMemoryMap(threadId) = unrollMemoryMap.getOrElse(threadId, 0L) + memory
    }
  }

  /**
   * Release memory used by this thread for unrolling blocks.
   * If the amount is not specified, remove the current thread's allocation altogether.
   */
  private[spark] def decreaseUnrollMemoryForThisThread(memory: Long = -1L): Unit = {
    val threadId = Thread.currentThread().getId
    accountingLock.synchronized {
      if (memory < 0) {
        unrollMemoryMap.remove(threadId)
      } else {
        unrollMemoryMap(threadId) = unrollMemoryMap.getOrElse(threadId, 0L) - memory
        // If this thread claims no more unroll memory, release it completely
        if (unrollMemoryMap(threadId) <= 0) {
          unrollMemoryMap.remove(threadId)
        }
      }
    }
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks across all threads.
   */
  private[spark] def currentUnrollMemory: Long = accountingLock.synchronized {
    unrollMemoryMap.values.sum
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks by this thread.
   */
  private[spark] def currentUnrollMemoryForThisThread: Long = accountingLock.synchronized {
    unrollMemoryMap.getOrElse(Thread.currentThread().getId, 0L)
  }

  private[spark] def removeUnrollMemoryForThisThread: Unit = accountingLock.synchronized {
    unrollMemoryMap.remove(Thread.currentThread().getId)
  }

  /**
   * Reserve additional memory for unrolling blocks used by this thread.
   * Return whether the request is granted.
   */
  private[spark] def increaseTryToPutMemoryForThisThread(memory: Long): Unit = {
    val threadId = Thread.currentThread().getId
    accountingLock.synchronized {
      tryToPutMemoryMap(threadId) = tryToPutMemoryMap.getOrElse(threadId, 0L) + memory
    }
  }

  /**
   * Release memory used by this thread for unrolling blocks.
   * If the amount is not specified, remove the current thread's allocation altogether.
   */
  private[spark] def decreaseTryToPutMemoryForThisThread(memory: Long = -1L): Unit = {
    val threadId = Thread.currentThread().getId
    accountingLock.synchronized {
      tryToPutMemoryMap(threadId) = tryToPutMemoryMap.getOrElse(threadId, memory) - memory
      // If this thread claims no more unroll memory, release it completely
      if (tryToPutMemoryMap(threadId) <= 0) {
        tryToPutMemoryMap.remove(threadId)
      }
    }
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks across all threads.
   */
  private[spark] def currentTryToPutMemory: Long = accountingLock.synchronized {
    tryToPutMemoryMap.values.sum
  }

  /**
   *
   */
  private[spark] def currentTryToPutMemoryForThisThread: Long = accountingLock.synchronized {
    tryToPutMemoryMap.getOrElse(Thread.currentThread().getId, 0L)
  }

  private[spark] def removeTryToPutMemoryForThisThread: Unit = accountingLock.synchronized {
    tryToPutMemoryMap.remove(Thread.currentThread().getId)
  }

  private[spark] def increaseDroppedMemoryForThisThread(memory: Long): Unit = {
    val threadId = Thread.currentThread().getId
    accountingLock.synchronized {
      droppedMemoryMap(threadId) = droppedMemoryMap.getOrElse(threadId, 0L) + memory
    }
  }
      
//          private[spark] def removeDroppedMemoryForThisThread: Unit = accountingLock.synchronized {
//    droppedMemoryMap.remove(Thread.currentThread().getId)
//    }
    
    
    
    
}

private[spark] case class ResultWithDroppedBlocks(
    success: Boolean,
    droppedBlocks: Seq[(BlockId, BlockStatus)])
    
private[spark] case class ResultBlocksIdMemory(
    success: Boolean,    
    toDropBlocksId: Seq[BlockId],
    selectedMemory: Long)
