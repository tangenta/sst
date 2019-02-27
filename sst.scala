import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

// TODO: make it reliable
class SSTEngine {
  import SSTEngine._

  case class SSTable(path: Path, sparseIndexTree: TreeMap[Key, Offset]) {
    override def equals(obj: Any): Boolean = AnyRef.equals(obj)
    override def hashCode(): Offset = AnyRef.hashCode()
  }
  case class State(segmentFiles: List[SSTable], memtable: TreeMap[Key, Value]) {
    override def equals(obj: Any): Boolean = AnyRef.equals(obj)
    override def hashCode(): Offset = AnyRef.hashCode()
  }


  trait Event
  case class SetKeyValueEvent(key: Key, value: Value) extends Event
  case class CompactEvent(files: Seq[Path]) extends Event
  case class TombStoneEvent() extends Event
  case class FutureEvent(producer: State => Event) extends Event


  private var state = State(List.empty[SSTable], TreeMap.empty)
  private val eventQueue = new LinkedBlockingDeque[Event]()

  // configurable parameters
  var executor: ExecutorService = Executors.newFixedThreadPool(3)
  var avlTreeSize: Int = 5
  var storingLocation: String = "/home/tangenta/Desktop/test-folder/"
  var idAllocator: IdAllocator = new IdAllocator {
    private val id = new AtomicInteger(0)
    override def allocate(): String = s"sst-${id.getAndIncrement()}"
  }
  var segmentFileNumThreshold = 5
  var compactionPolicy: CompactionPolicy = (files: Seq[Path]) => files
  var tombstone = "some-special-string"
  var sparseIndexEntryKeyRatio = 3

  // initialization
  executor.submit(new StateWriterRoutine)

  def compact(): Unit = {
    eventQueue.put(CompactEvent(compactionPolicy.filterFilesToCompact(state.segmentFiles.map(_.path))))
  }

  def compactLater(): Unit = {
    eventQueue.put(FutureEvent(s => CompactEvent(s.segmentFiles.map(_.path))))
  }

  def set(key: Key, value: Value): Unit = {
    eventQueue.put(SetKeyValueEvent(key, value))
  }

  def delete(key: Key): Unit = {
    set(key, tombstone)
  }

  def get(key: Key): Option[Value] = {
    val stateSnapshot = state
    // TODO: change to real sparse index trees
    val result = stateSnapshot.memtable.get(key).orElse {
      stateSnapshot.segmentFiles.dropWhile(!_.sparseIndexTree.contains(key)).headOption.map {
        case SSTable(path, indexTree) =>
          val byteArray = Files.readAllBytes(path)
          val byteBuffer = ByteBuffer.wrap(byteArray)
          val offset: Int = indexTree(key)
          val keyLength = byteBuffer.getInt(offset)
          val keyString = new String(byteArray, offset + 4, keyLength, UTF_8)
          val valueLength = byteBuffer.getInt(offset + 4 + keyLength)
          val valueString = new String(byteArray, offset + 4 + keyLength + 4, valueLength, UTF_8)
          assert(keyString == key)
          valueString
      }
    }
    if (result.contains(tombstone)) None
    else result
  }

  def shutdown(): Unit = {
    eventQueue.put(TombStoneEvent())
    executor.shutdown()
  }

  class StateWriterRoutine extends Runnable {

    private def writeAsFile(bytes: Array[Byte]): Path = {
      import StandardOpenOption._
      val path = Paths.get(storingLocation).resolve(idAllocator.allocate())
      Files.write(path, bytes, CREATE_NEW, WRITE)
    }


    private def compact(segments: Seq[ByteBuffer], candidate: Seq[Option[(Key, Value)]], result: ArrayBuffer[Byte],
                        indexTree: TreeMap[Key, Offset], offset: Offset): (ArrayBuffer[Byte], TreeMap[Key, Offset]) = {
      if (segments.isEmpty) (result, indexTree)
      else {
        // load element to candidates
        val candidates = segments.zip(candidate).map { case (seg, can) =>
          can match {
            case None =>
              val key = readByteBuffer(seg)
              val value = readByteBuffer(seg)
              Some(key, value)
            case r@Some(_) => r
          }
        }
        val Some((key, value)) = candidates.minBy(_.get._1)
        result ++= bytify(key) ++= bytify(value)

        val pairs = segments.zip(candidates).collect {
          case (seg, can) if seg.hasRemaining || (can.nonEmpty && can.get._1 != key)  =>
            (seg, if (can.get._1 == key) None else can)
        }

        compact(
          pairs.map(_._1),
          pairs.map(_._2),
          result,
          indexTree.updated(key, offset),
          offset + 8 + key.getBytes().length + value.getBytes().length
        )
      }

    }

    override def run(): Unit = {
      var time2Exit = false
      while (!time2Exit) {
        // TODO: break the limitation of exclusively setting and compacting
        eventQueue.take() match {
          case SetKeyValueEvent(key, value) =>
            state = State(state.segmentFiles, state.memtable.updated(key, value))
            if (state.memtable.size >= avlTreeSize) {
              val path = writeAsFile(convertBytes(state.memtable))
              val indexTree = convertIndexTree(state.memtable)
              state = State(SSTable(path, indexTree) :: state.segmentFiles, TreeMap.empty)
            }
          case CompactEvent(files) =>
            if (files.nonEmpty) {
              // TODO: optimize behavior -- read all bytes at once
              val byteBuffers = files.map(path => ByteBuffer.wrap(Files.readAllBytes(path)))
              val (arrayBuffer, indexTree) = compact(
                byteBuffers,
                files.map(_ => None: Option[(Key, Value)]),
                ArrayBuffer.empty,
                TreeMap.empty,
                0
              )
              val compressedIndexTree = compressIndexTree(indexTree, sparseIndexEntryKeyRatio)

              val newFilePath = writeAsFile(arrayBuffer.toArray)
              state = State(
                SSTable(newFilePath, compressedIndexTree) :: state.segmentFiles.filter(s => !files.contains(s.path)),
                state.memtable
              )
              files.foreach(Files.delete)
            }
          case TombStoneEvent() => time2Exit = true
          case FutureEvent(producer) => eventQueue.addFirst(producer(state))
        }
      }
    }
  }
}

object SSTEngine {
  private type FileName = String
  private type Key = String
  private type Value = String
  private type Offset = Int

  private val UTF_8 = "UTF-8"

  // convert a string to [string's length][string's byte array]
  private def bytify(str: String): Array[Byte] = {
    val bs = str.getBytes(UTF_8)
    ByteBuffer.allocate(4 + bs.length).putInt(bs.length).put(bs).array()
  }

  private def convertBytes(memtable: TreeMap[Key, Value]): Array[Byte] = {
    val arrBuffer = ArrayBuffer.empty[Byte]
    memtable.foreach { case (k, v) =>
      arrBuffer ++= bytify(k) ++= bytify(v)
    }
    arrBuffer.toArray
  }

  private def convertIndexTree(memtable: TreeMap[Key, Value]): TreeMap[Key, Offset] = {
    var accumulateOffset = 0
    memtable.map { case (k, v) =>
      val ret = k -> accumulateOffset

      accumulateOffset += 4 + k.getBytes(UTF_8).length + 4 + v.getBytes(UTF_8).length
      ret
    }
  }

  private def readByteBuffer(buffer: ByteBuffer): String = {
    val length = buffer.getInt()
    val ret = new String(buffer.array(), buffer.position(), length)
    buffer.position(buffer.position() + length)
    ret
  }

  private def searchKey(key: Key, lowerBound: Offset, upperBound: Offset, inFile: Path): Option[Value] = {
    val readBuffer = ByteBuffer.allocate(upperBound - lowerBound)
    val channel = Files.newByteChannel(inFile, StandardOpenOption.READ)
    channel.position(lowerBound)
    assert(channel.read(readBuffer) == readBuffer.array().length)
    var result: Option[Value] = None
    while (readBuffer.hasRemaining && result.isEmpty) {
      val k = readByteBuffer(readBuffer)
      val v = readByteBuffer(readBuffer)
      if (key == k) result = Some(v)
    }
    result
  }

  private def compressIndexTree(origin: TreeMap[Key, Offset], ratio: Int): TreeMap[Key, Offset] = {
    origin.foldLeft((TreeMap.empty[Key, Offset], 0)) { case ((result, index), elem) =>
      (if (index % ratio == 0) result else result + elem, index + 1)
    }._1
  }
}
