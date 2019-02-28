import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer

// TODO: make it reliable
class SSTEngine {
  import SSTEngine._

  case class SSTable(path: Path, sparseIndexTree: TreeMap[Key, Offset])
  case class State(segmentFiles: List[SSTable], memoryTable: TreeMap[Key, Value])

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
    val State(segmentFiles, memoryTable) = state

    val result = memoryTable.get(key).orElse {
      segmentFiles.dropWhile {
        case SSTable(path, iTree) =>
          searchKey(key, path, iTree).isEmpty
      }.headOption.flatMap {
        case SSTable(path, iTree) =>
          // TODO: optimize -- additional searching
          searchKey(key, path, iTree)
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
            state = State(state.segmentFiles, state.memoryTable.updated(key, value))
            if (state.memoryTable.size >= avlTreeSize) {
              val path = writeAsFile(convertBytes(state.memoryTable))
              val indexTree = convertIndexTree(state.memoryTable)
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
                state.memoryTable
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

  private def extractByteArray(path: Path, from: Option[Offset], to: Option[Offset]): ByteBuffer = {
    val lower = from.getOrElse(0)
    val upper = to.getOrElse(Files.size(path).toInt)

    val readChannel = Files.newByteChannel(path, StandardOpenOption.READ)
    val byteBuffer = ByteBuffer.allocate(upper - lower)

    readChannel.position(lower)
    assert(readChannel.read(byteBuffer) == byteBuffer.array().length)
    readChannel.close()

    byteBuffer.position(0)
    byteBuffer
  }

  private def searchKey(key: Key, inBuffer: ByteBuffer): Option[Value] = {
    val buf = inBuffer
    var result: Option[Value] = None
    while (buf.hasRemaining && result.isEmpty) {
      val k = readByteBuffer(buf)
      val v = readByteBuffer(buf)
      if (key == k) result = Some(v)
    }
    result
  }

  private def searchKey(key: Key, segmentFile: Path, indexTree: TreeMap[Key, Offset]): Option[Value] = {
    indexTree.get(key) match {
      case lower@Some(_) =>
        val iter = indexTree.valuesIteratorFrom(key)
        iter.next() // forward iterator
        val upper = if (iter.hasNext) Some(iter.next) else None
        searchKey(key, extractByteArray(segmentFile, lower, upper))
      case None =>
        val lower = indexTree.to(key).lastOption.map(_._2)
        val upper = indexTree.from(key).headOption.map(_._2)
        searchKey(key, extractByteArray(segmentFile, lower, upper))
    }
  }

  private def compressIndexTree(origin: TreeMap[Key, Offset], ratio: Int): TreeMap[Key, Offset] = {
    origin.foldLeft((TreeMap.empty[Key, Offset], 0)) {
      case ((result, index), elem) =>
        if (index % ratio == 0) (result, index + 1)
        else (result + elem, index + 1)
    }._1
  }

}