

object Application extends App {
  val sst = new SSTEngine()

  for (i <- 1 to 30) {
    sst.set(s"test$i", "aasdf")
  }

  sst.compactLater()
  Thread.sleep(1000)

  for (i <- 1 to 30) {
    println(sst.get(s"test$i"))
  }
//  println(sst.get("test2"))


  sst.shutdown()
}
