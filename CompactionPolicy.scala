import java.nio.file.Path

trait CompactionPolicy {
  def filterFilesToCompact(files: Seq[Path]): Seq[Path]
}
