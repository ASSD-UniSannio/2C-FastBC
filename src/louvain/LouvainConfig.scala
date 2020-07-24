package louvain

class LouvainConfig(
  var outputFile: String, var minProgress: Int, var progressCounter: Int, var saveLouvainData: Boolean) {

  def this(outputFile: String) = this(outputFile + "/louvain", 5000, 2, false)

}