package unisannio

case class Neighbor(id: Long, distance: Double) {

  override def equals(o: Any) = o match {
    case that: Neighbor => that.id.equals(this.id)
    case _              => false
  }
  override def hashCode = id.hashCode

  override def toString = "Id: " + id + ", distance from neighbor: " + distance
}