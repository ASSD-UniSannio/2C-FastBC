package louvain

import com.esotericsoftware.kryo.KryoSerializable
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input

class LouvainVertex (var community: Long,
    var communitySigmaTot: Double,
    var internalWeight: Double,
    var nodeWeight: Double,
    var changed: Boolean,
    var preCommunity: Long) extends Serializable with KryoSerializable {
  
    def this() = this(-1L, 0, 0, 0, false, -1)
    
    override def toString(): String = {
        "{community:"+community+",communitySigmaTot:"+communitySigmaTot+
        ",internalWeight:"+internalWeight+",nodeWeight:"+nodeWeight+
        ",preCommunity:"+preCommunity+"}"
        //userData.toString() + "}"
    }
    
    override def write(kryo: Kryo, output: Output): Unit = {
        kryo.writeObject(output, this.community)
        kryo.writeObject(output, this.communitySigmaTot)
        kryo.writeObject(output, this.internalWeight)
        kryo.writeObject(output, this.nodeWeight)
        kryo.writeObject(output, this.changed)
        kryo.writeObject(output, this.preCommunity)
        //kryo.writeObject(output, this.userData)
    }
  
    override def read(kryo: Kryo, input: Input): Unit = {
        this.community = kryo.readObject(input, classOf[Long])
        this.communitySigmaTot = kryo.readObject(input, classOf[Double])
        this.internalWeight = kryo.readObject(input, classOf[Double])
        this.nodeWeight = kryo.readObject(input, classOf[Double])
        this.changed = kryo.readObject(input, classOf[Boolean])
        this.preCommunity = kryo.readObject(input, classOf[Long])
        //this.userData = kryo.readObject(input, classOf[UserBriefData])
    }
}