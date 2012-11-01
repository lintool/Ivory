package ivory.sqe.retrieval;

import edu.umd.cloud9.io.map.HMapSFW;

public class PairOfFloatMap {
  HMapSFW map;
  Float weight;
 
  public PairOfFloatMap(HMapSFW map, Float weight) {
    super();
    this.map = map;
    this.weight = weight;
  }

  public HMapSFW getMap() {
    return map;
  }

  public void setMap(HMapSFW map) {
    this.map = map;
  }

  public Float getWeight() {
    return weight;
  }

  public void setWeight(Float weight) {
    this.weight = weight;
  }
  
}
