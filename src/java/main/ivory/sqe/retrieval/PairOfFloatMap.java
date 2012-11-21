package ivory.sqe.retrieval;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.io.map.HMapSFW;

public class PairOfFloatMap {
  private HMapSFW map;
  private float weight;
 
  public PairOfFloatMap(HMapSFW map, float weight) {
    super();
    this.map = Preconditions.checkNotNull(map);
    this.weight = weight;
  }

  public HMapSFW getMap() {
    return map;
  }

  public void setMap(HMapSFW map) {
    this.map = map;
  }

  public float getWeight() {
    return weight;
  }

  public void setWeight(float weight) {
    this.weight = weight;
  }
}
