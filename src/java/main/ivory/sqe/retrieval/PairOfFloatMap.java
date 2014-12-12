package ivory.sqe.retrieval;

import tl.lin.data.map.HMapStFW;

import com.google.common.base.Preconditions;

public class PairOfFloatMap {
  private HMapStFW map;
  private float weight;
 
  public PairOfFloatMap(HMapStFW map, float weight) {
    super();
    this.map = Preconditions.checkNotNull(map);
    this.weight = weight;
  }

  public HMapStFW getMap() {
    return map;
  }

  public void setMap(HMapStFW map) {
    this.map = map;
  }

  public float getWeight() {
    return weight;
  }

  public void setWeight(float weight) {
    this.weight = weight;
  }
}
