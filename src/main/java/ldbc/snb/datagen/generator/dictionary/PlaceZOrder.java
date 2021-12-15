package ldbc.snb.datagen.generator.dictionary;

/**
 * Private class used to sort countries by their z-order value.
 */
class PlaceZOrder implements Comparable<PlaceZOrder> {

    public int id;
    Integer zValue;

    PlaceZOrder(int id, int zValue) {
        this.id = id;
        this.zValue = zValue;
    }

    public int compareTo(PlaceZOrder obj) {
        return zValue.compareTo(obj.zValue);
    }
}
