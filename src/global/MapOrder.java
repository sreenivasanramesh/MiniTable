package global;

public class MapOrder {

    public static final int Ascending = 0;
    public static final int Descending = 1;
    public static final int Random = 2;

    public int mapOrder;

    /**
     * TupleOrder Constructor
     * <br>
     * A tuple ordering can be defined as
     * <ul>
     * <li>   TupleOrder tupleOrder = new TupleOrder(TupleOrder.Random);
     * </ul>
     * and subsequently used as
     * <ul>
     * <li>   if (tupleOrder.tupleOrder == TupleOrder.Random) ....
     * </ul>
     *
     * @param _mapOrder The possible ordering of the tuples
     */

    public MapOrder(int _mapOrder) {
        mapOrder = _mapOrder;
    }

    public String toString() {

        switch (mapOrder) {
            case Ascending:
                return "Ascending";
            case Descending:
                return "Descending";
            case Random:
                return "Random";
        }
        return ("Unexpected TupleOrder " + mapOrder);
    }
}
