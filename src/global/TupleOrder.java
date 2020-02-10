package global;

/**
 * Enumeration class for TupleOrder
 */

public class TupleOrder {

    public static final int Ascending = 0;
    public static final int Descending = 1;
    public static final int Random = 2;

    public int tupleOrder;

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
     * @param _tupleOrder The possible ordering of the tuples
     */

    public TupleOrder(int _tupleOrder) {
        tupleOrder = _tupleOrder;
    }

    public String toString() {

        switch (tupleOrder) {
            case Ascending:
                return "Ascending";
            case Descending:
                return "Descending";
            case Random:
                return "Random";
        }
        return ("Unexpected TupleOrder " + tupleOrder);
    }

}
