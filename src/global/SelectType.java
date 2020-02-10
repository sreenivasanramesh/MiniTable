package global;

/**
 * Enumeration class for SelectType
 */

public class SelectType {

    public static final int selRange = 0;
    public static final int selExact = 1;
    public static final int selBoth = 2;
    public static final int selUndefined = 3;

    public int selectType;

    /**
     * SelectType Constructor
     * <br>
     * A selected type can be defined as
     * <ul>
     * <li>   SelectType selectType = new SelectType(SelectType.SelBoth);
     * </ul>
     * and subsequently used as
     * <ul>
     * <li>   if (selectType.selectType == SelectType.SelBoth) ....
     * </ul>
     *
     * @param _selectType The possible types for selection
     */

    public SelectType(int _selectType) {
        selectType = _selectType;
    }

    public String toString() {

        switch (selectType) {
            case selRange:
                return "selRange";
            case selExact:
                return "selExact";
            case selBoth:
                return "selBoth";
            case selUndefined:
                return "Undefined";
        }
        return ("Unexpected SelectType " + selectType);
    }
}
