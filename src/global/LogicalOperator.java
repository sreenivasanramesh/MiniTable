package global;

/**
 * Enumeration class for LogicalOperator
 */

public class LogicalOperator {

    public static final int lopAND = 0;
    public static final int lopOR = 1;
    public static final int lopNOT = 2;

    public int logicalOperator;

    /**
     * LogicalOperator Constructor
     * <br>
     * A logical operator can be defined as
     * <ul>
     * <li>   LogicalOperator logicalOperator =
     * <br>                 new LogicalOperator(LogicalOperator.lopOR);
     * </ul>
     * and subsequently used as
     * <ul>
     * <li>   if (logicalOperator.logicalOperator == LogicalOperator.lopOR) ....
     * </ul>
     *
     * @param _logicalOperator The available logical operators
     */

    public LogicalOperator(int _logicalOperator) {
        logicalOperator = _logicalOperator;
    }

    public String toString() {

        switch (logicalOperator) {
            case lopAND:
                return "lopAND";
            case lopOR:
                return "lopOR";
            case lopNOT:
                return "lopNOT";
        }
        return ("Unexpected LogicalOperator " + logicalOperator);
    }

}
