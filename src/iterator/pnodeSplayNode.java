
package iterator;

/**
 * An element in the binary tree.
 * including pointers to the children, the parent in addition to the item.
 */
public class pnodeSplayNode {
    /**
     * a reference to the element in the node
     */
    public pnode item;

    /**
     * the left child pointer
     */
    public pnodeSplayNode lt;

    /**
     * the right child pointer
     */
    public pnodeSplayNode rt;

    /**
     * the parent pointer
     */
    public pnodeSplayNode par;

    /**
     * class constructor, sets all pointers to <code>null</code>.
     *
     * @param h the element in this node
     */
    public pnodeSplayNode(pnode h) {
        item = h;
        lt = null;
        rt = null;
        par = null;
    }

    /**
     * class constructor, sets all pointers.
     *
     * @param h the element in this node
     * @param l left child pointer
     * @param r right child pointer
     */
    public pnodeSplayNode(pnode h, pnodeSplayNode l, pnodeSplayNode r) {
        item = h;
        lt = l;
        rt = r;
        par = null;
    }

    /**
     * a static dummy node for use in some methods
     */
    public static pnodeSplayNode dummy = new pnodeSplayNode(null);

}

