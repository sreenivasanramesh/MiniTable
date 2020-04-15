package iterator;

import BigT.Map;
import global.AttrType;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;

import java.io.IOException;

/**
 * Jtuple has the appropriate types.
 * Jtuple already has its setHdr called to setup its vital stas.
 */

public class Projection {
    /**
     * Tuple t1 and Tuple t2 will be joined, and the result
     * will be stored in Tuple Jtuple,before calling this mehtod.
     * we know that this two tuple can join in the common field
     *
     * @param t1         The Tuple will be joined with t2
     * @param type1[]    The array used to store the each attribute type
     * @param t2         The Tuple will be joined with t1
     * @param type2[]    The array used to store the each attribute type
     * @param Jtuple     the returned Tuple
     * @param perm_mat[] shows what input fields go where in the output tuple
     * @param nOutFlds   number of outer relation field
     * @throws UnknowAttrType                 attrbute type does't match
     * @throws FieldNumberOutOfBoundException field number exceeds limit
     * @throws IOException                    some I/O fault
     */
    public static void Join(Map t1, AttrType[] type1,
                            Map t2, AttrType[] type2,
                            Map Jtuple, FldSpec[] perm_mat,
                            int nOutFlds
    )
            throws UnknowAttrType,
            FieldNumberOutOfBoundException,
            IOException {


        for (int i = 0; i < nOutFlds; i++) {
            switch (perm_mat[i].relation.key) {
                case RelSpec.outer:        // Field of outer (t1)
                    switch (type1[perm_mat[i].offset - 1].attrType) {
                        case AttrType.attrInteger:
                            // TODO: need getIntFld, getFloFld and getStrFld in Map.java
                            Jtuple.setIntFld(i + 1, t1.getIntFld(perm_mat[i].offset));
                            break;
                        case AttrType.attrReal:
                            Jtuple.setFloFld(i + 1, t1.getFloFld(perm_mat[i].offset));
                            break;
                        case AttrType.attrString:
                            Jtuple.setStrFld(i + 1, t1.getStrFld(perm_mat[i].offset));
                            break;
                        default:

                            throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull");

                    }
                    break;

                case RelSpec.innerRel:        // Field of inner (t2)
                    switch (type2[perm_mat[i].offset - 1].attrType) {
                        case AttrType.attrInteger:
                            Jtuple.setIntFld(i + 1, t2.getIntFld(perm_mat[i].offset));
                            break;
                        case AttrType.attrReal:
                            Jtuple.setFloFld(i + 1, t2.getFloFld(perm_mat[i].offset));
                            break;
                        case AttrType.attrString:
                            Jtuple.setStrFld(i + 1, t2.getStrFld(perm_mat[i].offset));
                            break;
                        default:

                            throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull");

                    }
                    break;
            }
        }
        return;
    }


    /**
     * Tuple t1 will be projected
     * the result will be stored in Tuple Jtuple
     *
     * @param t1         The Tuple will be projected
     * @param type1[]    The array used to store the each attribute type
     * @param Jtuple     the returned Tuple
     * @param perm_mat[] shows what input fields go where in the output tuple
     * @param nOutFlds   number of outer relation field
     * @throws UnknowAttrType                 attrbute type doesn't match
     * @throws WrongPermat                    wrong FldSpec argument
     * @throws FieldNumberOutOfBoundException field number exceeds limit
     * @throws IOException                    some I/O fault
     */

    public static void Project(Map t1, AttrType[] type1,
                               Map Jtuple, FldSpec[] perm_mat,
                               int nOutFlds
    )
            throws UnknowAttrType,
            WrongPermat,
            FieldNumberOutOfBoundException,
            IOException {


        for (int i = 0; i < nOutFlds; i++) {
            switch (perm_mat[i].relation.key) {
                case RelSpec.outer:      // Field of outer (t1)
                    switch (type1[perm_mat[i].offset - 1].attrType) {
                        case AttrType.attrInteger:
                            Jtuple.setIntFld(i + 1, t1.getIntFld(perm_mat[i].offset));
                            break;
                        case AttrType.attrReal:
                            Jtuple.setFloFld(i + 1, t1.getFloFld(perm_mat[i].offset));
                            break;
                        case AttrType.attrString:
                            Jtuple.setStrFld(i + 1, t1.getStrFld(perm_mat[i].offset));
                            break;
                        default:

                            throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull");

                    }
                    break;

                default:

                    throw new WrongPermat("something is wrong in perm_mat");

            }
        }
        return;
    }


    //overloading with maps, cause I don't want to remove the older project and break some internal feature
    public static void Project(Map mapObj, AttrType[] type1,
                               Map JMap, FldSpec[] perm_mat
    )
            throws UnknowAttrType,
            WrongPermat,
            FieldNumberOutOfBoundException,
            IOException {


        for (int i = 0; i < 4; i++) {
            switch (perm_mat[i].relation.key) {
                case RelSpec.outer:      // Field of outer (t1)
                    switch (type1[perm_mat[i].offset - 1].attrType) {
                        case AttrType.attrInteger:
                            JMap.setTimeStamp(mapObj.getTimeStamp());
                            break;
                        //case AttrType.attrReal:
                        //    JMap.setFloFld(i + 1, mapObj.getFloFld(perm_mat[i].offset));
                        //    break;
                        case AttrType.attrString:
                            JMap.setStrFld(i + 1, mapObj.getStringField((short) perm_mat[i].offset));
                            break;
                        default:

                            throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull");

                    }
                    break;

                default:

                    throw new WrongPermat("something is wrong in perm_mat");

            }
        }
        return;
    }

}


