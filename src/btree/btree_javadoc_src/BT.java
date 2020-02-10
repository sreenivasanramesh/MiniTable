/*
 * @(#) BT.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu)
 *
 */
package btree;

import java.io.*;
import java.lang.*;

import global.*;
import diskmgr.*;
import bufmgr.*;
import heap.*;

/**
 * This file contains, among some debug utilities, the interface to our
 * key and data abstraction.  The BTLeafPage and BTIndexPage code
 * know very little about the various key types.
 * <p>
 * Essentially we provide a way for those classes to package and
 * unpackage <key,data> pairs in a space-efficient manner.  That is, the
 * packaged result contains the key followed immediately by the data value;
 * No padding bytes are used (not even for alignment).
 * <p>
 * Furthermore, the BT<*>Page classes need
 * not know anything about the possible AttrType values, since all
 * their processing of <key,data> pairs is done by the functions
 * declared here.
 * <p>
 * In addition to packaging/unpacking of <key,value> pairs, we
 * provide a keyCompare function for the (obvious) purpose of
 * comparing keys of arbitrary type (as long as they are defined
 * here).
 * <p>
 * For debug utilities, you could implement methods to print any page
 * or the whole B+ tree structure or all leaf pages.
 */
public class BT implements GlobalConst {

    /**
     * It compares two keys.
     *
     * @param key1 the first key to compare. Input parameter.
     * @param key2 the second key to compare. Input parameter.
     * @return return negative if key1 less than key2; positive if key1 bigger
     * than  key2;
     * 0 if key1=key2.
     * @throws KeyNotMatchException key is not IntegerKey or StringKey class
     */
    public final static int keyCompare(KeyClass key1, KeyClass key2)
            throws KeyNotMatchException {
        if ((key1 instanceof IntegerKey) && (key2 instanceof IntegerKey)) {

            return (((IntegerKey) key1).getKey()).intValue()
                    - (((IntegerKey) key2).getKey()).intValue();
        } else if ((key1 instanceof StringKey) && (key2 instanceof StringKey)) {
            return ((StringKey) key1).getKey().compareTo(((StringKey) key2).getKey());
        } else {
            throw new KeyNotMatchException(null, "key types do not match");
        }
    }


    /**
     * It gets the length of the key
     *
     * @param key specify the key whose length will be calculated.
     *            Input parameter.
     * @return return the length of the key
     * @throws KeyNotMatchException key is neither StringKey nor  IntegerKey
     * @throws IOException          error  from the lower layer
     */
    public final static int getKeyLength(KeyClass key)
            throws KeyNotMatchException,
            IOException {
        if (key instanceof StringKey) {

            OutputStream out = new ByteArrayOutputStream();
            DataOutputStream outstr = new DataOutputStream(out);
            outstr.writeUTF(((StringKey) key).getKey());
            return outstr.size();
        } else if (key instanceof IntegerKey)
            return 4;
        else throw new KeyNotMatchException(null, "key types do not match");
    }


    /**
     * It gets the length of the data
     *
     * @param pageType NodeType.LEAF or  NodeType.INDEX. Input parameter.
     * @return return 8 if it is of NodeType.LEA;
     * return 4 if it is of NodeType.INDEX.
     * @throws NodeNotMatchException pageType is neither NodeType.LEAF
     *                               nor NodeType.INDEX.
     */
    public final static int getDataLength(short pageType)
            throws NodeNotMatchException {
        if (pageType == NodeType.LEAF)
            return 8;
        else if (pageType == NodeType.INDEX)
            return 4;
        else throw new NodeNotMatchException(null, "key types do not match");
    }

    /**
     * It gets the length of the (key,data) pair in leaf or index page.
     *
     * @param key      an object of KeyClass.  Input parameter.
     * @param pageType NodeType.LEAF or  NodeType.INDEX. Input parameter.
     * @return return the lenrth of the (key,data) pair.
     * @throws KeyNotMatchException  key is neither StringKey nor  IntegerKey
     * @throws NodeNotMatchException pageType is neither NodeType.LEAF
     *                               nor NodeType.INDEX.
     * @throws IOException           error from the lower layer
     */
    public final static int getKeyDataLength(KeyClass key, short pageType)
            throws KeyNotMatchException,
            NodeNotMatchException,
            IOException {
        return getKeyLength(key) + getDataLength(pageType);
    }

    /**
     * It gets an keyDataEntry from bytes array and position
     *
     * @param from     It's a bytes array where KeyDataEntry will come from.
     *                 Input parameter.
     * @param offset   the offset in the bytes. Input parameter.
     * @param keyType  It specifies the type of key. It can be
     *                 AttrType.attrString or AttrType.attrInteger.
     *                 Input parameter.
     * @param nodeType It specifes NodeType.LEAF or NodeType.INDEX.
     *                 Input parameter.
     * @param length   The length of (key, data) in byte array "from".
     *                 Input parameter.
     * @return return a KeyDataEntry object
     * @throws KeyNotMatchException  key is neither StringKey nor  IntegerKey
     * @throws NodeNotMatchException nodeType is neither NodeType.LEAF
     *                               nor NodeType.INDEX.
     * @throws ConvertException      error from the lower layer
     */
    public final static KeyDataEntry getEntryFromBytes(byte[] from, int offset,
                                                       int length, int keyType, short nodeType)
            throws KeyNotMatchException,
            NodeNotMatchException,
            ConvertException {
        KeyClass key;
        DataClass data;
        int n;
        try {

            if (nodeType == NodeType.INDEX) {
                n = 4;
                data = new IndexData(Convert.getIntValue(offset + length - 4, from));
            } else if (nodeType == NodeType.LEAF) {
                n = 8;
                RID rid = new RID();
                rid.slotNo = Convert.getIntValue(offset + length - 8, from);
                rid.pageNo = new PageId();
                rid.pageNo.pid = Convert.getIntValue(offset + length - 4, from);
                data = new LeafData(rid);
            } else throw new NodeNotMatchException(null, "node types do not match");

            if (keyType == AttrType.attrInteger) {
                key = new IntegerKey(new Integer
                        (Convert.getIntValue(offset, from)));
            } else if (keyType == AttrType.attrString) {
                //System.out.println(" offset  "+ offset + "  " + length + "  "+n);
                key = new StringKey(Convert.getStrValue(offset, from, length - n));
            } else
                throw new KeyNotMatchException(null, "key types do not match");

            return new KeyDataEntry(key, data);

        } catch (IOException e) {
            throw new ConvertException(e, "convert faile");
        }
    }


    /**
     * It convert a keyDataEntry to byte[].
     *
     * @param entry specify  the data entry. Input parameter.
     * @return return a byte array with size equal to the size of (key,data).
     * @throws KeyNotMatchException  entry.key is neither StringKey nor  IntegerKey
     * @throws NodeNotMatchException entry.data is neither LeafData nor IndexData
     * @throws ConvertException      error from converting from entry to bytes
     */
    public final static byte[] getBytesFromEntry(KeyDataEntry entry)
            throws KeyNotMatchException,
            NodeNotMatchException,
            ConvertException {
        byte[] data;
        int n, m;
        try {
            n = getKeyLength(entry.key);
            m = n;
            if (entry.data instanceof IndexData)
                n += 4;
            else if (entry.data instanceof LeafData)
                n += 8;

            data = new byte[n];

            if (entry.key instanceof IntegerKey) {
                Convert.setIntValue(((IntegerKey) entry.key).getKey().intValue(),
                        0, data);
            } else if (entry.key instanceof StringKey) {
                Convert.setStrValue(((StringKey) entry.key).getKey(),
                        0, data);
            } else throw new KeyNotMatchException(null, "key types do not match");

            if (entry.data instanceof IndexData) {
                Convert.setIntValue(((IndexData) entry.data).getData().pid,
                        m, data);
            } else if (entry.data instanceof LeafData) {
                Convert.setIntValue(((LeafData) entry.data).getData().slotNo,
                        m, data);
                Convert.setIntValue(((LeafData) entry.data).getData().pageNo.pid,
                        m + 4, data);

            } else throw new NodeNotMatchException(null, "node types do not match");
            return data;
        } catch (IOException e) {
            throw new ConvertException(e, "convert failed");
        }
    }

} // end of BT





