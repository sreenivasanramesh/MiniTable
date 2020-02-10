/*
 * @(#) BTIndexPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu)
 *
 */


package btree;

import java.io.*;
import java.lang.*;

import global.*;
import diskmgr.*;
import heap.*;

/**
 * A BTIndexPage is an index page on a B+ tree.  It holds abstract
 * {key, PageId} pairs; it doesn't know anything about the keys
 * (their lengths or their types), instead relying on the abstract
 * interface in BT.java
 * See those files for our {key,data} pairing interface and implementation.
 */
public class BTIndexPage extends BTSortedPage {


    /**
     * pin the page with pageno, and get the corresponding BTIndexPage,
     * also it sets the type of node to be NodeType.INDEX.
     *
     * @param pageno  Input parameter. To specify which page number the
     *                BTIndexPage will correspond to.
     * @param keyType either AttrType.attrInteger or AttrType.attrString.
     *                Input parameter.
     */
    public BTIndexPage(PageId pageno, int keyType)
    //throws IOException,
    //   ConstructPageException
    {
        super(pageno, keyType);
        setType(NodeType.INDEX);
    }

    /**
     * associate the BTIndexPage instance with the Page instance,
     * also it sets the type of node to be NodeType.INDEX.
     *
     * @param page    input parameter. To specify which page  the
     *                BTIndexPage will correspond to.
     * @param keyType either AttrType.attrInteger or AttrType.attrString.
     *                Input parameter.
     */
    public BTIndexPage(Page page, int keyType)
    //throws IOException,
    //   ConstructPageException
    {
        super(page, keyType);
        setType(NodeType.INDEX);
    }

    /* new a page, associate the BTIndexPage instance with the Page instance,
     * also it sets the type of node to be NodeType.INDEX.
     *@param keyType either AttrType.attrInteger or AttrType.attrString.
     *  Input parameter.
     */
    public BTIndexPage(int keyType)
    //throws IOException,
    //   ConstructPageException
    {
        super(keyType);
        setType(NodeType.INDEX);
    }


    /**
     * It inserts a <key, pageNo> value into the index page,
     *
     * @param key    the key value in <key, pageNO>. Input parameter.
     * @param pageNo the pageNo  in <key, pageNO>. Input parameter.
     * @return It returns the rid where the record is inserted;
     * null if no space left.
     */
    public RID insertKey(KeyClass key, PageId pageNo)
    //throws  IndexInsertRecException
    {
        RID rid;
        KeyDataEntry entry;
        try {
            entry = new KeyDataEntry(key, pageNo);
            rid = super.insertRecord(entry);
            return rid;
        } catch (Exception e) {
            throw new IndexInsertRecException(e, "Insert failed");

        }
    }

    /*  OPTIONAL: fullDeletekey
     * This is optional, and is only needed if you want to do full deletion.
     * Return its RID.  delete key may != key.  But delete key <= key,
     * and the delete key is the first biggest key such that delete key <= key
     *@param key the key used to search. Input parameter.
     * any reason
     *@return  RID of the record deleted. Can not return null.
     */
    RID deleteKey(KeyClass key)
    //throws IndexFullDeleteException
    {
        KeyDataEntry entry;
        RID rid = new RID();


        try {

            entry = getFirst(rid);

            if (entry == null)
                //it is supposed there is at least a record
                throw new IndexFullDeleteException(null, "No records found");


            if (BT.keyCompare(key, entry.key) < 0)
                //it is supposed to not smaller than first key
                throw new IndexFullDeleteException(null, "First key is bigger");


            while (BT.keyCompare(key, entry.key) > 0) {
                entry = getNext(rid);
                if (entry == null)
                    break;
            }

            if (entry == null) rid.slotNo--;
            else if (BT.keyCompare(key, entry.key) != 0)
                rid.slotNo--; // we want to delete the previous key

            deleteSortedRecord(rid);
            return rid;
        } catch (Exception e) {
            throw new IndexFullDeleteException(e, "Full delelte failed");
        }
    } // end of deleteKey


    /*
     * This function encapsulates the search routine to search a
     * BTIndexPage by B++ search algorithm
     *@param key  the key value used in search algorithm. Input parameter.
     *@return It returns the page_no of the child to be searched next.
     *@exception IndexSearchException Index search failed;
     */
    public PageId getPageNoByKey(KeyClass key)
    //throws IndexSearchException
    {
        KeyDataEntry entry;
        int i;

        try {

            for (i = getSlotCnt() - 1; i >= 0; i--) {
                entry = BT.getEntryFromBytes(getpage(), getSlotOffset(i),
                        getSlotLength(i), keyType, NodeType.INDEX);

                if (BT.keyCompare(key, entry.key) >= 0) {
                    return ((IndexData) entry.data).getData();
                }
            }

            return getPrevPage();
        } catch (Exception e) {
            throw new IndexSearchException(e, "Get entry failed");
        }

    } // getPageNoByKey


    /**
     * Iterators.
     * One of the two functions: getFirst and getNext
     * which  provide an iterator interface to the records on a BTIndexPage.
     *
     * @param rid It will be modified and the first rid in the index page
     *            will be passed out by itself. Input and Output parameter.
     * @return return the first KeyDataEntry in the index page.
     * null if NO MORE RECORD
     */
    public KeyDataEntry getFirst(RID rid)
    //throws IteratorException
    {

        KeyDataEntry entry;

        try {
            rid.pageNo = getCurPage();
            rid.slotNo = 0; // begin with first slot

            if (getSlotCnt() == 0) {
                return null;
            }

            entry = BT.getEntryFromBytes(getpage(), getSlotOffset(0),
                    getSlotLength(0),
                    keyType, NodeType.INDEX);

            return entry;
        } catch (Exception e) {
            throw new IteratorException(e, "Get first entry failed");
        }

    } // end of getFirst


    /**
     * Iterators.
     * One of the two functions: getFirst and getNext which  provide an
     * iterator interface to the records on a BTIndexPage.
     *
     * @param rid It will be modified and next rid will be passed out by itself.
     *            Input and Output parameter.
     * @return return the next KeyDataEntry in the index page.
     * null if no more record
     */
    public KeyDataEntry getNext(RID rid)
    //throws  IteratorException
    {
        KeyDataEntry entry;
        int i;
        try {
            rid.slotNo++; //must before any return;
            i = rid.slotNo;

            if (rid.slotNo >= getSlotCnt()) {
                return null;
            }

            entry = BT.getEntryFromBytes(getpage(), getSlotOffset(i),
                    getSlotLength(i),
                    keyType, NodeType.INDEX);

            return entry;
        } catch (Exception e) {
            throw new IteratorException(e, "Get next entry failed");
        }
    } // end of getNext


    /**
     * Left Link
     * You will recall that the index pages have a left-most
     * pointer that is followed whenever the search key value
     * is less than the least key value in the index node. The
     * previous page pointer is used to implement the left link.
     *
     * @return It returns the left most link.
     */
    public PageId getLeftLink()
    //throws IOException
    {
        return getPrevPage();
    }

    /**
     * You will recall that the index pages have a left-most
     * pointer that is followed whenever the search key value
     * is less than the least key value in the index node. The
     * previous page pointer is used to implement the left link.
     * The function sets the left link.
     *
     * @param left the PageId of the left link you wish to set. Input parameter.
     */
    public void setLeftLink(PageId left)
    //throws IOException
    {
        setPrevPage(left);
    }


    /*It is used in full delete
     *@param key the key is used to search. Input parameter.
     *@param pageNo It returns the pageno of the sibling. Input and Output
     *       parameter.
     *@return 0 if no sibling; -1 if left sibling; 1 if right sibling.
     */

    int getSibling(KeyClass key, PageId pageNo)
    //throws IndexFullDeleteException
    {

        try {
            if (getSlotCnt() == 0) // there is no sibling
                return 0;

            int i;
            KeyDataEntry entry;
            for (i = getSlotCnt() - 1; i >= 0; i--) {
                entry = BT.getEntryFromBytes(getpage(), getSlotOffset(i),
                        getSlotLength(i), keyType, NodeType.INDEX);
                if (BT.keyCompare(key, entry.key) >= 0) {
                    if (i != 0) {
                        entry = BT.getEntryFromBytes(getpage(), getSlotOffset(i - 1),
                                getSlotLength(i - 1), keyType, NodeType.INDEX);
                        pageNo.pid = ((IndexData) entry.data).getData().pid;
                        return -1; //left sibling
                    } else {
                        pageNo.pid = getLeftLink().pid;
                        return -1; //left sibling
                    }
                }
            }
            entry = BT.getEntryFromBytes(getpage(), getSlotOffset(0),
                    getSlotLength(0), keyType, NodeType.INDEX);
            pageNo.pid = ((IndexData) entry.data).getData().pid;
            return 1;  //right sibling
        } catch (Exception e) {
            throw new IndexFullDeleteException(e, "Get sibling failed");
        }
    } // end of getSibling  


    /* find the position for old key by findKeyData,
     *  where the  newKey will be returned .
     *@newKey It will replace certain key in index page. Input parameter.
     *@oldKey It helps us to find which key will be replaced by
     * the newKey. Input parameter.
     *@return false if no key was found; true if success.
     */

    boolean adjustKey(KeyClass newKey, KeyClass oldKey)
    //throws IndexFullDeleteException
    {

        try {

            KeyDataEntry entry;
            entry = findKeyData(oldKey);
            if (entry == null) return false;

            RID rid = deleteKey(entry.key);
            if (rid == null) throw new IndexFullDeleteException(null, "Rid is null");

            rid = insertKey(newKey, ((IndexData) entry.data).getData());
            if (rid == null) throw new IndexFullDeleteException(null, "Rid is null");

            return true;
        } catch (Exception e) {
            throw new IndexFullDeleteException(e, "Adjust key failed");
        }
    } // end of adjustKey


    /* find entry for key by B+ tree algorithm,
     * but entry.key may not equal KeyDataEntry.key returned.
     *@param key input parameter.
     *@return return that entry if found; otherwise return null;
     *
     */
    KeyDataEntry findKeyData(KeyClass key)
    //throws IndexSearchException
    {
        KeyDataEntry entry;

        try {

            for (int i = getSlotCnt() - 1; i >= 0; i--) {
                entry = BT.getEntryFromBytes(getpage(), getSlotOffset(i),
                        getSlotLength(i), keyType, NodeType.INDEX);

                if (BT.keyCompare(key, entry.key) >= 0) {
                    return entry;
                }
            }
            return null;
        } catch (Exception e) {
            throw new IndexSearchException(e, "finger key data failed");
        }
    } // end of findKeyData     


    /* find a key  by B++ algorithm,
     * but returned key may not equal the key passed in.
     *@param key input parameter.
     *@return return that key if found; otherwise return null;
     *
     */
    KeyClass findKey(KeyClass key)
    //throws IndexSearchException
    {
        return findKeyData(key).key;
    }


    /*It is used in full delete
     *@param indexPage the sibling page of this. Input parameter.
     *@param parentIndexPage the parant of indexPage and this. Input parameter.
     *@param direction -1 if "this" is left sibling of indexPage ;
     *      1 if "this" is right sibling of indexPage. Input parameter.
     *@param deletedKey the key which was already deleted, and cause
     *     redistribution. Input parameter.
     *@return true if redistrbution success. false if we can not redistribute them.
     */
    boolean redistribute(BTIndexPage indexPage, BTIndexPage parentIndexPage,
                         int direction, KeyClass deletedKey)
    //throws RedistributeException
    {

        // assertion: indexPage and parentIndexPage are  pinned
        try {
            boolean st;
            if (direction == -1) { // 'this' is the left sibling of indexPage
                if ((getSlotLength(getSlotCnt() - 1) + available_space()) >
                        ((MAX_SPACE - DPFIXED) / 2)) {
                    // cannot spare a record for its underflow sibling
                    return false;
                } else {
                    // get its sibling's first record's key
                    RID dummyRid = new RID();
                    KeyDataEntry firstEntry, lastEntry;
                    firstEntry = indexPage.getFirst(dummyRid);

                    // get the entry pointing to the right sibling

                    KeyClass splitKey = parentIndexPage.findKey(firstEntry.key);

                    // get the leftmost child pointer of the right sibling
                    PageId leftMostPageId = indexPage.getLeftLink();

                    // insert  <splitKey,leftMostPageId>  to its sibling
                    indexPage.insertKey(splitKey, leftMostPageId);

                    // get the last record of itself
                    lastEntry = BT.getEntryFromBytes(getpage(), getSlotOffset(getSlotCnt() - 1),
                            getSlotLength(getSlotCnt() - 1), keyType, NodeType.INDEX);

                    // set sibling's leftmostchild to be lastPageId
                    indexPage.setLeftLink(((IndexData) (lastEntry.data)).getData());

                    // delete the last record from the old page
                    RID delRid = new RID();
                    delRid.pageNo = getCurPage();
                    delRid.slotNo = getSlotCnt() - 1;

                    if (deleteSortedRecord(delRid) == false)
                        throw new RedistributeException(null, "Delete record failed");

                    // adjust the entry pointing to sibling in its parent
                    if (deletedKey != null)
                        st = parentIndexPage.adjustKey(lastEntry.key, deletedKey);

                    else
                        st = parentIndexPage.adjustKey(lastEntry.key, splitKey);
                    if (st == false)
                        throw new RedistributeException(null, "adjust key failed");
                    return true;
                }
            } else { // 'this' is the right sibling of indexPage
                if ((getSlotLength(0) + available_space()) > ((MAX_SPACE - DPFIXED) / 2)) {
                    // cannot spare a record for its underflow sibling
                    return false;
                } else {
                    // get the first record
                    KeyDataEntry firstEntry;
                    firstEntry = BT.getEntryFromBytes(getpage(),
                            getSlotOffset(0),
                            getSlotLength(0), keyType, NodeType.INDEX);

                    // get its leftmost child pointer
                    PageId leftMostPageId = getLeftLink();

                    // get the entry in its parent pointing to itself
                    KeyClass splitKey;
                    splitKey = parentIndexPage.findKey(firstEntry.key);

                    // insert <split, leftMostPageId> to its left sibling

                    indexPage.insertKey(splitKey, leftMostPageId);


                    // set its new leftmostchild
                    setLeftLink(((IndexData) (firstEntry.data)).getData());

                    // delete the first record
                    RID delRid = new RID();
                    delRid.pageNo = getCurPage();
                    delRid.slotNo = 0;
                    if (deleteSortedRecord(delRid) == false)
                        throw new RedistributeException(null, "delete record failed");

                    // adjust the entry pointing to itself in its parent
                    if (parentIndexPage.adjustKey(firstEntry.key, splitKey) == false)
                        throw new RedistributeException(null, "adjust key failed");
                    return true;
                }
            } //else
        } //try
        catch (Exception e) {
            throw new RedistributeException(e, "redistribute failed");
        }
    } // end of redistribute  
};
