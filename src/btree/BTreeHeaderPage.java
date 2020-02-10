/*
 * @(#) BTIndexPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu)
 *
 */


package btree;

import java.io.*;

import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;

/**
 * Intefrace of a B+ tree index header page.
 * Here we use a HFPage as head page of the file
 * Inside the headpage, Logicaly, there are only seven
 * elements inside the head page, they are
 * magic0, rootId, keyType, maxKeySize, deleteFashion,
 * and type(=NodeType.BTHEAD)
 */
class BTreeHeaderPage extends HFPage {

    void setPageId(PageId pageno)
            throws IOException {
        setCurPage(pageno);
    }

    PageId getPageId()
            throws IOException {
        return getCurPage();
    }

    /**
     * set the magic0
     *
     * @param magic magic0 will be set to be equal to magic
     */
    void set_magic0(int magic)
            throws IOException {
        setPrevPage(new PageId(magic));
    }


    /**
     * get the magic0
     */
    int get_magic0()
            throws IOException {
        return getPrevPage().pid;
    }

    ;

    /**
     * set the rootId
     */
    void set_rootId(PageId rootID)
            throws IOException {
        setNextPage(rootID);
    }

    ;

    /**
     * get the rootId
     */
    PageId get_rootId()
            throws IOException {
        return getNextPage();
    }

    /**
     * set the key type
     */
    void set_keyType(short key_type)
            throws IOException {
        setSlot(3, (int) key_type, 0);
    }

    /**
     * get the key type
     */
    short get_keyType()
            throws IOException {
        return (short) getSlotLength(3);
    }

    /**
     * get the max keysize
     */
    void set_maxKeySize(int key_size)
            throws IOException {
        setSlot(1, key_size, 0);
    }

    /**
     * set the max keysize
     */
    int get_maxKeySize()
            throws IOException {
        return getSlotLength(1);
    }

    /**
     * set the delete fashion
     */
    void set_deleteFashion(int fashion)
            throws IOException {
        setSlot(2, fashion, 0);
    }

    /**
     * get the delete fashion
     */
    int get_deleteFashion()
            throws IOException {
        return getSlotLength(2);
    }


    /**
     * pin the page with pageno, and get the corresponding SortedPage
     */
    public BTreeHeaderPage(PageId pageno)
            throws ConstructPageException {
        super();
        try {

            SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/);
        } catch (Exception e) {
            throw new ConstructPageException(e, "pinpage failed");
        }
    }

    /**
     * associate the SortedPage instance with the Page instance
     */
    public BTreeHeaderPage(Page page) {

        super(page);
    }


    /**
     * new a page, and associate the SortedPage instance with the Page instance
     */
    public BTreeHeaderPage()
            throws ConstructPageException {
        super();
        try {
            Page apage = new Page();
            PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
            if (pageId == null)
                throw new ConstructPageException(null, "new page failed");
            this.init(pageId, apage);

        } catch (Exception e) {
            throw new ConstructPageException(e, "construct header page failed");
        }
    }

} // end of BTreeHeaderPage
