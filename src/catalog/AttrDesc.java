//------------------------------------
// AttrDesc.java
//
// Ning Wang, April,24  1998
//-------------------------------------

package catalog;

import global.*;

// AttrDesc class: schema of attribute catalog:
public class AttrDesc {
    String relName;                       // relation name
    String attrName;                      // attribute name
    int attrOffset = 0;                  // attribute offset
    int attrPos = 0;                     // attribute position
    AttrType attrType;                    // attribute type
    int attrLen = 0;                     // attribute length
    int indexCnt = 0;                    // number of indexes
    attrData minVal;                      // min max key values
    attrData maxVal;
};


