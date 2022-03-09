/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "btree.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "filescan.h"

//#define DEBUG

namespace badgerdb {

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string &relationName,
                       std::string &outIndexName, BufMgr *bufMgrIn,
                       const int attrByteOffset, const Datatype attrType) {
  // initalize vars
  this->bufMgr = bufMgrIn;
  this->headerPageNum = 1;
  this->attrByteOffset = attrByteOffset;
  this->attributeType = attrType;
  this->scanExecuting = false;
  this->leafOccupancy = INTARRAYLEAFSIZE;
  this->nodeOccupancy = INTARRAYNONLEAFSIZE;

  // Index File Name
  std::ostringstream idxStr;
  idxStr << relationName << '.' << attrByteOffset;
  outIndexName = idxStr.str();
  try {
    // if the index already exists
    BlobFile *indexFile = new BlobFile(outIndexName, false);
    this->file = indexFile;
    Page *temp;
    IndexMetaInfo *header;
    bufMgr->readPage(file, headerPageNum, temp);

    // check vailidy of index
    header = reinterpret_cast<IndexMetaInfo *>(temp);
    if (std::string(header->relationName) != relationName ||
        header->attrByteOffset != attrByteOffset ||
        header->attrType != attrType) {
      throw BadIndexInfoException("Invalid index was found!");
    }
    // update root
    this->rootPageNum = header->rootPageNo;
    bufMgr->unPinPage(file, headerPageNum, false);
  } catch (FileNotFoundException ex) {
    // no pre-existing index
    BlobFile *indexFile = new BlobFile(outIndexName, true);
    this->file = indexFile;
    this->rootPageNum = 2;

    // create header page
    Page *temp;
    IndexMetaInfo *header;
    bufMgr->allocPage(file, headerPageNum, temp);
    header = reinterpret_cast<IndexMetaInfo *>(temp);

    // Initialize empty root
    NonLeafNodeInt *root;
    bufMgr->allocPage(file, rootPageNum, temp);

    root = reinterpret_cast<NonLeafNodeInt *>(temp);
    std::fill(root->keyArray, root->keyArray + nodeOccupancy, INT_MAX);
    std::fill(root->pageNoArray, root->pageNoArray + nodeOccupancy, 0);
    root->level = 1;

    // Initialize first leaf
    LeafNodeInt *leaf;
    PageId leafNum;
    bufMgr->allocPage(file, leafNum, temp);

    leaf = reinterpret_cast<LeafNodeInt *>(temp);
    std::fill(leaf->keyArray, leaf->keyArray + nodeOccupancy, INT_MAX);
    leaf->rightSibPageNo = 0;

    root->pageNoArray[0] = leafNum;

    bufMgr->unPinPage(file, rootPageNum, true);
    bufMgr->unPinPage(file, leafNum, true);

    // fill header info

    relationName.copy(header->relationName, 20);
    header->attrByteOffset = attrByteOffset;
    header->attrType = attrType;
    header->rootPageNo = this->rootPageNum;
    bufMgr->unPinPage(file, headerPageNum, true);

    // populate index
    FileScan scanner(relationName, bufMgr);
    std::string recordStr;
    RecordId rid;
    const char *record;
    const void *key;
    while (true) {
      try {
        scanner.scanNext(rid);
        recordStr = scanner.getRecord();
        record = recordStr.c_str();
        key = record + attrByteOffset;
        this->insertEntry(key, rid);
      } catch (EndOfFileException x) {
        break;
      }
    }
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
  // endscan
  if (scanExecuting) {
    endScan();
  }

  // flush the file
  bufMgr->flushFile(this->file);

  delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
  KeyPagePair toCheck =
      recursiveInsert(*((int *)key), rid, false, this->rootPageNum);

  // there is a root split
  if (toCheck.pageId) {
    Page *temp;
    PageId oldRoot = this->rootPageNum;
    this->bufMgr->allocPage(this->file, this->rootPageNum, temp);
    // root node
    NonLeafNodeInt *newRoot;
    newRoot = reinterpret_cast<NonLeafNodeInt *>(temp);

    std::fill(newRoot->keyArray, newRoot->keyArray + nodeOccupancy, INT_MAX);
    std::fill(newRoot->pageNoArray, newRoot->pageNoArray + nodeOccupancy, 0);
    newRoot->level = 0;

    newRoot->keyArray[0] = toCheck.key;
    newRoot->pageNoArray[0] = oldRoot;
    newRoot->pageNoArray[1] = toCheck.pageId;

    IndexMetaInfo *header;
    bufMgr->readPage(file, headerPageNum, temp);
    header = reinterpret_cast<IndexMetaInfo *>(temp);

    header->rootPageNo = this->rootPageNum;
    bufMgr->unPinPage(file, headerPageNum, true);
    bufMgr->unPinPage(file, rootPageNum, true);
  }
}

// ------------------------------------------------------------------------------
// Recursicve insert
// Returns a KeyPagePair to push up when splitting
// ------------------------------------------------------------------------------
KeyPagePair BTreeIndex::recursiveInsert(int key, const RecordId rid,
                                        const bool isLeaf, PageId currPageId) {
  KeyPagePair nullPair;
  nullPair.pageId = 0;
  // get the page
  Page *temp;
  this->bufMgr->readPage(this->file, currPageId, temp);

  if (!isLeaf)  // look for leaf
  {
    NonLeafNodeInt *currNode;
    currNode = reinterpret_cast<NonLeafNodeInt *>(temp);

    // index to look for in the page array
    int index = 0;
    for (int currKey : currNode->keyArray) {
      if (key < currKey) {
        break;
      }
      index++;
    }

    // check whether the next node is a leaf or not
    bool isNextLeaf = false;
    if (currNode->level == 1) {
      isNextLeaf = true;
    }

    // recursive call. check if splitting has occurred
    KeyPagePair pairToAdd =
        recursiveInsert(key, rid, isNextLeaf, currNode->pageNoArray[index]);
    if (pairToAdd.pageId) {
      // check if there is space for the key page pair in the current array

      // there does not need to be an internal node split and the middle value
      // of the old node is being pushed up
      if (currNode->getNumKeys() < nodeOccupancy) {
        simpleNodeInsert(pairToAdd.key, pairToAdd.pageId, currNode);
        this->bufMgr->unPinPage(this->file, currPageId, true);
        return nullPair;
      }

      // there is an internal node split
      else {
        // new page to create
        Page *newPage;
        PageId newInternalId;
        this->bufMgr->allocPage(this->file, newInternalId, newPage);
        NonLeafNodeInt *newInternalNode =
            reinterpret_cast<NonLeafNodeInt *>(newPage);

        // fill the arrays
        std::fill(newInternalNode->keyArray,
                  newInternalNode->keyArray + nodeOccupancy, INT_MAX);
        std::fill(newInternalNode->pageNoArray,
                  newInternalNode->pageNoArray + nodeOccupancy, 0);

        // maintain the level of the pred
        newInternalNode->level = currNode->level;

        // create pair to send up to the new node
        // PUSH key
        KeyPagePair newPair;
        newPair.pageId = newInternalId;
        newPair.key = currNode->keyArray[(nodeOccupancy) / 2];
        currNode->keyArray[(nodeOccupancy) / 2] = INT_MAX;

        // split
        int j = 0;
        // copy values from the arrays up to occupancy < 2
        for (int i = nodeOccupancy / 2 + 1; i < nodeOccupancy; i++) {
          newInternalNode->pageNoArray[j] = currNode->pageNoArray[i];
          newInternalNode->keyArray[j] = currNode->keyArray[i];
          j++;

          currNode->pageNoArray[i] = 0;
          currNode->keyArray[i] = INT_MAX;
        }

        // set the last page Id on the new node
        newInternalNode->pageNoArray[j] = currNode->pageNoArray[nodeOccupancy];
        currNode->pageNoArray[nodeOccupancy] = 0;

        // Insert the new key value pair into the node of choice
        if (pairToAdd.key < newPair.key) {
          simpleNodeInsert(pairToAdd.key, pairToAdd.pageId, currNode);
        } else {
          simpleNodeInsert(pairToAdd.key, pairToAdd.pageId, newInternalNode);
        }
        this->bufMgr->unPinPage(this->file, currPageId, true);
        this->bufMgr->unPinPage(this->file, newInternalId, true);

        return newPair;
      }
    }

    this->bufMgr->unPinPage(this->file, currPageId, true);
    return nullPair;
  } else  // insert into leaf
  {
    LeafNodeInt *currNode;
    currNode = reinterpret_cast<LeafNodeInt *>(temp);

    // check if there is enough space available on the leaf
    // I.E NO SPLITTING
    if (currNode->getNumKeys() < INTARRAYLEAFSIZE) {
      simpleLeafInsert(key, rid, currNode);
      this->bufMgr->unPinPage(this->file, currPageId, true);
      return nullPair;
    } else  // SPLITTING TIME BABY
    {
      // remember with leaf nodes, we COPY up instead of pushing up
      // create sibling
      Page *newSibPage;
      LeafNodeInt *newSibNode;
      PageId sibId;
      this->bufMgr->allocPage(this->file, sibId, newSibPage);
      newSibNode = reinterpret_cast<LeafNodeInt *>(newSibPage);

      std::fill(newSibNode->keyArray, newSibNode->keyArray + INTARRAYLEAFSIZE,
                INT_MAX);

      // copy contents of old array into new array
      for (int i = INTARRAYLEAFSIZE / 2; i < INTARRAYLEAFSIZE; i++) {
        newSibNode->keyArray[i - (INTARRAYLEAFSIZE / 2)] =
            currNode->keyArray[i];
        newSibNode->ridArray[i - (INTARRAYLEAFSIZE / 2)] =
            currNode->ridArray[i];

        currNode->keyArray[i] = INT_MAX;
      }

      // have new sibling point to original nodes neighbor
      newSibNode->rightSibPageNo = currNode->rightSibPageNo;
      currNode->rightSibPageNo = sibId;

      // now we insert the value as we did before
      if (newSibNode->keyArray[0] > key)  // insert into orig sib
      {
        simpleLeafInsert(key, rid, currNode);
      } else  // insert into new sib
      {
        simpleLeafInsert(key, rid, newSibNode);
      }

      // close both nodes and push up inserted values
      KeyPagePair pair;
      pair.key = newSibNode->keyArray[0];
      pair.pageId = sibId;

      // unpinPages
      this->bufMgr->unPinPage(this->file, currPageId, true);
      this->bufMgr->unPinPage(this->file, sibId, true);

      return pair;
    }
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void *lowValParm, const Operator lowOpParm,
                           const void *highValParm, const Operator highOpParm) {
  //check validity
  if (!(lowOpParm == GT || lowOpParm == GTE) ||
      !(highOpParm == LT || highOpParm == LTE)) {
    throw BadOpcodesException();
  }
  
  //parse params
  lowValInt = *(int *)lowValParm;
  highValInt = *(int *)highValParm;
  lowValDouble = 1.0 * lowValInt;
  highValDouble = 1.0 * highValInt;
  lowValString = std::to_string(lowValInt);
  highValString = std::to_string(highValInt);
  lowOp = lowOpParm;
  highOp = highOpParm;

  if (lowValInt > highValInt) {
    throw BadScanrangeException();
  }

  scanExecuting = true;

  int lb = lowValInt;
  int ub = highValInt;
  if (lowOp == GT) {
    lb++;
  }
  if (highOp == LT) {
    ub--;
  }

  //itterate down to leaf
  Page *temp;
  NonLeafNodeInt *curr;
  bufMgr->readPage(file, rootPageNum, temp);
  curr = reinterpret_cast<NonLeafNodeInt *>(temp);
  int currNo = rootPageNum;
  int next;
  while (curr->level == 0) {
    next = curr->pageNoArray[nodeOccupancy];
    for (int i = 0; i < nodeOccupancy; i++) {
      if (lb < curr->keyArray[i]) {
        next = curr->pageNoArray[i];
        break;
      }
    }
    if (next == 0) {
      scanExecuting = false;
      throw NoSuchKeyFoundException();
    }
    bufMgr->unPinPage(file, currNo, false);

    currNo = next;
    bufMgr->readPage(file, currNo, temp);
    curr = reinterpret_cast<NonLeafNodeInt *>(temp);
  }

  //find leaf
  next = curr->pageNoArray[nodeOccupancy];
  for (int i = 0; i < nodeOccupancy; i++) {
    if (lb < curr->keyArray[i]) {
      next = curr->pageNoArray[i];
      break;
    }
  }
  if (next == 0) {
    scanExecuting = false;
    throw NoSuchKeyFoundException();
  }
  bufMgr->unPinPage(file, currNo, false);

  //find record matching range
  currNo = next;
  bufMgr->readPage(file, currNo, temp);
  LeafNodeInt *currLeaf = reinterpret_cast<LeafNodeInt *>(temp);
  while (true) {
    for (int i = 0; i < leafOccupancy; i++) {
      if (currLeaf->keyArray[i] > ub) {
        scanExecuting = false;
        bufMgr->unPinPage(file, currNo, false);
        throw NoSuchKeyFoundException();
      }
      if (currLeaf->keyArray[i] >= lb) {
        currentPageNum = currNo;
        currentPageData = temp;
        nextEntry = i;
        return;
      }
    }
    next = currLeaf->rightSibPageNo;
    if(next == 0){
      break;
    }
    bufMgr->unPinPage(file, currNo, false);
    currNo = next;
    bufMgr->readPage(file, currNo, temp);
    currLeaf = reinterpret_cast<LeafNodeInt *>(temp);
  }
  //no valid keys in range
  scanExecuting = false;
  bufMgr->unPinPage(file, currNo, false);
  throw NoSuchKeyFoundException();
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId &outRid) {
  //check validity
  if (!scanExecuting) {
    throw ScanNotInitializedException();
  }
  if (nextEntry == -1) {
    throw IndexScanCompletedException();
  }
  LeafNodeInt *currLeaf = reinterpret_cast<LeafNodeInt *>(currentPageData);

  //set outputs
  outRid = currLeaf->ridArray[nextEntry];

  //move to next key
  nextEntry = (nextEntry + 1) % leafOccupancy;

  int ub = highValInt;
  if (highOp == LT) {
    ub--;
  }

  //chekc if no more keys in leaf
  if (currLeaf->keyArray[nextEntry] == INT_MAX) {
    nextEntry = 0;
  }

  //move to next leaf if possible
  if (nextEntry == 0) {
    if (currLeaf->rightSibPageNo == 0) {
      bufMgr->unPinPage(file, currentPageNum, false);
      nextEntry = -1;
      return;
    }
    PageId next = currLeaf->rightSibPageNo;
    bufMgr->unPinPage(file, currentPageNum, false);
    currentPageNum = next;
    bufMgr->readPage(file, currentPageNum, currentPageData);
    currLeaf = reinterpret_cast<LeafNodeInt *>(currentPageData);
  }

  if (currLeaf->keyArray[nextEntry] > ub) {
    bufMgr->unPinPage(file, currentPageNum, false);
    nextEntry = -1;
    return;
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() {
  if (!scanExecuting) throw ScanNotInitializedException();

  // Set all values to null
  this->scanExecuting = false;
  this->highValInt = -1;
  this->lowValInt = -1;
  this->nextEntry = -1;
  this->currentPageData = 0;
  this->currentPageNum = -1;
}

void BTreeIndex::simpleLeafInsert(int key, const RecordId rid,
                                  LeafNodeInt *currNode) {
  int i = 0;
  // find where to insert
  int size = currNode->getNumKeys();
  while (i < size) {
    if (currNode->keyArray[i] > key) {
      // Shift contents of keyarray
      for (int l = size; l > i; l--) {
        currNode->keyArray[l] = currNode->keyArray[l - 1];
      }
      // shift contents of ridArray
      for (int k = size; k > i; k--) {
        currNode->ridArray[k] = currNode->ridArray[k - 1];
      }

      // after contents shifted, exit
      break;
    }
    i += 1;
  }

  // insert key and rid at specified positions
  currNode->keyArray[i] = key;
  currNode->ridArray[i] = rid;
}

void BTreeIndex::simpleNodeInsert(int key, const PageId pageId,
                                  NonLeafNodeInt *currNode) {
  for (int i = currNode->getNumKeys(); i > 0; i--) {
    if (key > currNode->keyArray[i - 1]) {
      currNode->keyArray[i] = key;
      currNode->pageNoArray[i + 1] = pageId;
      return;
    }
    currNode->keyArray[i] = currNode->keyArray[i - 1];
    currNode->pageNoArray[i + 1] = currNode->pageNoArray[i];
  }
  currNode->keyArray[0] = key;
  currNode->pageNoArray[1] = pageId;
}

}  // namespace badgerdb
