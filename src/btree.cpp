/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/page_pinned_exception.h"

//#define DEBUG

namespace badgerdb
{

	// -----------------------------------------------------------------------------
	// BTreeIndex::BTreeIndex -- Constructor
	// -----------------------------------------------------------------------------

	BTreeIndex::BTreeIndex(const std::string &relationName,
						   std::string &outIndexName,
						   BufMgr *bufMgrIn,
						   const int attrByteOffset,
						   const Datatype attrType)
	{

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

		try
		{
			// if the index already exists
			BlobFile indexFile = BlobFile::open(outIndexName);
			this->file = &indexFile;
			Page *temp;
			IndexMetaInfo *header;
			bufMgr->readPage(file, headerPageNum, temp);

			// check vailidy of index
			header = reinterpret_cast<IndexMetaInfo *>(temp);
			if (std::string(header->relationName) != relationName ||
				header->attrByteOffset != attrByteOffset ||
				header->attrType != attrType)
			{
				throw BadIndexInfoException("Invalid index was found!");
			}

			// update root
			this->rootPageNum = header->rootPageNo;
			bufMgr->unPinPage(file, headerPageNum, false);
		}
		catch (FileNotFoundException *ex)
		{
			// no pre-existing index
			BlobFile indexFile = BlobFile::create(outIndexName);
			this->file = &indexFile;
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
			int leafNum;
			bufMgr->allocPage(file, leafNum, temp);

			leaf = reinterpret_cast<NonLeafNodeInt *>(temp);
			std::fill(leaf->keyArray, leaf->keyArray + nodeOccupancy, INT_MAX);
			std::fill(leaf->ridArray, leaf->ridArray + leafOccupancy, 0);
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
			while (true)
			{
				try
				{
					scanner.scanNext(rid);
					recordStr = scanner.getRecord();
					record = recordStr.c_str();
					key = record + attrByteOffset;
					this->insertEntry(key, rid);
				}
				catch (EndOfFileException *x)
				{
					break;
				}
			}
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::~BTreeIndex -- destructor
	// -----------------------------------------------------------------------------

	BTreeIndex::~BTreeIndex()
	{
		// endscan
		if (scanExecuting)
		{
			endScan();
		}

		// flush the file
		bufMgr->flushFile(this->file);

		delete file;
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::insertEntry
	// -----------------------------------------------------------------------------

	void BTreeIndex::insertEntry(const void *key, const RecordId rid)
	{
		recursiveInsert(*((int *)key), rid, false, this->rootPageNum);
	}

	// ------------------------------------------------------------------------------
	// Recursicve insert
	// Returns a KeyPagePair to push up when splitting
	// ------------------------------------------------------------------------------
	KeyPagePair *BTreeIndex::recursiveInsert(int key, const RecordId rid, const bool isLeaf, PageId currPageId)
	{
		// get the page
		Page *temp;
		this->bufMgr->readPage(this->file, currPageId, temp);

		if (!isLeaf) // look for leaf
		{
			NonLeafNodeInt *currNode;
			currNode = reinterpret_cast<NonLeafNodeInt *>(temp);

			// index to look for in the page array
			int index = 0;
			for (int currKey : currNode->keyArray)
			{
				if (key < currKey)
				{
					break;
				}
				index++;
			}

			// check whether the next node is a leaf or not
			bool isNextLeaf = false;
			if (currNode->level == 1)
			{
				isNextLeaf = true;
			}

			// recursive call. check if splitting has occurred
			KeyPagePair *pairToAdd = recursiveInsert(key, rid, isNextLeaf, currNode->pageNoArray[index]);
			if (pairToAdd != nullptr)
			{
				// check if there is space for the key page pair in the current array

				// there does not need to be an internal node split and the middle value of the old node is being pushed up
				if (currNode->numKeys < nodeOccupancy)
				{

					// shifting all values in the key array one to the right
					for (int i = INTARRAYNONLEAFSIZE - 1; i > 0; i--)
					{
						if (pairToAdd->key > currNode->keyArray[i])
						{
							currNode->keyArray[i] = pairToAdd->key;
							currNode->pageNoArray[i] = pairToAdd->pageId;
							break;
						}
						currNode->keyArray[i] = currNode->keyArray[i - 1];
						currNode->pageNoArray[i] = currNode->pageNoArray[i - 1];
					}
					return nullptr;
				}

				// there is an internal node split
				else
				{
					Page *newPage;
					PageId newInternalId;
					this->bufMgr->allocPage(this->file, newInternalId, newPage);
					NonLeafNodeInt *newInternalNode = reinterpret_cast<NonLeafNodeInt *>(newPage);

					std::fill(newInternalNode->keyArray, newInternalNode->keyArray + nodeOccupancy, INT_MAX);
					std::fill(newInternalNode->pageNoArray, newInternalNode->pageNoArray + nodeOccupancy, 0);
					newInternalNode->level = currNode->level;

					newInternalNode->numKeys = 0;

					pairToAdd newPair;
					newPair.pageNo = newInternalId;
					newPair.key = currNode->keyArray[(nodeOccupancy) % 2];
					currNode->keyArray[(nodeOccupancy) % 2] = INT_MAX;

					int j = 0;
					for (int i = nodeOccupancy % 2 + 1; i < nodeOccupancy; i++)
					{
						newInternalNode->pageNoArray[j] = currNode->pageNoArray[i];
						newInternalNode->keyArray[j] = currNode->keyArray[i];
						j++;

						currNode->pageArray[i] = 0;
						currNode->keyArray[i] = INT_MAX;
					}
					newInternalNode->keyArray[j] = currNode->keyArray[nodeOccupancy];
					currNode->pageNoArray[nodeOccupancy] = INT_MAX;

					retrun newPair;
				}
			}
		}
		else // insert into leaf
		{
			LeafNodeInt *currNode;
			currNode = reinterpret_cast<LeafNodeInt *>(temp);

			// check if there is enough space available on the leaf
			// I.E NO SPLITTING
			if (currNode->numKeys + 1 <= INTARRAYLEAFSIZE)
			{
				int i = 0;
				// find where to insert
				while (i < currNode->numKeys)
				{
					if (currNode->keyArray[i] > key)
					{
						// Shift contents of keyarray
						for (int l = currNode->numKeys; l > i; l--)
						{
							currNode->keyArray[l] = currNode->keyArray[l - 1];
						}
						// shift contents of ridArray
						for (int k = currNode->numKeys; k > i; k--)
						{
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
				currNode->numKeys += 1;

				// unpin page and return
				this->bufMgr->unPinPage(this->file, currPageId, true);
				return nullptr;
			}
			else // SPLITTING TIME BABY
			{
				// remember with leaf nodes, we COPY up instead of pushing up
				// create sibling
				Page *newSibPage;
				LeafNodeInt *newSibNode;
				PageId sibId;
				this->bufMgr->allocPage(this->file, sibId, newSibPage);
				newSibNode = reinterpret_cast<LeafNodeInt *>(newSibPage);
				newSibNode->numKeys = 0;

				// copy contents of old array into new array
				for (int i = INTARRAYLEAFSIZE / 2; i < INTARRAYLEAFSIZE; i++)
				{
					newSibNode->keyArray[i - (INTARRAYLEAFSIZE / 2)] = currNode->keyArray[i];
					newSibNode->ridArray[i - (INTARRAYLEAFSIZE / 2)] = currNode->ridArray[i];
					currNode->numKeys -= 1;
					newSibNode->numKeys += 1;
				}

				// have new sibling point to original nodes neighbor
				newSibNode->rightSibPageNo = currNode->rightSibPageNo;
				currNode->rightSibPageNo = sibId;

				// now we insert the value as we did before
				if (newSibNode->keyArray[0] > key) // insert into orig sib
				{
					int i = 0;
					// find where to insert
					while (i < currNode->numKeys)
					{
						if (currNode->keyArray[i] > key)
						{
							// Shift contents of keyarray
							for (int l = currNode->numKeys; l > i; l--)
							{
								currNode->keyArray[l] = currNode->keyArray[l - 1];
							}
							// shift contents of ridArray
							for (int k = currNode->numKeys; k > i; k--)
							{
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
					currNode->numKeys += 1;
				}
				else // insert into new sib
				{
					int i = 0;
					// find where to insert
					while (i < newSibNode->numKeys)
					{
						if (newSibNode->keyArray[i] > key)
						{
							// Shift contents of keyarray
							for (int l = newSibNode->numKeys; l > i; l--)
							{
								newSibNode->keyArray[l] = newSibNode->keyArray[l - 1];
							}
							// shift contents of ridArray
							for (int k = newSibNode->numKeys; k > i; k--)
							{
								newSibNode->ridArray[k] = newSibNode->ridArray[k - 1];
							}

							// after contents shifted, exit
							break;
						}
						i += 1;
					}

					// insert key and rid at specified positions
					newSibNode->keyArray[i] = key;
					newSibNode->ridArray[i] = rid;
					newSibNode->numKeys += 1;
				}

				// close both nodes and push up inserted values
				KeyPagePair *pair;
				pair->key = newSibNode->keyArray[0];
				pair->pageId = sibId;

				// unpinPages
				this->bufMgr->unPinPage(this->file, currPageId, true);
				this->bufMgr->unPinPage(this->file, sibId, true);

				return pair;
			}
		}

		// unpin page. HAVE TO MOVE TO BEFORE RETURNS ONCE IMPLEMENTED
		this->bufMgr->unPinPage(this->file, currPageId, false);
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::startScan
	// -----------------------------------------------------------------------------

	void BTreeIndex::startScan(const void *lowValParm,
							   const Operator lowOpParm,
							   const void *highValParm,
							   const Operator highOpParm)
	{
		if (!(lowOpParm == GT || lowOpParm == GTE) ||
			!(highOpParm == LT || highOpParm == LTE))
		{
			throw BadOpcodesException();
		}

		lowValInt = *(int *)lowValParm;
		highValInt = *(int *)highValParm;
		lowValDouble = 1.0 * lowValInt;
		highValDouble = 1.0 * highValInt;
		lowValString = std::to_string(lowValInt);
		highValString = std::to_string(highValInt);
		lowOp = lowOpParm;
		highOp = highOpParm;

		if (lowValInt > highValInt)
		{
			throw BadScanrangeException();
		}

		scanExecuting = true;

		int lb = lowValInt;
		int ub = highValInt;
		if (lowOp == GT)
		{
			lb++;
		}
		if (highOp == LT)
		{
			ub--;
		}

		Page *temp;
		NonLeafNodeInt *curr;
		bufMgr->readPage(file, rootPageNum, temp);
		curr = reinterpret_cast<NonLeafNodeInt *>(temp);
		int currNo = rootPageNum;
		int next;
		while (curr->level == 0)
		{
			next = curr->pageNoArray[nodeOccupancy];
			for (int i = 0; i < nodeOccupancy; i++)
			{
				if (lb < curr->keyArray[i])
				{
					next = curr->pageNoArray[i];
					break;
				}
			}
			if (next == 0)
			{
				scanExecuting = false;
				throw NoSuchKeyFoundException();
			}
			bufMgr->unPinPage(file, currNo, false);

			currNo = next;
			bufMgr->readPage(file, currNo, temp);
			curr = reinterpret_cast<NonLeafNodeInt *>(temp);
		}

		next = curr->pageNoArray[nodeOccupancy];
		for (int i = 0; i < nodeOccupancy; i++)
		{
			if (lb < curr->keyArray[i])
			{
				next = curr->pageNoArray[i];
				break;
			}
		}
		if (next == 0)
		{
			scanExecuting = false;
			throw NoSuchKeyFoundException();
		}
		bufMgr->unPinPage(file, currNo, false);

		currNo = next;
		bufMgr->readPage(file, currNo, temp);
		LeafNodeInt *currLeaf = reinterpret_cast<LeafNodeInt *>(temp);
		while (currLeaf->rightSibPageNo != 0)
		{
			for (int i = 0; i < leafOccupancy; i++)
			{
				if (currLeaf->keyArray[i] > ub)
				{
					scanExecuting = false;
					throw NoSuchKeyFoundException();
				}
				if (currLeaf->keyArray[i] >= lb)
				{
					currentPageNum = currNo;
					currentPageData = temp;
					nextEntry = i;
					return;
				}
			}
			next = currLeaf->rightSibPageNo;
			bufMgr->unPinPage(file, currNo, false);
			currNo = next;
			bufMgr->readPage(file, currNo, temp);
			currLeaf = reinterpret_cast<LeafNodeInt *>(temp);
		}
		for (int i = 0; i < leafOccupancy; i++)
		{
			if (currLeaf->keyArray[i] > ub)
			{
				scanExecuting = false;
				throw NoSuchKeyFoundException();
			}
			if (currLeaf->keyArray[i] >= lb)
			{
				currentPageNum = currNo;
				currentPageData = temp;
				nextEntry = i;
				return;
			}
		}
		scanExecuting = false;
		throw NoSuchKeyFoundException();
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::scanNext
	// -----------------------------------------------------------------------------

	void BTreeIndex::scanNext(RecordId &outRid)
	{
		if (!scanExecuting)
		{
			throw ScanNotInitializedException();
		}
		if (nextEntry == -1)
		{
			throw IndexScanCompletedException();
		}
		LeafNodeInt *currLeaf = reinterpret_cast<LeafNodeInt *>(currentPageData);
		outRid = currLeaf->ridArray[nextEntry];
		int ub = highValInt;
		if (highOp == LT)
		{
			ub--;
		}
		nextEntry = (nextEntry + 1) % leafOccupancy;
		if (nextEntry == 0)
		{
			if (currLeaf->rightSibPageNo == 0)
			{
				bufMgr->unPinPage(file, currentPageNum, false);
				nextEntry = -1;
				return;
			}
			int next = currLeaf->rightSibPageNo;
			bufMgr->unPinPage(file, currentPageNum, false);
			currentPageNum = next;
			bufMgr->readPage(file, currentPageNum, currentPageData);
			currLeaf = reinterpret_cast<LeafNodeInt *>(currentPageData);

			if (currLeaf->keyArray[nextEntry] > ub)
			{
				bufMgr->unPinPage(file, currentPageNum, false);
				nextEntry = -1;
				return;
			}
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::endScan
	// -----------------------------------------------------------------------------
	//
	void BTreeIndex::endScan()
	{
		if (!scanExecuting)
			throw ScanNotInitializedException("No scan in progress!");

		// Set all values to null
		this->scanExecuting = false;
		this->highValInt = -1;
		this->lowValInt = -1;
		this->nextEntry = -1;
		this->currentPageData = 0;
		this->currentPageNum = -1;
	}
}
