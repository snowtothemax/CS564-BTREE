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
		
		//initalize vars
		this->bufMgr = bufMgrIn;
		this->headerPageNum = 1;
		this-> attrByteOffset = attrByteOffset;
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
			//if the index already exists
			BlobFile indexFile = BlobFile::open(outIndexName);
			this->file = &indexFile;
			Page* temp;
			IndexMetaInfo* header;
			bufMgr->readPage(file, headerPageNum, temp);

			//check vailidy of index
			header = reinterpret_cast<IndexMetaInfo*>(temp);
			if(std::string(header->relationName) != relationName||
			   header->attrByteOffset != attrByteOffset||
			   header->attrType != attrType)
			{
				throw BadIndexInfoException("Invalid index was found!");
			}

			//update root
			this -> rootPageNum = header->rootPageNo;
			bufMgr ->unPinPage(file,headerPageNum,false);

		}
		catch (FileNotFoundException* ex)
		{
			//no pre-existing index
			BlobFile indexFile = BlobFile::create(outIndexName);
			this->file = &indexFile;
			this -> rootPageNum = 2;

			//create header page
			Page* temp;
			IndexMetaInfo* header;
			bufMgr->allocPage(file, headerPageNum, temp);
			header = reinterpret_cast<IndexMetaInfo*>(temp);
			
			//Initialize empty root
			NonLeafNodeInt* root;
			bufMgr->allocPage(file, rootPageNum, temp);
			root = reinterpret_cast<NonLeafNodeInt*>(temp);
			std::fill(root->keyArray,root->keyArray+nodeOccupancy,INT_MAX);
			std::fill(root->pageNoArray,root->pageNoArray+nodeOccupancy,-1);
			root -> level = 1;
			bufMgr->unPinPage(file, rootPageNum, true);

			//fill header info
			relationName.copy(header->relationName,20);
			header->attrByteOffset = attrByteOffset;
			header->attrType = attrType;
			header->rootPageNo = this -> rootPageNum;
			bufMgr ->unPinPage(file,headerPageNum,true);
			


			//populate index
			FileScan scanner(relationName, bufMgr);
			std::string recordStr;
			RecordId rid;
			const char *record;
			const void* key;
			while(true){
				try{
					scanner.scanNext(rid);
					recordStr = scanner.getRecord();
					record = recordStr.c_str();
                                	key = record+attrByteOffset;
					this->insertEntry(key,rid);
				}catch(EndOfFileException* x){
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
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::insertEntry
	// -----------------------------------------------------------------------------

	void BTreeIndex::insertEntry(const void *key, const RecordId rid)
	{

	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::startScan
	// -----------------------------------------------------------------------------

	void BTreeIndex::startScan(const void *lowValParm,
							   const Operator lowOpParm,
							   const void *highValParm,
							   const Operator highOpParm)
	{
		if( !(lowOpParm == GT ||lowOpParm == GTE) ||
		    !(highOpParm == LT||highOpParm == LTE)    ){
				throw BadOpcodesException();
		}

		
		lowValInt = *(int*)lowValParm;
		highValInt = *(int*)highValParm;
		lowValDouble = 1.0 * lowValInt;
		highValDouble = 1.0 * highValInt;
		lowValString = std::to_string(lowValInt);
		highValString = std::to_string(highValInt);
		lowOp = lowOpParm;
		highOp = highOpParm;

		if(lowValInt>highValInt){
			throw  BadScanrangeException();
		}

		scanExecuting = true;

		int lb = lowValInt;
		int ub = highValInt;
		if (lowOp == GT){
			lb ++;
		}
		if(highOp == LT){
			ub--;
		}

		Page* temp;
                NonLeafNodeInt* curr;
                bufMgr->readPage(file, rootPageNum, temp);
                curr = reinterpret_cast<NonLeafNodeInt*>(temp);
		int currNo = rootPageNum;
		int next;
		while(curr->level == 0){
			next = curr->pageNoArray[nodeOccupancy];
			for(int i = 0; i<nodeOccupancy; i++){
				if (lb < curr->keyArray[i]){
					next = curr->pageNoArray[i];
					break;
				}
			}
			if(next == -1){
				scanExecuting = false;
				throw NoSuchKeyFoundException();
			}
			bufMgr ->unPinPage(file,currNo,false);

			currNo = next;
			bufMgr->readPage(file, currNo, temp);
                	curr = reinterpret_cast<NonLeafNodeInt*>(temp);
		}


		next = curr->pageNoArray[nodeOccupancy];
                for(int i = 0; i<nodeOccupancy; i++){
                        if (lb < curr->keyArray[i]){
                                next = curr->pageNoArray[i];
                                break;
                        }
                }
                if(next == -1){
			scanExecuting = false;
                        throw NoSuchKeyFoundException();
                }
                bufMgr ->unPinPage(file,currNo,false);

                currNo = next;
                bufMgr->readPage(file, currNo, temp);
                LeafNodeInt* currLeaf = reinterpret_cast<LeafNodeInt*>(temp);
		while(currLeaf->rightSibPageNo  != 0){
			for(int i = 0; i<leafOccupancy; i++){
				if(currLeaf->keyArray[i]>ub){
					scanExecuting = false;
					throw NoSuchKeyFoundException();
				}
				if(currLeaf->keyArray[i]>=lb){
					currentPageNum = currNo;
					currentPageData = temp;
					nextEntry = i;
					return;
                                }

			}
			next = currLeaf->rightSibPageNo;
			bufMgr ->unPinPage(file,currNo,false);
			currNo = next;
                	bufMgr->readPage(file, currNo, temp);
                	currLeaf = reinterpret_cast<LeafNodeInt*>(temp);
		}
		for(int i = 0; i<leafOccupancy; i++){
                	if(currLeaf->keyArray[i]>ub){
				scanExecuting = false;
                                throw NoSuchKeyFoundException();
                        }
                        if(currLeaf->keyArray[i]>=lb){
                                currentPageNum = currNo;
                                currentPageData= temp;
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
		if (!scanExecuting){
			throw ScanNotInitializedException();
		}
		if (nextEntry == -1){
			throw IndexScanCompletedException();
		}
		LeafNodeInt* currLeaf = reinterpret_cast<LeafNodeInt*>(currentPageData);
		outRid = currLeaf->ridArray[nextEntry];
                int ub = highValInt;
                if(highOp == LT){
                        ub--;
                }
		nextEntry = (nextEntry+1)%leafOccupancy;
		if(nextEntry ==0){
			if(currLeaf->rightSibPageNo  == 0){
				bufMgr ->unPinPage(file,currentPageNum ,false);
				nextEntry = -1;
				return;
			}
			int next = currLeaf->rightSibPageNo;
                        bufMgr ->unPinPage(file,currentPageNum ,false);
                        currentPageNum = next;
                        bufMgr->readPage(file, currentPageNum, currentPageData);
			currLeaf = reinterpret_cast<LeafNodeInt*>(currentPageData);

			if(currLeaf->keyArray[nextEntry]>ub){
				bufMgr ->unPinPage(file,currentPageNum ,false);
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
	}

}
