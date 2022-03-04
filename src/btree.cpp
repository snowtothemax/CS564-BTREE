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

		this->bufMgr = bufMgrIn;
		this->headerPageNum = 1;
		this-> attrByteOffset = attrByteOffset;
		this->attributeType = attrType;
		this->scanExecuting = false;
		// Index File Name
		std::ostringstream idxStr;
		idxStr << relationName << '.' << attrByteOffset;
		outIndexName = idxStr.str();

		try
		{
			BlobFile indexFile = BlobFile::open(outIndexName);
			this->file = &indexFile;
			Page* temp;
			IndexMetaInfo* header;
			bufMgr->readPage(file, headerPageNum, temp);
			header = reinterpret_cast<IndexMetaInfo*>(temp);
			if(std::string(header->relationName) != relationName||
			   header->attrByteOffset != attrByteOffset||
			   header->attrType != attrType)
			{
				throw BadIndexInfoException("Invalid index was found!");
			}
			this -> rootPageNum = header->rootPageNo;
			bufMgr ->unPinPage(file,headerPageNum,false);

		}
		catch (FileNotFoundException* ex)
		{
			BlobFile indexFile = BlobFile::create(outIndexName);
			this->file = &indexFile;
			this -> rootPageNum = 2;
			Page* temp;
			IndexMetaInfo* header;
			bufMgr->allocPage(file, headerPageNum, temp);
			header = reinterpret_cast<IndexMetaInfo*>(temp);
			header->relationName = relationName.c_str();
			header->attrByteOffset = attrByteOffset;
			header->attrType = attrType;
			header->rootPageNo = this -> rootPageNum;
			bufMgr ->unPinPage(file,headerPageNum,true);
			FileScan scanner(relationName, bufMgr);

			std::string record;
			RecordId rid;
			while(true){
				try{
					scanner.scanNext(rid);
					record = scanner.getRecord();
					this->insertEntry(KEY TO BE PARSED,rid);
				}catch(EndOfFileException* ex){
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
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::scanNext
	// -----------------------------------------------------------------------------

	void BTreeIndex::scanNext(RecordId &outRid)
	{
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::endScan
	// -----------------------------------------------------------------------------
	//
	void BTreeIndex::endScan()
	{
	}

}
