
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <cstring>

#include "../rbf/rbfm.h"

#define CATALOG_CREATE_FAIL 13
#define CATALOG_DELETE_FAIL 14
#define TUPLE_INSERT_FAIL 15
#define TUPLE_DELETE_FAIL 16
#define TUPLE_UPDATE_FAIL 17
#define TUPLE_READ_FAIL 18

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RBFM_ScanIterator rbfm_ScanIterator;
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data) { return rbfm_ScanIterator.getNextRecord(rid, data); };
  RC close() { return rbfm_ScanIterator.close(); };
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);


protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  static RecordBasedFileManager *_rbfm; // need an instance of rbfm

  // privet helpers
  // add a record to the Tables paged file
  void addTable (const int tableid, const string& tablename, const string filename, FileHandle& fileHandle);
  // add a record to the Columns paged file
  void addColumn (const int tableid, const string& colname, const AttrType coltype, 
                  const int collen, const int colpos, FileHandle& fileHandle);
};

#endif
