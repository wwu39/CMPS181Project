
#include "rm.h"
#include <iostream>

RelationManager* RelationManager::_rm = 0;
RecordBasedFileManager* RelationManager::_rbfm = 0;

static int tableidnumber = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
    // Initialize the internal RecordBasedFileManager instance
    _rbfm = RecordBasedFileManager::instance();
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    // initialize Tables
    FileHandle fileHandle; // we need a filehandle
    if (_rbfm->createFile("Tables")) return CATALOG_CREATE_FAIL; // Create the file called Tables
    if (_rbfm->openFile("Tables", fileHandle)) return RBFM_OPEN_FAILED; // open the file using filehandle
    addTable(1, "Tables", "Tables", fileHandle);
    addTable(2, "Columns", "Columns", fileHandle);
    if (_rbfm->closeFile(fileHandle)) return RBFM_CLOSE_FAILED;

    // initialize Columns
    if ( _rbfm->createFile("Columns")) return CATALOG_CREATE_FAIL;
    if (_rbfm->openFile("Columns", fileHandle)) return RBFM_OPEN_FAILED;

    addColumn(1, "table-id", TypeInt, 4, 1, fileHandle);
    addColumn(1, "table-name", TypeVarChar, 50, 2, fileHandle);
    addColumn(1, "file-name", TypeVarChar, 50, 3, fileHandle);

    addColumn(2, "table-id", TypeInt, 4, 1, fileHandle);
    addColumn(2, "column-name", TypeVarChar, 50, 2, fileHandle);
    addColumn(2, "column-type", TypeVarChar, 50, 3, fileHandle);
    addColumn(2, "column-length", TypeInt, 4, 4, fileHandle);
    addColumn(2, "column-position", TypeInt, 4, 5, fileHandle);

    tableidnumber += 2;
    if (_rbfm->closeFile(fileHandle)) return RBFM_CLOSE_FAILED;
    return SUCCESS;
}

// private helper
// add a record to the Tables paged file
void RelationManager::addTable (const int tableid, const string& tablename, const string filename, FileHandle& fileHandle) 
{
    // construct the recordDescriptor for Tables
    vector<Attribute> recordDescriptor;
    recordDescriptor.push_back( {"table-id", TypeInt, INT_SIZE } );
    recordDescriptor.push_back( {"table-name", TypeVarChar, 50 } );
    recordDescriptor.push_back( {"file-name", TypeVarChar, 50 } );
    RID rid; // we need an rid
    char * data = (char *)malloc(PAGE_SIZE); // |NI|F1|F2|...
    int offset = 0; // keep track of where we are

    // since none of the fields are null, we put 0000 0000 for 3 fields
    uint8_t NI = 0;
    memcpy(data + offset, &NI, 1); offset += 1; // 1 byte null indicator
    // table-id
    memcpy(data + offset, &tableid, INT_SIZE); offset += INT_SIZE;
    // table-name
    int tablenameSize = tablename.size();
    memcpy(data + offset, &tablenameSize, 4); offset += 4; // VacChar length
    memcpy(data + offset, tablename.c_str(), tablenameSize); offset += tablenameSize;
    // file-name
    int filenameSize = filename.size();
    memcpy(data + offset, &filenameSize, 4); offset += 4; // VacChar length
    memcpy(data + offset, filename.c_str(), filenameSize);
    // insert
    _rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
    free(data);
}

// add a record to the Columns paged file
void RelationManager::addColumn (const int tableid, const string& colname, const AttrType coltype, 
                const int collen, const int colpos, FileHandle& fileHandle)
{
    vector<Attribute> recordDescriptor;
    recordDescriptor.push_back( {"table-id", TypeInt, INT_SIZE } );
    recordDescriptor.push_back( {"column-name", TypeVarChar, 50 } );
    recordDescriptor.push_back( {"column-type", TypeInt, 4 } );
    recordDescriptor.push_back( {"column-length", TypeInt, 4 } );
    recordDescriptor.push_back( {"column-position", TypeInt, 4 } );
    // since none of the fields are null, we put 0000 0000 for 3 fields
    char * data = (char *)malloc(PAGE_SIZE);
    int offset = 0;
    uint8_t NI = 0;
    memcpy(data + offset, &NI, 1); offset += 1;
    // table-id
    memcpy(data + offset, &tableid, 4); offset += 4;
    // column-name
    int colnameSize = colname.size();
    memcpy(data + offset, &colnameSize, 4); offset += 4;
    memcpy(data + offset, colname.c_str(), colnameSize); offset += colnameSize;
    // column-type
    memcpy(data + offset, &coltype, INT_SIZE); offset += INT_SIZE;
    // column-length
    memcpy(data + offset, &collen, 4); offset += 4;
    // column-position
    memcpy(data + offset, &colpos, 4); //offset += 4;
    // insert
    RID rid;
    _rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
    free(data);
}

RC RelationManager::deleteCatalog()
{
    // delete the 2 files
    if (_rbfm->destroyFile("Tables")) return CATALOG_DELETE_FAIL;
    if (_rbfm->destroyFile("Columns")) return CATALOG_DELETE_FAIL;
    tableidnumber = 0;
    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    ++tableidnumber; // table-id
    if (_rbfm->createFile(tableName)) return CATALOG_CREATE_FAIL; // create the table-file

    FileHandle fileHandle;

    // register the new table in Tables
    if (_rbfm->openFile("Tables", fileHandle)) return RBFM_OPEN_FAILED;
    addTable(tableidnumber, tableName, tableName, fileHandle);
    if (_rbfm->closeFile(fileHandle)) return RBFM_CLOSE_FAILED;

    // register all columns in Columns
    if(_rbfm->openFile("Columns", fileHandle)) return RBFM_OPEN_FAILED;
    for(unsigned i = 0; i < attrs.size(); ++i)
        addColumn(tableidnumber, attrs[i].name, attrs[i].type, attrs[i].length, i + 1, fileHandle);
    if (_rbfm->closeFile(fileHandle)) return RBFM_CLOSE_FAILED;

    return SUCCESS;

}

RC RelationManager::deleteTable(const string &tableName)
{
    // delete the table-file only
    return (_rbfm->destroyFile(tableName));
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    FileHandle fileHandle;
    RBFM_ScanIterator rbfmsi;
    void * data = malloc(PAGE_SIZE);
    RID rid;

    // find the table-id of the given table
    if (_rbfm->openFile("Tables", fileHandle)) return RBFM_OPEN_FAILED;

    // prepare the rd for Tables
    vector<Attribute> tablesRecordDescriptor;
    tablesRecordDescriptor.push_back( {"table-id", TypeInt, INT_SIZE } );
    tablesRecordDescriptor.push_back( {"table-name", TypeVarChar, 50 } );
    tablesRecordDescriptor.push_back( {"file-name", TypeVarChar, 50 } );

    unsigned numOfPage = fileHandle.getNumberOfPages();
    rid = {0, 0};
    bool findTableid = false;
    int tableid;
    // read all pages in Tables
    for (rid.pageNum = 0; rid.pageNum < numOfPage; ++rid.pageNum) {
        // while has record to read
        while(!_rbfm->readRecord(fileHandle, tablesRecordDescriptor, rid, data)) {
            int tablenamesize;
            memcpy(&tablenamesize, (char*)data + 5, INT_SIZE);
            char tablename[tablenamesize + 1];
            memcpy(tablename, (char*)data + 9, tablenamesize);
            tablename[tablenamesize] = '\0';
            if (tableName == string(tablename)) {
                findTableid = true;
                memcpy(&tableid, (char*)data + 1, INT_SIZE);
                break;
            }
            if (findTableid) break;
            ++rid.slotNum;
        }
    }
    if (_rbfm->closeFile(fileHandle)) return RBFM_CLOSE_FAILED;


    // use the tableid to find all columns
    if (_rbfm->openFile("Columns", fileHandle)) return RBFM_OPEN_FAILED;

    // prepare the rd for Columns
    vector<Attribute> columnsRecordDescriptor;
    columnsRecordDescriptor.push_back( {"table-id", TypeInt, INT_SIZE } );
    columnsRecordDescriptor.push_back( {"column-name", TypeVarChar, 50 } );
    columnsRecordDescriptor.push_back( {"column-type", TypeInt, 4 } );
    columnsRecordDescriptor.push_back( {"column-length", TypeInt, 4 } );
    columnsRecordDescriptor.push_back( {"column-position", TypeInt, 4 } );

    rid = {0, 0};
    // read all pages in Columns
    for (rid.pageNum = 0; rid.pageNum < numOfPage; ++rid.pageNum) {
        // while has record to read
        while(!_rbfm->readRecord(fileHandle, columnsRecordDescriptor, rid, data)) {
            int curTableid; // get the tableid
            memcpy(&curTableid, (char*)data + 1, INT_SIZE);
            if (curTableid == tableid) { // if tableid match
                int offset = 5; // skip NI and Field1
                int namesize;
                memcpy(&namesize, (char*)data + offset, 4); offset += 4;
                char name[namesize + 1];
                memcpy(name, (char*)data + offset, namesize); offset += namesize;
                name[namesize] = '\0';

                AttrType type;
                memcpy(&type, (char*)data + offset, INT_SIZE); offset += INT_SIZE;

                int length;
                memcpy(&length, (char*)data + offset, sizeof(int));

                attrs.push_back({string(name), type, (unsigned)length});
            }
            ++rid.slotNum;
        }
    }
    if (_rbfm->closeFile(fileHandle)) return RBFM_CLOSE_FAILED;
    free(data);
    return SUCCESS;
}



RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    FileHandle fileHandle;
    if (_rbfm->openFile(tableName, fileHandle)) return RBFM_OPEN_FAILED;
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    if (_rbfm->insertRecord(fileHandle, recordDescriptor, data, rid)) return TUPLE_INSERT_FAIL;
    if (_rbfm->closeFile(fileHandle)) return RBFM_CLOSE_FAILED;
    return SUCCESS;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    FileHandle fileHandle;
    if (_rbfm->openFile(tableName, fileHandle)) return RBFM_OPEN_FAILED;
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    if (_rbfm->deleteRecord(fileHandle, recordDescriptor, rid)) return TUPLE_DELETE_FAIL;
    if (_rbfm->closeFile(fileHandle)) return RBFM_CLOSE_FAILED;
    return SUCCESS;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    FileHandle fileHandle;
    if (_rbfm->openFile(tableName, fileHandle)) return RBFM_OPEN_FAILED;
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    if (_rbfm->updateRecord(fileHandle, recordDescriptor, data, rid)) return TUPLE_UPDATE_FAIL;
    if (_rbfm->closeFile(fileHandle)) return RBFM_CLOSE_FAILED;
    return SUCCESS;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    FileHandle fileHandle;
    if (_rbfm->openFile(tableName, fileHandle)) return RBFM_OPEN_FAILED;
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    if (_rbfm->readRecord(fileHandle, recordDescriptor, rid, data)) return TUPLE_READ_FAIL;
    if (_rbfm->closeFile(fileHandle)) return RBFM_CLOSE_FAILED;
    return SUCCESS;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return _rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    FileHandle fileHandle;
    if (_rbfm->openFile(tableName, fileHandle)) return RBFM_OPEN_FAILED;
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    if (_rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data)) return TUPLE_READ_FAIL;
    if (_rbfm->closeFile(fileHandle)) return RBFM_CLOSE_FAILED;
    return SUCCESS;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    FileHandle fileHandle;
    if (_rbfm->openFile(tableName, fileHandle)) return RBFM_OPEN_FAILED;
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    if (_rbfm->scan(fileHandle, 
                    recordDescriptor, 
                    conditionAttribute,
                    compOp,
                    value,
                    attributeNames,
                    rm_ScanIterator.rbfm_ScanIterator
                    )) 
        return TUPLE_READ_FAIL;
    if (_rbfm->closeFile(fileHandle)) return RBFM_CLOSE_FAILED;
    return SUCCESS;
}



