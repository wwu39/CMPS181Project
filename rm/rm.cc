
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
    FileHandle fileHandle;
    if (_rbfm->createFile("Tables")) return CATALOG_CREATE_FAIL;
    if (_rbfm->openFile("Tables", fileHandle)) return RBFM_OPEN_FAILED;
    addTable(1, "Tables", "Tables", fileHandle);
    addTable(2, "Columns", "Columns", fileHandle);
    if (_rbfm->closeFile(fileHandle)) return RBFM_CLOSE_FAILED;

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
void RelationManager::addTable (const int tableid, const string& tablename, const string filename, FileHandle& fileHandle) 
{
    // construct the recordDescriptor
    vector<Attribute> recordDescriptor;
    recordDescriptor.push_back( {"table-id", TypeInt, INT_SIZE } );
    recordDescriptor.push_back( {"table-name", TypeVarChar, 50 } );
    recordDescriptor.push_back( {"file-name", TypeVarChar, 50 } );
    RID rid; // we need an rid
    char * data = (char *)malloc(PAGE_SIZE);
    int offset = 0; // keep track of where we are

    // since none of the fields are null, we put 0000 0000 for 3 fields
    int NI = 0;
    memcpy(data + offset, &NI, 1); offset += 1;
    // table-id
    memcpy(data + offset, &tableid, 4); offset += 4;
    // table-name
    int tablenameSize = tablename.size();
    memcpy(data + offset, &tablenameSize, 4); offset += 4;
    memcpy(data + offset, tablename.c_str(), tablenameSize); offset += tablenameSize;
    // file-name
    int filenameSize = filename.size();
    memcpy(data + offset, &filenameSize, 4); offset += 4;
    memcpy(data + offset, filename.c_str(), filenameSize);
    // insert
    if (_rbfm->insertRecord(fileHandle, recordDescriptor, data, rid));
}

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
    int NI = 0;
    memcpy(data + offset, &NI, 1); offset += 1;
    // table-id
    memcpy(data + offset, &tableid, 4); offset += 4;
    // column-name
    int colnameSize = colname.size();
    memcpy(data + offset, &colnameSize, 4); offset += 4;
    memcpy(data + offset, colname.c_str(), colnameSize); offset += colnameSize;
    // column-type
    int coltypeInt = (int) coltype;
    memcpy(data + offset, &coltypeInt, 4); offset += 4;
    // column-length
    memcpy(data + offset, &collen, 4); offset += 4;
    // column-position
    memcpy(data + offset, &colpos, 4); //offset += 4;
    // insert
    RID rid;
    if (_rbfm->insertRecord(fileHandle, recordDescriptor, data, rid));

}

RC RelationManager::deleteCatalog()
{
    if (_rbfm->destroyFile("Tables")) return CATALOG_DELETE_FAIL;
    if (_rbfm->destroyFile("Columns")) return CATALOG_DELETE_FAIL;
    tableidnumber = 0;
    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    ++tableidnumber;
    if (_rbfm->createFile(tableName)) return CATALOG_CREATE_FAIL;

    FileHandle fileHandle;
    // add table
    if (_rbfm->openFile("Tables", fileHandle)) return RBFM_OPEN_FAILED;
    addTable(tableidnumber, tableName, tableName, fileHandle);
    if (_rbfm->closeFile(fileHandle)) return RBFM_CLOSE_FAILED;

    // add all columns
    if(_rbfm->openFile("Columns", fileHandle)) return RBFM_OPEN_FAILED;
    for(unsigned i = 0; i < attrs.size(); i++)
        addColumn(tableidnumber, attrs[i].name, attrs[i].type, attrs[i].length, i + 1, fileHandle);
    if (_rbfm->closeFile(fileHandle)) return RBFM_CLOSE_FAILED;

    return SUCCESS;

}

RC RelationManager::deleteTable(const string &tableName)
{
    return (_rbfm->destroyFile(tableName));
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    FileHandle fileHandle;
    if (_rbfm->openFile("Tables", fileHandle)) return RBFM_OPEN_FAILED;
    vector<string> attrs1;
    attrs1.push_back("table-id");
    RM_ScanIterator rm_ScanIterator;
    scan("Tables", "table-name" ,EQ_OP, tableName.c_str(), attrs1, rm_ScanIterator);
    void * data = malloc(PAGE_SIZE);
    RID rid;
    rm_ScanIterator.getNextTuple(rid, data);
    int * tableid = (int*) malloc (sizeof (int));
    memcpy(tableid, (char *)data + 1, 4);
    rm_ScanIterator.close();
    if (_rbfm->closeFile(fileHandle)) return RBFM_CLOSE_FAILED;

    if (_rbfm->openFile("Columns", fileHandle)) return RBFM_OPEN_FAILED;
    vector<string> attrs2;
    attrs2.push_back("column-name");
    attrs2.push_back("column-type");
    attrs2.push_back("column-length");
    scan("Columns", "table-id", EQ_OP, tableid, attrs2, rm_ScanIterator);
    while ( rm_ScanIterator.getNextTuple(rid, data) != RM_EOF ) {
        int offset = 1;
        int * vclen = (int*) malloc (sizeof(int));
        memcpy(vclen, (char*)data + offset, 4);
        offset += 4;
        char * name = (char*) malloc(*vclen);
        memcpy(name, (char *)data + offset, *vclen);
        offset += *vclen;
        int * type = (int*) malloc (sizeof(int));
        memcpy(type, (char*)data + offset, 4);
        offset += 4;
        int * length = (int*) malloc (sizeof(int));
        memcpy(length, (char*)data + offset, 4);
        attrs.push_back({name, static_cast<AttrType>(*type), *length });
        free(vclen); free(name); free(type); free(length);
    }
    free(tableid);
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



