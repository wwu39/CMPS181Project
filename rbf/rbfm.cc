#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>

#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    // Initialize the internal PagedFileManager instance
    _pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) 
{
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);

    // Adds the first record based page.
    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) 
{
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) 
{
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) 
{
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) 
{
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    // Setting the return RID.
    rid.pageNum = i;
    rid.slotNum = slotHeader.recordEntriesNumber;

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    slotHeader.recordEntriesNumber += 1;
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) 
{
    // Retrieve the specific page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    // handle forwrding recursively
    if (recordEntry.offset >= 0) {
        // Retrieve the actual entry data
        if (recordEntry.length > 0) {
            getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);
            free(pageData);
        } else {
            free(pageData);
            return RBFM_RECORD_DELETED;
        }
    } else {
        free(pageData);
        RID newrid = {recordEntry.length, (uint32_t)(- recordEntry.offset)};
        readRecord(fileHandle, recordDescriptor, newrid, data);
    }
    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Parse the null indicator into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                cout << "" << data_integer << endl;
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                cout << "" << data_real << endl;
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                offset += varcharSize;

                cout << data_string << endl;
                free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}

// asg2 spec
RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) 
{
    // Retrieve the specific page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData)) {
        free(pageData);
        return RBFM_READ_FAILED;
    }

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if (slotHeader.recordEntriesNumber < rid.slotNum) {
        free(pageData);
        return RBFM_SLOT_DN_EXIST;
    }

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    // handle forwrding recursively --Weikai Wu
    if (recordEntry.offset >= 0) {
        if (recordEntry.length > 0) { // it's not deleted before
            deleteRecordAtOffset(pageData, recordEntry, rid);
            recordEntry.length = 0; // means this record deleted
            setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
            //Flush pageData to the physical page
            if (fileHandle.writePage(rid.pageNum, pageData)) {
                free(pageData);
                return RBFM_WRITE_FAILED;
            }
            free(pageData);
        } else {
            free(pageData);
            return RBFM_RECORD_DELETED;
        }
    } else {
        free(pageData);
        RID newrid = {recordEntry.length, (uint32_t)(- recordEntry.offset)};
        deleteRecord(fileHandle, recordDescriptor, newrid);
    }
    return SUCCESS;
}


RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
    // Retrieve the specific page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData)) {
        free(pageData);
        return RBFM_READ_FAILED;
    }

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if (slotHeader.recordEntriesNumber < rid.slotNum) {
        free(pageData);
        return RBFM_SLOT_DN_EXIST;
    }

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    // handle forwrding recursively
    if (recordEntry.offset >= 0) {
        if (recordEntry.length > 0) { // it's not deleted before
            void * record = malloc(PAGE_SIZE); // get the old record
            getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, record);
            // get sizes
            // unsigned oldRecordSize = getRecordSize(recordDescriptor, record);
            unsigned newRecordSize = getRecordSize(recordDescriptor, data);
            // delete old record
            deleteRecordAtOffset(pageData, recordEntry, rid);
            if (getPageFreeSpaceSize(pageData) - newRecordSize >= 0) { 
                // if there is space in the page for the update
                // FSptr changed after deletion, but N isn't
                SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
                
                // Updating record reference in the slot directory.
                SlotDirectoryRecordEntry newRecordEntry {newRecordSize, (int32_t)(slotHeader.freeSpaceOffset - newRecordSize)};
                setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);
                
                // Updating the slot directory header.
                slotHeader.freeSpaceOffset -= newRecordSize;
                // slotHeader.recordEntriesNumber += 0; // unchanged
                setSlotDirectoryHeader(pageData, slotHeader);

                // Adding the record data.
                setRecordAtOffset(pageData, slotHeader.freeSpaceOffset, recordDescriptor, data);
            } else {
                RID newrid;
                if (insertRecord(fileHandle, recordDescriptor, data, newrid)) { // Insert the new record to another page
                    free(pageData); free(record);
                    return RBFM_UPDATE_FAILED_ON_INSERT;
                }
                // prepare a forwarding address
                SlotDirectoryRecordEntry recordEntry;
                recordEntry.length = newrid.pageNum;
                recordEntry.offset = - newrid.slotNum;
                setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry); // Flush the changes to pageData
            }
            if (fileHandle.writePage(rid.pageNum, pageData)) { // Flush the changes to the physical page
                free(pageData); free(record);
                return RBFM_WRITE_FAILED;
            }
            free(pageData); free(record);
        } else {
            free(pageData);
            return RBFM_RECORD_DELETED;
        }
    } else {
        free(pageData);
        RID newrid = {recordEntry.length, (uint32_t)(- recordEntry.offset)};
        updateRecord(fileHandle, recordDescriptor, data, newrid);
    }
    return SUCCESS;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{
    // checking the existence of the attribute
    bool attrFound = false; 
    size_t i = 0; // get the ith field
    size_t fieldCount = recordDescriptor.size();
    for (; i < fieldCount; ++i) {
        if (recordDescriptor[i].name == attributeName) {
            attrFound = true;
            break;
        }
    }
    if (!attrFound) return RBFM_ATTR_NOTFOUND;

    void * pageData = malloc(PAGE_SIZE);
    // read page by pageNum
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;
    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;
    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    
    // handle forwrding recursively
    if (recordEntry.offset >= 0) {
        if (recordEntry.length <= 0) {  // it's not deleted before
            free(pageData);
            return RBFM_RECORD_DELETED;
        }
        // record format: |Num of Record|NI|CO1|CO2|...|F1|F2|...
        // reading the ith field
        // get to the start of the record
        char * start = (char *) pageData + recordEntry.offset;
        int rec_offset = 0; // offset relates to the start of the record
        rec_offset += 2; // jump across Num of Record
        // get the nullIndicator
        int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
        char nullIndicator[nullIndicatorSize];
        memset(nullIndicator, 0, nullIndicatorSize);
        memcpy(nullIndicator, start + rec_offset, nullIndicatorSize);
        rec_offset += nullIndicatorSize;
        if (fieldIsNull(nullIndicator, i)) { // if the field is null, memcpy 1000 0000 and done
            unsigned a = (1 << (CHAR_BIT - 1));
            memcpy(data, &a, 1);
        } else {
            unsigned a = (0 << (CHAR_BIT - 1));
            memcpy(data, &a, 1); // if the field isn't null, memcpy 0000 0000
            ColumnOffset fieldStart, fieldEnd;
            
            // the ith field starts at the END of the (i-1)th field
            rec_offset += (sizeof(ColumnOffset) * (i - 1));
            memcpy(&fieldStart, start + rec_offset, sizeof(ColumnOffset));

            // where the ith field ends
            rec_offset += sizeof(ColumnOffset); 
            memcpy(&fieldEnd, start + rec_offset, sizeof(ColumnOffset));

            // so we can get the size of the field
            int fieldSize = fieldEnd - fieldStart;
            memcpy((char *)data + 1, start + fieldStart, fieldSize); // get the field
        }
        free(pageData);
    } else {
        free(pageData);
        RID newrid = {recordEntry.length, (uint32_t)(- recordEntry.offset)};
        readAttribute(fileHandle, recordDescriptor, newrid, attributeName, data);
    }
    return SUCCESS;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator) 
{
    rbfm_ScanIterator.curRid = {0, 0};
    rbfm_ScanIterator.fileHandle = fileHandle;
    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
    rbfm_ScanIterator.conditionAttribute = conditionAttribute;
    rbfm_ScanIterator.compOp = compOp;
    rbfm_ScanIterator.value = value;
    rbfm_ScanIterator.attributeNames = attributeNames;
    return SUCCESS;
}

// Private helper methods

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
    setSlotDirectoryHeader(page, slotHeader);
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
            );
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) 
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize to size of header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
            break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for nulls-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
// Memset null indicator as 1?
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator. The returned null indicator may be larger than
    // the null indicator in the table has had fields added to it
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write data to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;
        
        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

void RecordBasedFileManager::deleteRecordAtOffset(void *page, const SlotDirectoryRecordEntry &recordEntry, const RID &rid) {
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    uint16_t backupFreeSpaceOffset = slotHeader.freeSpaceOffset;
    slotHeader.freeSpaceOffset += recordEntry.length;  //Update the freeSpaceOffset after deletion
    setSlotDirectoryHeader(page, slotHeader); // Flush the updates to the array pageData

    // Update the Record entries by increasing their offset by the length of the deleted entry
    unsigned bytesToShift = 0; // This is used in the "shift data properly" part

    // shift every record after the deleted one to keep records compact
    for (unsigned i = rid.slotNum + 1; i <= slotHeader.recordEntriesNumber; i++)
    {
        SlotDirectoryRecordEntry updatedRecordEntry = getSlotDirectoryRecordEntry(page, i);
        bytesToShift += updatedRecordEntry.length;
        updatedRecordEntry.offset += recordEntry.length;
        setSlotDirectoryRecordEntry(page, i, updatedRecordEntry);
    }
    // -----

    // Shift the data properly
    void* dataToShift  = malloc(bytesToShift); // Used to store the data to shift
    memcpy(dataToShift, (char *)page + backupFreeSpaceOffset, bytesToShift); // Store data to shift in dataToShift
    memcpy((char *)page + backupFreeSpaceOffset + recordEntry.length, (char *)dataToShift, bytesToShift); // Shift data
}



// RBFM_ScanIterator methods

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) 
{
    void * pageData = malloc(PAGE_SIZE);
    // for all pages
    for (unsigned i = curRid.pageNum; i < fileHandle.getNumberOfPages(); ++i) {
        fileHandle.readPage(i, pageData);
        // get the number of records in the page
        SlotDirectoryHeader slotHeader;
        memcpy (&slotHeader, pageData, sizeof(SlotDirectoryHeader));
        free(pageData);
        // for all record in the page
        for (unsigned j = curRid.slotNum; j < slotHeader.recordEntriesNumber; ++j) {
            void * field = malloc(PAGE_SIZE);
            int fieldSize;
            // read the conditionAttribute
            readAttribute(fileHandle, recordDescriptor, {i, j}, conditionAttribute, field, fieldSize);
            // figure out the type of the field
            AttrType conAttrType;
            for (size_t i = 0; i < recordDescriptor.size(); ++i) {
                if (recordDescriptor[i].name == conditionAttribute) {
                    conAttrType = recordDescriptor[i].type;
                    break;
                }
            }
            // compare to value
            int cmp; // comparson result
            switch (conAttrType) {
                case TypeInt: // skip the NI byte
                    cmp = memcmp((char *)field + 1, value, INT_SIZE);
                    break;
                case TypeReal: // skip the NI byte
                    cmp = memcmp((char *)field + 1, value, REAL_SIZE);
                    break;
                case TypeVarChar: // skip the NI byte + VarCharLength byte
                    cmp = memcmp((char *)field + 5, value, fieldSize);
                    break;
            }
            bool condition = false;
            switch (compOp) {
                case EQ_OP: if (cmp == 0) condition = true; break;
                case LT_OP: if (cmp < 0) condition = true; break;
                case LE_OP: if (cmp <= 0) condition = true; break;
                case GT_OP: if (cmp > 0) condition = true; break;
                case GE_OP: if (cmp >= 0) condition = true; break;
                case NE_OP: if (cmp != 0) condition = true; break;
                case NO_OP: condition = true; break;
            }
            if (condition) { // if satisfy condition, retrieve attributes
                int numOfAttrs = attributeNames.size();
                int nullIndicatorSize = int(ceil((double) numOfAttrs / CHAR_BIT));
                char nullIndicator[nullIndicatorSize];
                memset(nullIndicator, 0, nullIndicatorSize);
                int offset = nullIndicatorSize;
                for (int k = 0; k < numOfAttrs; ++k) {
                    readAttribute(fileHandle, recordDescriptor, {i, j}, attributeNames[k], field, fieldSize);
                    // if the first byte of field isn't 0, the field is null
                    // set the corresponding NI bit to 1
                    // the corresponding bit is at byte k/8, bit k%8
                    if (*(char*)field) nullIndicator[k / CHAR_BIT] |= 1 << (k % CHAR_BIT);
                    // skip the first bytes (NI bytes) of field
                    memcpy((char*)data + offset, (char*)field + 1, fieldSize);
                    offset += fieldSize;
                }
                // finally copy NI
                memcpy(data, nullIndicator, nullIndicatorSize);
                return SUCCESS;
            }
        }
    }
    return RBFM_EOF;
}

// private helper

SlotDirectoryHeader RBFM_ScanIterator::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

SlotDirectoryRecordEntry RBFM_ScanIterator::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

RC RBFM_ScanIterator::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data, int &fieldSize)
{
    // checking the existence of the attribute
    bool attrFound = false; 
    size_t i = 0; // get the ith field
    size_t fieldCount = recordDescriptor.size();
    for (; i < fieldCount; ++i) {
        if (recordDescriptor[i].name == attributeName) {
            attrFound = true;
            break;
        }
    }
    if (!attrFound) return RBFM_ATTR_NOTFOUND;

    void * pageData = malloc(PAGE_SIZE);
    // read page by pageNum
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;
    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;
    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    
    // handle forwrding recursively
    if (recordEntry.offset >= 0) {
        if (recordEntry.length <= 0) {  // it's not deleted before
            free(pageData);
            return RBFM_RECORD_DELETED;
        }
        // record format: |Num of Record|NI|CO1|CO2|...|F1|F2|...
        // reading the ith field
        // get to the start of the record
        char * start = (char *) pageData + recordEntry.offset;
        int rec_offset = 0; // offset relates to the start of the record
        rec_offset += 2; // jump across Num of Record
        // get the nullIndicator
        int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
        char nullIndicator[nullIndicatorSize];
        memset(nullIndicator, 0, nullIndicatorSize);
        memcpy(nullIndicator, start + rec_offset, nullIndicatorSize);
        rec_offset += nullIndicatorSize;
        if (fieldIsNull(nullIndicator, i)) { // if the field is null, memcpy 1000 0000 and done
            unsigned a = (1 << (CHAR_BIT - 1));
            memcpy(data, &a, 1);
        } else {
            unsigned a = (0 << (CHAR_BIT - 1));
            memcpy(data, &a, 1); // if the field isn't null, memcpy 0000 0000
            ColumnOffset fieldStart, fieldEnd;
            
            // the ith field starts at the END of the (i-1)th field
            rec_offset += (sizeof(ColumnOffset) * (i - 1));
            memcpy(&fieldStart, start + rec_offset, sizeof(ColumnOffset));

            // where the ith field ends
            rec_offset += sizeof(ColumnOffset); 
            memcpy(&fieldEnd, start + rec_offset, sizeof(ColumnOffset));

            // so we can get the size of the field
            fieldSize = fieldEnd - fieldStart;
            memcpy((char *)data + 1, start + fieldStart, fieldSize); // get the field
        }
        free(pageData);
    } else {
        free(pageData);
        RID newrid = {recordEntry.length, (uint32_t)(- recordEntry.offset)};
        readAttribute(fileHandle, recordDescriptor, newrid, attributeName, data, fieldSize);
    }
    return SUCCESS;
}

RC RBFM_ScanIterator::close()
{
    curRid = {0, 0};
    return SUCCESS;
};

int RBFM_ScanIterator::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RBFM_ScanIterator::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}
