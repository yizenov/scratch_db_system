#ifndef _DBFILE_H
#define _DBFILE_H

#include <string>

#include "Record.h"
#include "Schema.h"
#include "File.h"


class DBFile {
private:
	File file;
	string fileName;
	int fileStatus; // 0 when file is open, -1 otherwise

public:
	DBFile ();
	virtual ~DBFile ();
	DBFile(const DBFile& _copyMe);
	DBFile& operator=(const DBFile& _copyMe);

	// this function is called when a new table is created
    // return 0 on success, -1 otherwise
	int Create (char* fpath, FileType file_type);

	// file path is passed from catalog
	int Open (char* fpath);
	int Close ();

	void Swap(DBFile& _other);

	// loading from text file to heap file
	void Load (Schema& _schema, char* textFile);

	// resets the file pointer to the beginning of the heap file, i.e. the first record
	void MoveFirst ();

	// appends a record to the end of the file
	void AppendRecord (Record& _addMe);

	// get next record from the heap file
	int GetNext (Record& _fetchMe);
};

#endif //_DBFILE_H
