#include <string>
#include <bits/stdc++.h>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"
#include "Swap.h"

using namespace std;


DBFile::DBFile () :
	fileName(""), fileStatus(-1), page_idx(0) {}

DBFile::~DBFile () {
    current_page.EmptyItOut();
	file.Close();
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	fileName(_copyMe.fileName), fileStatus(_copyMe.fileStatus),
	page_idx(_copyMe.page_idx) {
}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;
	fileStatus = _copyMe.fileStatus;
	page_idx = _copyMe.page_idx;

	return *this;
}


int DBFile::Create (char* f_path, FileType f_type) {

	if (f_type != Heap) {
		cout << "Heap files are only supported" << endl;
		return -1;
	}

	// creating new file with zero file length
	int status = file.Open(0, f_path);
	if (status == 0) {
		fileName = f_path;
        fileStatus = 0;
		return 0;
	}
	return -1;
}

int DBFile::Open (char* f_path) {

	FILE *heap_file_data = fopen(f_path, "rb");

	if (heap_file_data == nullptr) {
		cout << "Input file wasn't found" << endl;
		return -1;
	}

	fseek(heap_file_data, 0, SEEK_END);
	off_t file_size = ftell (heap_file_data);
	fclose(heap_file_data);

	int status = file.Open(file_size, f_path);
	if (status == 0) {
		fileName = f_path;
		page_idx = -1; // reading file metadata
		return 0;
	}
	return -1;
}

void DBFile::Swap(DBFile& _other) {
	SWAP(fileName, _other.fileName);
	OBJ_SWAP(file, _other.file);
	SWAP(fileStatus, _other.fileStatus);
	SWAP(page_idx, _other.page_idx);
	OBJ_SWAP(current_page, _other.current_page);
}

void DBFile::Load (Schema& schema, char* textFile) {

    //MoveFirst(); //TODO: DBFiles needs to be deleted from the stack
	// db_file must be opened before loading
	if (fileStatus == -1) {
		cout << "Database file is not open" << endl;
		exit(-1);
	}

	FILE *text_data = fopen(textFile, "rt");

	if (text_data == nullptr) {
		cout << "Input file wasn't found" << endl;
		exit(-1);
	}

	while(true) {
		Record record;
		if(record.ExtractNextRecord(schema, *text_data)) {
			AppendRecord(record);
		} else { //EOF
			file.AddPage(current_page, page_idx); // writing the last page
			//current_page.EmptyItOut();
			break;
		}
	}

	fclose(text_data);
}

int DBFile::Close () {
	fileStatus = -1;
	return file.Close();
}

void DBFile::MoveFirst () {
	page_idx = 0;
	if (file.GetPage(current_page, page_idx) == -1) {
		cout << "failed to extract first page from the file: " << fileName << endl;
		exit(-1);
	}
    fileStatus = 0;
}

void DBFile::AppendRecord (Record& rec) {

    if (current_page.Append(rec) == 0) { // page is full
        file.AddPage(current_page, page_idx);
        current_page.EmptyItOut();
        page_idx++;
        AppendRecord(rec);
    }
}

int DBFile::GetNext (Record& rec) {
	if (fileStatus == -1) {
		unsigned long heap_file_name_len = fileName.length() + 1;
		char heap_file_path[heap_file_name_len];
		strcpy(heap_file_path, fileName.c_str());
		if (Open(heap_file_path) == -1) {
			cout << "the input heap file failed to open: " << fileName << endl;
			exit(-1);
		}
		MoveFirst();
	}

	// file current page is null or it is empty
    if (current_page.GetFirst(rec) == 0) {
        page_idx++;
        if (page_idx + 1 == file.GetLength() || file.GetPage(current_page, page_idx) == -1) {
            return -1; // EOF or error
        }
		if (current_page.GetFirst(rec) == 0) {
            cout << "failed to extract pages from the file: " << fileName << endl;
            exit(-1);
		}
    }

    if (rec.GetBits() == NULL) {
        cout << "failed to read the next record" << endl;
        exit(-1);
    }

	return 0;
}
