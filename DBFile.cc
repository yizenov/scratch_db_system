#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"
#include "Swap.h"

using namespace std;


DBFile::DBFile () : fileName("") {}

DBFile::~DBFile () {
	file.Close();
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	fileName(_copyMe.fileName), fileStatus(_copyMe.fileStatus) {}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;
	fileStatus = _copyMe.fileStatus;

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
	//
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
		fileStatus = 0;
		return 0;
	}
	return -1;
}

void DBFile::Swap(DBFile& _other) {
	SWAP(fileName, _other.fileName);
	OBJ_SWAP(file, _other.file);
}

void DBFile::Load (Schema& schema, char* textFile) {

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

	// initializing first page in the given file
	Page first_page;
	file.AddPage(first_page, 0);

	MoveFirst();
	while(true) {
		Record record;
		if(record.ExtractNextRecord(schema, *text_data)) {
			AppendRecord(record);
		} else { //EOF
			break;
		}
	}

	fclose(text_data);
}

int DBFile::Close () {
	file.Close();
	fileStatus = -1;
}

void DBFile::MoveFirst () {
	page_idx = 0;
	Page first_page;
	if (file.GetPage(first_page, page_idx) != 0) {
		cout << "failed to extract first page from the file: " << fileName << endl;
		exit(-1);
	}
}

void DBFile::AppendRecord (Record& rec) {
	Page current_page;
	if (file.GetPage(current_page, page_idx) == 0) {
		if (current_page.Append(rec) == 0) { // page is full
			page_idx++;
			Page new_page;
			file.AddPage(new_page, page_idx);
			AppendRecord(rec); // or new_page.Append(rec);
		}
	} else {
		cout << "failed to extract page from the file: " << fileName << " page index: " << page_idx << endl;
		exit(-1);
	}
}

int DBFile::GetNext (Record& rec) {
}
