#pragma once

#include "data.h"
#include "index.h"

#include <string>
#include <unordered_map>
#include <unordered_set>

class Field {
public:
	std::string name;
	DataType type;
	Field* refParent;
	std::unordered_set<Field*> refChildren;
private:
	// int flag;
	PackedData data;
public:
	virtual Int32 getInt32(int pos) = 0;
	virtual Int64 getInt64(int pos) = 0;
	virtual String getString(int pos) = 0;
	virtual Date getDate(int pos) = 0;
	virtual DateTime getDateTime(int pos) = 0;
	virtual HashedInt getHashedInt(int pos) = 0;

	virtual void setInt32(int pos, Int32 val) = 0;
	virtual void setInt64(int pos, Int64 val) = 0;
	virtual void setString(int pos, const String& val) = 0;
	virtual void setDate(int pos, const Date& val) = 0;
	virtual void setDateTime(int pos, const DateTime& val) = 0;
	virtual void setHashedInt(int pos, const HashedInt& val) = 0;
};

class Table {
public:
	std::string name;
private:
	std::vector<Field*> fieldList;
	std::unordered_map<std::string, int> fieldNameToNum;
	int primaryKey;
	std::list<Index> indexList;
	int size;
public:

};