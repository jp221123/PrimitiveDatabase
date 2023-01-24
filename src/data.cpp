#include "data.h"

#include <cassert>
#include <chrono>

Date::Date()
{
	auto now_s = std::chrono::time_point_cast<std::chrono::days>(std::chrono::system_clock::now());
	days = now_s.time_since_epoch().count();
}

DateTime::DateTime()
{
	auto now_s = std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
	seconds = now_s.time_since_epoch().count();
}

HashedInt::HashedInt(const std::string& s)
{
	 hash = 0;
	 for (auto ch : s) {
		 hash *= BASE;
		 hash += ch;
		 hash %= MOD;
	 }
}

PackedData::PackedData()
{
	base = nullptr;
	capacity = 0;
	size = 0;
}

PackedData::PackedData(const std::vector<DataType>& types, const std::vector<std::string>& data)
{
	int n = types.size();
	capacity = getSize(types);
	base = malloc(capacity);
	size = 0;
	for(int i=0; i<n; i++){
		auto& type = types[i];
		switch (type) {
		case DataType::INT32:
			push(std::stoi(data[i]));
			break;
		case DataType::INT64:
			push(std::stoll(data[i]));
			break;
		case DataType::STRING:
			push(data[i]);
			break;
		case DataType::DATE:
			push(std::stoi(data[i]));
			break;
		case DataType::DATETIME:
			push(std::stoll(data[i]));
			break;
		case DataType::HASHED_INT:
			push(std::stoll(data[i]));
			break;
		}
	}
}

PackedData::PackedData(const PackedData& other)
{
	assert(other.capacity != 0);
	assert(other.base != nullptr);
	capacity = other.capacity;
	base = malloc(capacity);
	size = other.size;
	{
		auto dst = static_cast<std::int32_t*>(base);
		auto src = static_cast<std::int32_t*>(other.base);
		for (int i = 0; i < size / 4; i++)
			*(dst + i) = *(src + i);
	}
	if(size % 4 != 0){
		int r = size % 4;
		auto dst = static_cast<std::byte*>(base);
		auto src = static_cast<std::byte*>(other.base);
		for (int i = size - 1; i >= size - r; i--)
			*(dst + i) = *(src + i);
	}
}

PackedData::PackedData(PackedData&& other) noexcept
{
	capacity = other.capacity;
	size = other.size;
	free(base);
	base = other.base;
	other.base = nullptr;
}

PackedData& PackedData::operator=(PackedData&& other) noexcept
{
	capacity = other.capacity;
	size = other.size;
	free(base);
	base = other.base;
	other.base = nullptr;
	return *this;
}

PackedData::~PackedData()
{
	free(base);
}

void PackedData::push(std::int32_t val)
{
	while (size + sizeof(val) > capacity)
		grow();
	auto ptr = reinterpret_cast<std::int32_t*> ((size_t)base + size);
	*ptr = val;
	size += sizeof(val);
}

void PackedData::push(std::int64_t val)
{
	while (size + sizeof(val) > capacity)
		grow();
	auto ptr = reinterpret_cast<std::int64_t*> ((size_t)base + size);
	*ptr = val;
	size += sizeof(val);
}

void PackedData::push(const std::string& val)
{
	while (size + sizeof(val) > capacity)
		grow();
	auto ptr = reinterpret_cast<std::string*> ((size_t)base + size);
	*ptr = val;
	size += sizeof(val);
}

void PackedData::push(std::string&& val)
{
	while (size + sizeof(val) > capacity)
		grow();
	auto ptr = reinterpret_cast<std::string*> ((size_t)base + size);
	*ptr = std::move(val);
	size += sizeof(val);
}

void PackedData::push(const Date& val)
{
	push(val.data());
}

void PackedData::push(const DateTime& val)
{
	push(val.data());
}

void PackedData::push(const HashedInt& val)
{
	push(val.data());
}

int PackedData::getSize(const std::vector<DataType>& types)
{
	int size = 0;
	for (auto& type : types) {
		switch (type) {
		case DataType::INT32:
			size += sizeof(Int32);
			break;
		case DataType::INT64:
			size += sizeof(Int64);
			break;
		case DataType::STRING:
			size += sizeof(String);
			break;
		case DataType::DATE:
			size += sizeof(Date);
			break;
		case DataType::DATETIME:
			size += sizeof(DateTime);
			break;
		case DataType::HASHED_INT:
			size += sizeof(HashedInt);
		}
	}
	return size;
}

void PackedData::grow()
{
	if (capacity == 0) {
		capacity = 4;
		base = malloc(capacity);
		return;
	}
	capacity *= 2;
	void* dest = malloc(capacity);
	std::memcpy(dest, base, size);
	delete base;
	base = dest;
}