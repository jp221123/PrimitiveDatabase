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
	_base = nullptr;
	_capacity = 0;
	_size = 0;
}

PackedData::PackedData(const std::vector<DataType>& types, const std::vector<std::string>& data)
{
	int n = types.size();
	_capacity = computeSize(types);
	_base = malloc(_capacity);
	_size = 0;
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

PackedData::PackedData(int capacity) :
	_capacity(capacity)
{
	_base = malloc(capacity);
	_size = 0;
}

PackedData::PackedData(const PackedData& other)
{
	assert(other._capacity != 0);
	assert(other._base != nullptr);
	_capacity = other._capacity;
	_base = malloc(_capacity);
	_size = other._size;
	{
		auto dst = static_cast<std::int32_t*>(_base);
		auto src = static_cast<std::int32_t*>(other._base);
		for (int i = 0; i < _size / 4; i++)
			*(dst + i) = *(src + i);
	}
	if(_size % 4 != 0){
		int r = _size % 4;
		auto dst = static_cast<std::byte*>(_base);
		auto src = static_cast<std::byte*>(other._base);
		for (int i = _size - 1; i >= _size - r; i--)
			*(dst + i) = *(src + i);
	}
}

PackedData::PackedData(PackedData&& other) noexcept
{
	_capacity = other._capacity;
	_size = other._size;
	free(_base);
	_base = other._base;
	other._base = nullptr;
}

PackedData& PackedData::operator=(const PackedData& other)
{
	assert(other._capacity != 0);
	assert(other._base != nullptr);
	if (_capacity < other._size) {
		free(_base);
		_capacity = other._size;
		_base = malloc(_capacity);
	}
	_size = other._size;
	{
		auto dst = static_cast<std::int32_t*>(_base);
		auto src = static_cast<std::int32_t*>(other._base);
		for (int i = 0; i < _size / 4; i++)
			*(dst + i) = *(src + i);
	}
	if (_size % 4 != 0) {
		int r = _size % 4;
		auto dst = static_cast<std::byte*>(_base);
		auto src = static_cast<std::byte*>(other._base);
		for (int i = _size - 1; i >= _size - r; i--)
			*(dst + i) = *(src + i);
	}
	return *this;
}

PackedData& PackedData::operator=(PackedData&& other) noexcept
{
	_capacity = other._capacity;
	_size = other._size;
	free(_base);
	_base = other._base;
	other._base = nullptr;
	return *this;
}

void PackedData::reset()
{
	free(_base);
	_base = nullptr;
	_capacity = 0;
	_size = 0;
}

PackedData::~PackedData()
{
	free(_base);
}

void PackedData::push(std::int32_t val)
{
	while (_size + sizeof(val) > _capacity)
		grow();
	auto ptr = reinterpret_cast<std::int32_t*> ((size_t)_base + _size);
	*ptr = val;
	_size += sizeof(val);
}

void PackedData::push(std::int64_t val)
{
	while (_size + sizeof(val) > _capacity)
		grow();
	auto ptr = reinterpret_cast<std::int64_t*> ((size_t)_base + _size);
	*ptr = val;
	_size += sizeof(val);
}

void PackedData::push(const std::string& val)
{
	while (_size + sizeof(val) > _capacity)
		grow();
	auto ptr = reinterpret_cast<std::string*> ((size_t)_base + _size);
	*ptr = val;
	_size += sizeof(val);
}

void PackedData::push(std::string&& val)
{
	while (_size + sizeof(val) > _capacity)
		grow();
	auto ptr = reinterpret_cast<std::string*> ((size_t)_base + _size);
	*ptr = std::move(val);
	_size += sizeof(val);
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

int PackedData::computeSize(const std::vector<DataType>& types)
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
	if (_capacity == 0) {
		_capacity = 4;
		_base = malloc(_capacity);
		return;
	}
	_capacity *= 2;
	void* dest = malloc(_capacity);
	std::memcpy(dest, _base, _size);
	delete _base;
	_base = dest;
}