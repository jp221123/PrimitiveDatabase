#pragma once

#include <string>
#include <vector>
#include <array>

enum class DataType {
	INT32,
	INT64,
	STRING,
	DATE,
	DATETIME,
	HASHED_INT,
};

using Int32 = std::int32_t;
using Int64 = std::int64_t;
using String = std::string;

struct Date {
public:
	Date();
	Date(Int32 days) : days(days) {}
	Int32 data() const { return days; };
private:
	Int32 days;
};

struct DateTime {
public:
	DateTime();
	DateTime(Int64 seconds) : seconds(seconds) {}
	Int64 data() const { return seconds; }
private:
	Int64 seconds;
};

// rolling hash
struct HashedInt {
public:
	HashedInt(const std::string& s);
	HashedInt(Int64 hash) : hash(hash) {}
	Int64 data() const { return hash; }
private:
	Int64 hash;
	static constexpr Int64 BASE = 1'000'000'007;
	static constexpr Int64 MOD = 1'000'000'009;
	//static Int64 pow(char ch) {
	//	static bool init = false;
	//	static std::array<Int64, 256> precomputed;
	//	if (!init) {
	//		init = true;
	//		precomputed[0] = 1;
	//		for (int i = 1; i < 256; i++)
	//			precomputed[i] = precomputed[i - 1] * BASE % MOD;
	//	}
	//	return precomputed[ch];
	//}
};

class PackedData {
public:
	PackedData();
	PackedData(const std::vector<DataType>& types, const std::vector<std::string>& data);
	PackedData(int capacity);

	PackedData(const PackedData& other);
	PackedData(PackedData&& other) noexcept;
	PackedData& operator=(const PackedData& other);
	PackedData& operator=(PackedData&& other) noexcept;
	void reset();
	~PackedData();

	void push(std::int32_t val);
	void push(std::int64_t val);
	void push(const std::string& val);
	void push(std::string&& val);

	void push(const Date& val);
	void push(const DateTime& val);
	void push(const HashedInt& val);

	static PackedData combine(const PackedData& data, std::int64_t val);

	int size() const { return (int)_size; }
	void* get() const { return _base; }
	static int computeSize(const std::vector<DataType>& types);
private:
	void* _base;
	size_t _size;
	size_t _capacity;

	void grow();
};