#pragma once

#include "constants.h"
#include "data.h"

#include <array>
#include <map>
#include <optional>
#include <iostream>

// assumption for branching factor: BLOCK_SIZE is big enough to accomodate a node with at least two keys and values
// it is better to directly allocate BLOCK_SIZE for Node and allocate memory for KeyValue inside that memory space
class Index {
	struct Node;
	union Value {
		Node* child;
		Int64 rid;
	};

	struct KeyValue {
		PackedData key;
		Value value;
		KeyValue(KeyValue&& kv) noexcept {
			key = std::move(kv.key);
			value = kv.value; // Value is a POD type
		}
		KeyValue& operator=(KeyValue&& kv) noexcept {
			key = std::move(kv.key);
			value = kv.value;
			return *this;
		}
		KeyValue(const PackedData& key, Int64 rid) :
			key{key}, value{.rid = rid} {}
		KeyValue(Node* child) :
			key(), value{.child = child} {}
		KeyValue(const PackedData& key, Node* child) :
			key{key}, value{.child = child} {}
	};

	// for internal nodes, the last element of kvs contains null key, which is greater than any other key
	struct Node {
		std::vector<KeyValue> kvs;
		std::vector<KeyValue> kvsUnsorted;
		int numKvs{ 0 };

		std::vector<KeyValue> kvsToInsert;
		std::vector<KeyValue> kvsToRemove;

		std::vector<KeyValue>::iterator parentIt;
		Node* prev{ nullptr };
		Node* next{ nullptr };
		bool isLeaf;
		Node(bool isLeaf, int maxBranchingFactor, int maxLazySize) :
			isLeaf(isLeaf) {
			kvs.reserve(maxBranchingFactor);
			kvsUnsorted.reserve(maxLazySize);
			kvsToInsert.reserve(maxLazySize);
			kvsToRemove.reserve(maxLazySize);
		}
	};

	struct Result {
		bool hasMerged{ false };
		std::optional<Index::KeyValue> kvToInsert{};
	};

public:
	Index(const std::vector<DataType>& types, const std::vector<std::string>& names, bool allowsDuplicate); 
	Index(Index&& other) noexcept;
	~Index();

	// returns true if success
	bool insert(const PackedData& key, Int64 rid, bool checksIntegrity=false);
	// returns true if success
	bool remove(const PackedData& key, Int64 rid, bool checksIntegrity=false);
	// returns rids: equal search
	std::vector<int> select(const PackedData& key);
	// returns rids: range search
	std::vector<int> select(const PackedData& loKey, const PackedData& hiKey);
	// returns true if exists
	bool select(const PackedData& key, Int64 rid);
	void dump(std::ostream& os = std::cout);
	void checkIntegrity();

private:
	const bool allowsDuplicate;
	const int maxBranchingFactor;
	const int maxLazySize;
	const std::vector<DataType> types;
	const std::vector<std::string> names;
	Node* root;

	Result insert(Node* curr, std::vector<KeyValue>&& tempKvs);
	Result remove(Node* curr, std::vector<KeyValue>&& tempKvs);
	// merge unsortedKvs into kvs and remove deleted kvs
	void sortKvs(Node* curr);
	void invalidateDuplicate(std::vector<KeyValue>& kvs1, std::vector<KeyValue>& kvs2);
	// sort and perform split, redistribute, merge if necessary
	Result maintain(Node* curr);
	// raise or lower the depth if necessary
	void maintainRoot(Result&& res);
	std::vector<int> select(Node* curr, const PackedData& key);
	void clean(Node* curr);

	void dump(Node* curr, std::ostream& os);
	void dump(const std::vector<KeyValue>& kvs, bool printsRID, std::ostream& os);
	void checkIntegrity(Node* curr, const PackedData& lb, bool existsLB, const PackedData& ub);

	static int computeBranchingFactor(const std::vector<DataType>& types, int size);
	int comparePackData(const PackedData& data1, const PackedData& data2);
	bool compareKeyValue(const KeyValue& kv1, const KeyValue& kv2);
};