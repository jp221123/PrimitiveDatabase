#pragma once

#include "constants.h"
#include "data.h"

#include <array>
#include <map>
#include <optional>
#include <iostream>

// assumption for branching factor: BLOCK_SIZE is big enough to accomodate a node with at least two keys and values
// it is better to directly allocate BLOCK_SIZE for Node and allocate memory for KeyValue inside that memory space

// Value v{.child = INVALID_NODE};
// -> v.rid == INVALID_RID
// and vice versa
class Index {
	struct Node;
	union Value {
		Node* child;
		Int64 rid;
	};
	static constexpr Node* INVALID_NODE = static_cast<Node*>(nullptr);
	static constexpr Int64 INVALID_RID = reinterpret_cast<Int64>(nullptr);

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
			kvs.reserve(maxBranchingFactor + maxLazySize*2);
			kvsUnsorted.reserve(maxLazySize*2); // *2 to prevent reallocating when merge
			kvsToInsert.reserve(maxLazySize*2);
			kvsToRemove.reserve(maxLazySize*2);
		}
	};

	struct Result {
		int countMerged{0};
		std::optional<Index::KeyValue> kvToInsert{};
	};

public:
	Index(const std::vector<DataType>& types, const std::vector<std::string>& names, bool allowsDuplicate); 
	Index(Index&& other) noexcept;
	~Index();

	// returns true if success
	bool insert(const PackedData& key, Int64 rid, bool checksIntegrity=false);
	// todo: to support this operation that inserts multiple keys at once,
	// Result must be changed so that it can contain multiple kvs, and maintainRoot() must be changed as well
	bool insert(const std::vector<PackedData>& keys, const std::vector<Int64>& rids, bool checksIntegtrity = false);
	// returns true if success
	bool remove(const PackedData& key, Int64 rid, bool checksIntegrity=false);
	bool remove(const std::vector<PackedData>& keys, const std::vector<Int64>& rids, bool checksIntegtrity = false);
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
	std::vector<int> select(Node* curr, const PackedData& key);

	// merge unsortedKvs into kvs and remove invalid kvs
	void sortKvs(Node* curr);
	// invalidate kvs contained in both arrays
	void invalidateDuplicate(std::vector<KeyValue>& kvs1, std::vector<KeyValue>& kvs2);
	void pushInsert(Node* curr);
	void pushRemove(Node* curr);
	// push down kvsToInsert/Remove if necessary
	void push(Node* curr, bool forInsert);
	// perform split, redistribute, merge if necessary
	Result maintain(Node* curr);
	// raise or lower the depth if necessary
	void maintainRoot(Result&& res);
	PackedData getSmallestKey(Node* curr);

	void clean(Node* curr);

	void dump(Node* curr, std::ostream& os);
	void dump(const std::vector<KeyValue>& kvs, bool printsRID, std::ostream& os);
	void checkIntegrity(Node* curr, const PackedData& lb, bool existsLB, const PackedData& ub);

	static int computeBranchingFactor(const std::vector<DataType>& types, int size);
	int comparePackData(const PackedData& data1, const PackedData& data2);
	bool compareKeyValue(const KeyValue& kv1, const KeyValue& kv2);
	bool isInvalid(const KeyValue& kv);
};