#include "index.h"

#include <cmath>
#include <algorithm>
#include <cassert>
#include <iostream>
#include <functional>

Index::Index(const std::vector<DataType>& types, const std::vector<std::string>& names, bool allowsDuplicate) :
	types(types), names(names), allowsDuplicate(allowsDuplicate),
	maxBranchingFactor(computeBranchingFactor(types, BLOCK_SIZE)),
	maxLazySize(sqrt(maxBranchingFactor)), // (13.3) - Then, non-static data members are initialized in the order they were declared in the class definition (again regardless of the order of the mem-initializers).
	root(new Node(true, maxBranchingFactor, maxLazySize))
{
}

Index::Index(Index&& other) noexcept :
	types(std::move(other.types)), names(std::move(other.names)), allowsDuplicate(other.allowsDuplicate),
	maxBranchingFactor(other.maxBranchingFactor), maxLazySize(other.maxLazySize),
	root(other.root)
{
	other.root = nullptr;
}

Index::~Index()
{
	clean(root);
}

bool Index::insert(const PackedData& key, int rid, bool checksIntegrity)
{
	if(checksIntegrity && !allowsDuplicate && !select(key).empty())
		return false;

	std::vector<KeyValue> tempKvs;
	tempKvs.push_back(KeyValue(key, rid));
	auto res = insert(root, std::move(tempKvs));
	maintainRoot(std::move(res));
	return true;
}

std::optional<Index::KeyValue> Index::insert(Node* curr, std::vector<KeyValue>&& tempKvs)
{
	assert(!tempKvs.empty());

	curr->kvsToInsert.insert(curr->kvsToInsert.end(),
		std::make_move_iterator(tempKvs.begin()),
		std::make_move_iterator(tempKvs.end()));

	if (curr->kvsToInsert.size() <= maxLazySize)
		return {};

	if (!curr->isLeaf) {
		// pushdown kvsToInsert in the correct children

		auto cmp = std::bind(&Index::compareKeyValue, this, std::placeholders::_1, std::placeholders::_2);
		std::sort(curr->kvsToInsert.begin(), curr->kvsToInsert.end(), cmp);
		std::sort(curr->kvsUnsorted.begin(), curr->kvsUnsorted.end(), cmp);

		auto downIt = curr->kvsToInsert.begin();
		auto upIt = curr->kvsUnsorted.begin();
		auto mainIt = curr->kvs.begin();

		std::vector<KeyValue> pulledUp;

		while (mainIt != curr->kvs.end() || upIt != curr->kvsUnsorted.end()) {
			auto& it =
				upIt == curr->kvsUnsorted.end() ? mainIt :
				mainIt == curr->kvs.end() ? upIt :
				compareKeyValue(*mainIt, *upIt) ? mainIt : upIt;

			if (it->value.child == nullptr) {
				it++;
				continue;
			}

			std::vector<KeyValue> pd;
			while (downIt != curr->kvsToInsert.end()) {
				if (downIt->value.rid == -1) {
					downIt++;
					continue;
				}
				auto res = compareKeyValue(*downIt, *it);
				if (!res)
					break;
				// *downIt < *it
				pd.push_back(std::move(*downIt));
				downIt++;
			}
			if (!pd.empty()) {
				auto res = insert(it->value.child, std::move(pd));
				if (res.has_value())
					pulledUp.push_back(std::move(res.value()));
			}

			it++;
		}
		assert(downIt == curr->kvsToInsert.end());
		curr->kvsToInsert.clear();

		curr->numKvs += pulledUp.size();
		curr->kvsUnsorted.insert(curr->kvsUnsorted.end(),
			std::make_move_iterator(pulledUp.begin()),
			std::make_move_iterator(pulledUp.end()));
		for(auto it = curr->kvsUnsorted.end() - pulledUp.size(); it != curr->kvsUnsorted.end(); it++)
			it->value.child->parentIt = it;

		if (curr->kvsUnsorted.size() <= maxLazySize && curr->numKvs >= (maxBranchingFactor+1)/2)
			return {};

		std::sort(curr->kvsUnsorted.begin(), curr->kvsUnsorted.end(), cmp);
	}
	else {
		// kvsUnsorted are simply not used in a leaf node
		curr->numKvs += curr->kvsToInsert.size();
		for (auto& kv : curr->kvsToInsert)
			if (kv.value.rid == -1)
				curr->numKvs--;
		std::swap(curr->kvsUnsorted, curr->kvsToInsert);
	}

	return maintain(curr);
}

bool Index::remove(const PackedData& key, int rid, bool checksIntegrity)
{
	if (checksIntegrity && !select(key, rid))
		return false;

	std::vector<KeyValue> tempKvs;
	tempKvs.push_back(KeyValue(key, rid));
	auto res = remove(root, std::move(tempKvs));
	maintainRoot(std::move(res));
	return true;
}

std::optional<Index::KeyValue> Index::remove(Node* curr, std::vector<KeyValue>&& tempKvs)
{
	// discrepancy that (++parentIt).key != curr->kv.front().key is actually OK
	// as long as all keys are bounded above by the key in its parent
	assert(false);
	return {};
}

std::vector<int> Index::select(const PackedData& key)
{
	assert(false);
	// consider kvsToInsert and kvsUnsorted as well
	return std::vector<int>();
}

std::vector<int> Index::select(Node* curr, const PackedData& key)
{
	assert(false);
	return std::vector<int>();
}

std::vector<int> Index::select(const PackedData& keyLo, const PackedData& keyhi)
{
	assert(false);
	return std::vector<int>();
}

bool Index::select(const PackedData& key, int rid)
{
	assert(false);
	return false;
}

void Index::sortKvs(Node* curr)
{
	int k = curr->kvs.size();
	auto cmp = std::bind(&Index::compareKeyValue, this, std::placeholders::_1, std::placeholders::_2);
	std::sort(curr->kvsUnsorted.begin(), curr->kvsUnsorted.end(), cmp);
	curr->kvs.insert(curr->kvs.end(),
		std::make_move_iterator(curr->kvsUnsorted.begin()),
		std::make_move_iterator(curr->kvsUnsorted.end()));
	std::inplace_merge(curr->kvs.begin(), curr->kvs.begin() + k, curr->kvs.end(), cmp);
	curr->kvsUnsorted.clear();
	auto it = curr->kvs.begin();
	auto it2 = curr->kvs.begin();
	while (it2 != curr->kvs.end()) {
		if (curr->isLeaf) {
			while (it2 != curr->kvs.end() && it2->value.rid == -1)
				it2++;
		}
		else {
			while (it2 != curr->kvs.end() && it2->value.child == nullptr)
				it2++;
		}
		if (it != it2)
			std::swap(*it, *it2);
		if (!curr->isLeaf)
			it->value.child->parentIt = it;
		it++;
		it2++;
	}
	assert(it - curr->kvs.begin() == curr->numKvs);
	int numRemoved = curr->kvs.end() - it;
	for (int i = 0; i < numRemoved; i++)
		curr->kvs.pop_back();
	assert(curr->kvs.size() == curr->numKvs);
	assert(curr->kvsUnsorted.size() == 0);
}

std::optional<Index::KeyValue> Index::maintain(Node* curr)
{
	sortKvs(curr);
	if (curr != root && curr->numKvs < (maxBranchingFactor + 1) / 2) {
		//redistribute();
		assert(false);
		return {};
	}

	if (curr->numKvs <= maxBranchingFactor)
		return {};

	// split curr->kvs
	int k = curr->numKvs / 2;
	auto prev = new Node(curr->isLeaf, maxBranchingFactor, maxLazySize);
	if (curr->prev != nullptr) {
		curr->prev->next = prev;
		prev->prev = curr->prev;
	}
	curr->prev = prev;
	prev->next = curr;
	prev->kvs.insert(prev->kvs.end(),
		std::make_move_iterator(curr->kvs.begin()),
		std::make_move_iterator(curr->kvs.begin() + k));
	if (curr->isLeaf) {
		// copy the k-th kv and pull it up
		int n = curr->numKvs;
		curr->kvs.erase(curr->kvs.begin(), curr->kvs.begin() + k);
		prev->numKvs = prev->kvs.size();
		curr->numKvs = curr->kvs.size();
		return KeyValue(curr->kvs.front().key, prev);
	}
	else {
		// erase the k-th kv and pull it up
		auto key = std::move(curr->kvs[k].key);
		prev->kvs.emplace_back(curr->kvs[k].value.child);
		curr->kvs.erase(curr->kvs.begin(), curr->kvs.begin() + k + 1);
		prev->numKvs = prev->kvs.size();
		curr->numKvs = curr->kvs.size();
		return KeyValue(std::move(key), prev);
	}
}

void Index::maintainRoot(std::optional<KeyValue>&& res) {
	if (res.has_value()) {
		auto node = new Node(false, maxBranchingFactor, maxLazySize);
		node->kvs.push_back(std::move(res.value()));
		node->kvs.emplace_back(root);
		node->kvs.front().value.child->parentIt = node->kvs.begin();
		node->kvs.back().value.child->parentIt = node->kvs.begin() + 1;
		node->numKvs = 2;
		root = node;
	}
	else if (root->numKvs == 1 && !root->isLeaf) {
		auto node = root->kvs.front().value.child;
		delete root;
		root = node;
	}
}

void Index::clean(Node* curr)
{
}

void Index::dump(std::ostream& os)
{
	os << "==========dump start==========\n";
	dump(root, os);
	os << "==========dump end==========\n\n";
}

void Index::dump(Node* curr, std::ostream& os)
{
	os << curr << "\n";
	os << "numKvs = " << curr->numKvs << "\n";
	os << "kvs = ";
	dump(curr->kvs, curr->isLeaf, os);
	os << "kvsToInsert = ";
	dump(curr->kvsToInsert, curr->isLeaf, os);
	os << "kvsUnsorted = ";
	dump(curr->kvsUnsorted, curr->isLeaf, os);
	if (!curr->isLeaf) {
		for (auto& kv : curr->kvs) {
			if (kv.value.child != nullptr)
				dump(kv.value.child, os);
		}
		for (auto& kv : curr->kvsUnsorted) {
			if (kv.value.child != nullptr)
				dump(kv.value.child, os);
		}
	}
}

void Index::dump(const std::vector<KeyValue>& kvs, bool isLeaf, std::ostream& os)
{
	os << "[";
	for (auto& kv : kvs) {
		os << "(";
		if (kv.key.get() == nullptr)
			os << "null";
		else {
			os << "(";
			std::byte* ptr = static_cast<std::byte*>(kv.key.get());
			for (auto& t : types) {
				switch (t) {
				case DataType::INT32:
				case DataType::DATE:
					{
						Int32* val = reinterpret_cast<Int32*>(ptr);
						os << *val << " ";
						ptr += sizeof(Int32);
						break;
					}
				case DataType::INT64:
				case DataType::DATETIME:
				case DataType::HASHED_INT:
					{
						Int64* val = reinterpret_cast<Int64*>(ptr);
						os << *val << " ";
						ptr += sizeof(Int64);
						break;
					}
				case DataType::STRING:
					{
						String* val = reinterpret_cast<String*>(ptr);
						os << *val << " ";
						ptr += sizeof(String);
						break;
					}
				}
			}
			os << ")";
		}
		os << ",";
		if (isLeaf)
			os << kv.value.rid;
		else
			os << kv.value.child;
		os << "),";
	}
	os << "]\n";
}

void Index::checkIntegrity()
{
	checkIntegrity(root, PackedData());
}

void Index::checkIntegrity(Node* curr, const PackedData& ub)
{
	if (!curr->isLeaf) {
		for (auto& kv : curr->kvs) {
			assert(kv.key.get() == nullptr || kv.value.child == nullptr || comparePackData(kv.key, ub) < 0);
			if (kv.value.child != nullptr)
				checkIntegrity(kv.value.child, kv.key);
		}
		for (auto& kv : curr->kvsUnsorted) {
			assert(kv.key.get() == nullptr || kv.value.child == nullptr || comparePackData(kv.key, ub) < 0);
			if (kv.value.child != nullptr)
				checkIntegrity(kv.value.child, kv.key);
		}
	}
	else {
		for (auto& kv : curr->kvs) {
			assert(kv.key.get() == nullptr || kv.value.rid == -1 || comparePackData(kv.key, ub) < 0);
		}
		for (auto& kv : curr->kvsUnsorted) {
			assert(kv.key.get() == nullptr || kv.value.rid == -1 || comparePackData(kv.key, ub) < 0);
		}
	}
}

int Index::computeBranchingFactor(const std::vector<DataType>& types, int size)
{
	int keySize = PackedData::getSize(types);
	keySize += 3 - (keySize + 3) % 4;
	auto computeSize = [=](int k) {
		int size = sizeof(Node);
		size -= sizeof(std::vector<KeyValue>) * 3;
		size -= sizeof(std::vector<KeyValue>::iterator);
		size += (keySize + sizeof(Value)) * k;
		size += (keySize + sizeof(Value)) * (int)sqrt(k) * 3;
		return size;
	};
	int lo = 2;
	int hi = BLOCK_SIZE;
	while (lo < hi) {
		int mid = (lo + hi + 1) / 2;
		if (computeSize(mid) > size)
			hi = mid - 1;
		else // computeSize(mid) <= size
			lo = mid;
	}
	return lo;
}

bool Index::compareKeyValue(const KeyValue& kv1, const KeyValue& kv2)
{
	return comparePackData(kv1.key, kv2.key) < 0;
}

int Index::comparePackData(const PackedData& data1, const PackedData& data2)
{
	if (data1.get() == nullptr) {
		assert(data2.get() != nullptr);
		return 1;
	}
	if (data2.get() == nullptr)
		return -1;
	std::byte* ptr1 = static_cast<std::byte*>(data1.get());
	std::byte* ptr2 = static_cast<std::byte*>(data2.get());
	for (auto& t : types) {
		switch (t) {
		case DataType::INT32:
		case DataType::DATE:
			{
				Int32 val1 = *reinterpret_cast<Int32*>(ptr1);
				Int32 val2 = *reinterpret_cast<Int32*>(ptr2);
				if (val1 < val2)
					return -1;
				if (val1 > val2)
					return 1;
				ptr1 += sizeof(Int32);
				ptr2 += sizeof(Int32);
				break;
			}
		case DataType::INT64:
		case DataType::DATETIME:
		case DataType::HASHED_INT:
			{
				Int64 val1 = *reinterpret_cast<Int64*>(ptr1);
				Int64 val2 = *reinterpret_cast<Int64*>(ptr2);
				if (val1 < val2)
					return -1;
				if (val1 > val2)
					return 1;
				ptr1 += sizeof(Int64);
				ptr2 += sizeof(Int64);
				break;
			}
		case DataType::STRING:
			{
				int cmp = (*reinterpret_cast<String*>(ptr1))
					.compare(*reinterpret_cast<String*>(ptr2));
				if (cmp < 0)
					return -1;
				if (cmp > 0)
					return 1;
				ptr1 += sizeof(String);
				ptr2 += sizeof(String);
				break;
			}
		}
	}

	return 0;
}

