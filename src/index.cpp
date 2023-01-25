#include "index.h"

#include <cmath>
#include <algorithm>
#include <cassert>
#include <iostream>
#include <functional>

std::vector<DataType> makeTypes(const std::vector<DataType>& types, bool allowsDuplicate) {
	auto res = types;
	if (allowsDuplicate)
		res.push_back(DataType::INT64);
	return res;
}

Index::Index(const std::vector<DataType>& types, const std::vector<std::string>& names, bool allowsDuplicate) :
	types(makeTypes(types, allowsDuplicate)), names(names), allowsDuplicate(allowsDuplicate),
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

bool Index::insert(const PackedData& key, Int64 rid, bool checksIntegrity)
{
	PackedData temp;
	if (!allowsDuplicate)
		temp = key;
	else {
		PackedData pair(key.size() + sizeof(Int64));
		pair = key;
		pair.push(rid);
		temp = std::move(pair);
	}

	if(checksIntegrity && !allowsDuplicate && !select(temp).empty())
		return false;

	std::vector<KeyValue> tempKvs;
	tempKvs.push_back(KeyValue(std::move(temp), rid));
	auto res = insert(root, std::move(tempKvs));
	maintainRoot(std::move(res));
	return true;
}

Index::Result Index::insert(Node* curr, std::vector<KeyValue>&& tempKvs)
{
	if (tempKvs.empty())
		return {};

	curr->kvsToInsert.insert(curr->kvsToInsert.end(),
		std::make_move_iterator(tempKvs.begin()),
		std::make_move_iterator(tempKvs.end()));

	if (curr->kvsToInsert.size() <= maxLazySize)
		return {};

	if (!curr->isLeaf) {
		// pushdown kvsToInsert in the correct children
		sortKvs(curr);
		invalidateDuplicate(curr->kvsToInsert, curr->kvsToRemove);

		auto itToInsert = curr->kvsToInsert.begin();
		auto it = curr->kvs.begin();

		std::vector<KeyValue> pulledUp;

		while (it != curr->kvs.end()){
			if (it->value.child == nullptr) {
				it++;
				continue;
			}

			std::vector<KeyValue> pd;
			while (itToInsert != curr->kvsToInsert.end()) {
				if (itToInsert->value.rid == -1) {
					itToInsert++;
					continue;
				}
				auto res = compareKeyValue(*itToInsert, *it);
				if (!res)
					break;
				// *itToInsert < *it
				pd.push_back(std::move(*itToInsert));
				itToInsert++;
			}
			auto res = insert(it->value.child, std::move(pd));
			if (res.hasMerged) {
				assert(!res.kvToInsert.has_value());
				curr->numKvs--;
			}
			if (res.kvToInsert.has_value()) {
				assert(!res.hasMerged);
				pulledUp.push_back(std::move(res.kvToInsert.value()));
			}
			it++;
		}
		assert(itToInsert == curr->kvsToInsert.end()); // since the last key is always null, greater than anything else
		curr->kvsToInsert.clear();

		curr->numKvs += (int)pulledUp.size();
		curr->kvsUnsorted.insert(curr->kvsUnsorted.end(),
			std::make_move_iterator(pulledUp.begin()),
			std::make_move_iterator(pulledUp.end()));
		for(auto it = curr->kvsUnsorted.end() - pulledUp.size(); it != curr->kvsUnsorted.end(); it++)
			it->value.child->parentIt = it;
	}
	else {
		// finally stop pushing down at a leaf node
		curr->numKvs += (int)curr->kvsToInsert.size();
		for (auto& kv : curr->kvsToInsert)
			if (kv.value.rid == -1)
				curr->numKvs--;
		std::swap(curr->kvsUnsorted, curr->kvsToInsert);
	}

	return maintain(curr);
}

bool Index::remove(const PackedData& key, Int64 rid, bool checksIntegrity)
{
	if (checksIntegrity && !select(key, rid))
		return false;

	PackedData temp;
	if (!allowsDuplicate)
		temp = key;
	else {
		PackedData pair(key.size() + sizeof(Int64));
		pair = key;
		pair.push(rid);
		temp = std::move(pair);
	}

	std::vector<KeyValue> tempKvs;
	tempKvs.push_back(KeyValue(std::move(temp), rid));

	auto res = remove(root, std::move(tempKvs));
	maintainRoot(std::move(res));
	return true;
}

Index::Result Index::remove(Node* curr, std::vector<KeyValue>&& tempKvs)
{
	// discrepancy that (++parentIt).key != curr->kv.front().key is actually OK
	// as long as all keys are bounded above by the corresponding key in its parent
	if (tempKvs.empty())
		return {};

	curr->kvsToRemove.insert(curr->kvsToRemove.end(),
		std::make_move_iterator(tempKvs.begin()),
		std::make_move_iterator(tempKvs.end()));

	if (curr->kvsToRemove.size() <= maxLazySize)
		return {};

	if (!curr->isLeaf) {
		// pushdown kvsToRemove in the correct children
		sortKvs(curr);
		invalidateDuplicate(curr->kvsToInsert, curr->kvsToRemove);

		auto itToRemove = curr->kvsToRemove.begin();
		auto it = curr->kvs.begin();

		std::vector<KeyValue> pulledUp;

		while (it != curr->kvs.end()) {
			if (it->value.child == nullptr) {
				it++;
				continue;
			}

			std::vector<KeyValue> pd;
			while (itToRemove != curr->kvsToRemove.end()) {
				if (itToRemove->value.rid == -1) {
					itToRemove++;
					continue;
				}
				auto res = compareKeyValue(*itToRemove, *it);
				if (!res)
					break;
				// *itToRemove < *it
				pd.push_back(std::move(*itToRemove));
				itToRemove++;
			}
			auto res = remove(it->value.child, std::move(pd));
			if (res.hasMerged) {
				assert(!res.kvToInsert.has_value());
				curr->numKvs--;
			}
			if (res.kvToInsert.has_value()) {
				assert(!res.hasMerged);
				pulledUp.push_back(std::move(res.kvToInsert.value()));
			}
			it++;
		}
		assert(itToRemove == curr->kvsToRemove.end()); // since the last key is always null, greater than anything else
		curr->kvsToRemove.clear();

		curr->numKvs += (int)pulledUp.size();
		curr->kvsUnsorted.insert(curr->kvsUnsorted.end(),
			std::make_move_iterator(pulledUp.begin()),
			std::make_move_iterator(pulledUp.end()));
		for (auto it = curr->kvsUnsorted.end() - pulledUp.size(); it != curr->kvsUnsorted.end(); it++)
			it->value.child->parentIt = it;
	}
	else {
		// finally stop pushing down at a leaf node
		invalidateDuplicate(curr->kvsToInsert, curr->kvsToRemove);
		curr->numKvs -= (int)curr->kvsToRemove.size();
		for (auto& kv : curr->kvsToRemove)
			if (kv.value.rid == -1)
				curr->numKvs++;
		sortKvs(curr);
		invalidateDuplicate(curr->kvs, curr->kvsToRemove);
		sortKvs(curr);
		for (auto& kv : curr->kvsToRemove)
			assert(kv.value.rid == -1);
		curr->kvsToRemove.clear();
	}

	return maintain(curr);
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

bool Index::select(const PackedData& key, Int64 rid)
{
	assert(false);
	return false;
}

void Index::sortKvs(Node* curr)
{
	if (curr->kvsUnsorted.empty() && curr->kvs.size() == curr->numKvs)
		return;

	// merge kvsUnsorted into kvs
	if (!curr->kvsUnsorted.empty()) {
		int k = (int)curr->kvs.size();
		auto cmp = std::bind(&Index::compareKeyValue, this, std::placeholders::_1, std::placeholders::_2);
		std::sort(curr->kvsUnsorted.begin(), curr->kvsUnsorted.end(), cmp);
		curr->kvs.insert(curr->kvs.end(),
			std::make_move_iterator(curr->kvsUnsorted.begin()),
			std::make_move_iterator(curr->kvsUnsorted.end()));
		std::inplace_merge(curr->kvs.begin(), curr->kvs.begin() + k, curr->kvs.end(), cmp);
		curr->kvsUnsorted.clear();
	}

	// pull out removed kvs
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
		if (it2 == curr->kvs.end())
			break;
		if (it != it2)
			std::swap(*it, *it2);
		if (!curr->isLeaf)
			it->value.child->parentIt = it;
		it++;
		it2++;
	}
	assert(it - curr->kvs.begin() == curr->numKvs);
	int numRemoved = (int)(curr->kvs.end() - it);
	for (int i = 0; i < numRemoved; i++)
		curr->kvs.pop_back();

	assert(curr->kvs.size() == curr->numKvs);
	assert(curr->kvsUnsorted.size() == 0);
}

void Index::invalidateDuplicate(std::vector<KeyValue>& kvs1, std::vector<KeyValue>& kvs2)
{
	std::sort(kvs1.begin(), kvs1.end(),
		[this](const KeyValue& kv1, const KeyValue& kv2) {return compareKeyValue(kv1, kv2); });

	std::sort(kvs2.begin(), kvs2.end(),
		[this](const KeyValue& kv1, const KeyValue& kv2) {return compareKeyValue(kv1, kv2); });

	if (kvs1.empty() || kvs2.empty())
		return;

	// (with the same key)
	// invalid pendingInserts and pendingRemoves -> ignore
	// count # valid pendingInserts and pendingRemoves, abs (diff #) is at most 1

	auto it1 = kvs1.begin();
	auto it2 = kvs2.begin();
	while (it1 != kvs1.end() && it2 != kvs2.end()) {
		if (it1->value.rid == -1) {
			it1++;
			continue;
		}
		if (it2->value.rid == -1) {
			it2++;
			continue;
		}
		int cmp = comparePackData(it1->key, it2->key);
		if (cmp == 0) {
			it1->value.rid = -1;
			it2->value.rid = -1;
			it1++;
			it2++;
		}
		else if (cmp < 0)
			it1++;
		else
			it2++;
	}
}

Index::Result Index::maintain(Node* curr)
{
	if (curr->kvs.size() > maxBranchingFactor || curr->kvsUnsorted.size() > maxLazySize)
		sortKvs(curr);

	// todo: what if # kvsToInsert or kvsToRemove exceeds maxLazySize after redistribute or merge?

	if (curr != root && curr->numKvs < (maxBranchingFactor + 1) / 2) {
		// regard the first kv as special and allow small numKvs
		// -- this doesn't affect much overall with large enough branching factor
		if (curr->prev == nullptr || curr->prev->parentIt->key.get() == nullptr)
			return {};

		sortKvs(curr);
		
		auto prev = curr->prev;
		if (prev->numKvs + curr->numKvs <= maxBranchingFactor) {
			// merge with prev
			// invalidate and delete curr
			int k = prev->numKvs;
			prev->next = curr->next;
			if (curr->next != nullptr)
				curr->next->prev = prev;
			prev->kvs.insert(prev->kvs.end(),
				std::make_move_iterator(curr->kvs.begin()),
				std::make_move_iterator(curr->kvs.end()));
			if (!prev->isLeaf) {
				for (auto it = prev->kvs.end() - k; it != prev->kvs.end(); it++)
					it->value.child->parentIt = it;
			}
			assert(curr->kvsUnsorted.empty());
			prev->kvsToInsert.insert(prev->kvsToInsert.end(),
				std::make_move_iterator(curr->kvsToInsert.begin()),
				std::make_move_iterator(curr->kvsToInsert.end()));
			prev->kvsToRemove.insert(prev->kvsToRemove.end(),
				std::make_move_iterator(curr->kvsToRemove.begin()),
				std::make_move_iterator(curr->kvsToRemove.end()));
			if (curr->parentIt->key.get() == nullptr)
				prev->parentIt->key.reset();
			else
				prev->parentIt->key = curr->parentIt->key;
			curr->parentIt->value.child = nullptr;
			prev->numKvs += curr->numKvs;
			delete curr;
			return Result{ .hasMerged = true };
		}
		else {
			// redistribute with prev
			sortKvs(prev);
			int k = (prev->numKvs + curr->numKvs) / 2 - curr->numKvs;
			assert(k > 0);
			prev->numKvs -= k;
			curr->numKvs += k;
			prev->parentIt->key = (prev->kvs.end() - k)->key;
			curr->kvsUnsorted.insert(curr->kvsUnsorted.end(),
				std::make_move_iterator(prev->kvs.end() - k),
				std::make_move_iterator(prev->kvs.end()));
			if (!curr->isLeaf) {
				for (auto it = curr->kvsUnsorted.end() - k; it != curr->kvsUnsorted.end(); it++)
					it->value.child->parentIt = it;
			}
			prev->kvs.erase(prev->kvs.end() - k, prev->kvs.end());
			assert(prev->kvsUnsorted.empty());
			for (auto& kv : prev->kvsToInsert) {
				if (compareKeyValue(kv, *prev->parentIt))
					continue;
				if (!curr->isLeaf) {
					curr->kvsToInsert.emplace_back(std::move(kv.key), kv.value.child);
					kv.value.child = nullptr;
				}
				else {
					curr->kvsToInsert.emplace_back(std::move(kv.key), kv.value.rid);
					kv.value.rid = -1;
				}
			}
			for (auto& kv : prev->kvsToRemove) {
				if (compareKeyValue(kv, *prev->parentIt))
					continue;
				if (!curr->isLeaf) {
					curr->kvsToRemove.emplace_back(std::move(kv.key), kv.value.child);
					kv.value.child = nullptr;
				}
				else {
					curr->kvsToRemove.emplace_back(std::move(kv.key), kv.value.rid);
					kv.value.rid = -1;
				}
			}
			return {};
		}
	}

	if (curr->numKvs > maxBranchingFactor) {
		sortKvs(curr);

		// split
		assert(curr->numKvs < maxBranchingFactor * 2);
		int k = curr->numKvs / 2;
		auto prev = new Node(curr->isLeaf, maxBranchingFactor, maxLazySize);
		prev->prev = curr->prev;
		if (curr->prev != nullptr)
			curr->prev->next = prev;
		curr->prev = prev;
		prev->next = curr;
		prev->kvs.insert(prev->kvs.end(),
			std::make_move_iterator(curr->kvs.begin()),
			std::make_move_iterator(curr->kvs.begin() + k));
		if (!prev->isLeaf) {
			for (auto it = prev->kvs.begin(); it != prev->kvs.end(); it++)
				it->value.child->parentIt = it;
		}
		if (curr->isLeaf) {
			// copy the k-th kv and pull it up
			int n = curr->numKvs;
			curr->kvs.erase(curr->kvs.begin(), curr->kvs.begin() + k);
			prev->numKvs = (int)prev->kvs.size();
			curr->numKvs = (int)curr->kvs.size();
			auto res = KeyValue(curr->kvs.front().key, prev);
			for (auto& kv : curr->kvsToInsert) {
				if (compareKeyValue(kv, res)) {
					prev->kvsToInsert.emplace_back(std::move(kv.key), kv.value.rid);
					kv.value.rid = -1;
				}
			}
			for (auto& kv : curr->kvsToRemove) {
				if (compareKeyValue(kv, res)) {
					prev->kvsToRemove.emplace_back(std::move(kv.key), kv.value.rid);
					kv.value.rid = -1;
				}
			}
			return Result{ .kvToInsert = std::move(res) };
		}
		else {
			// erase the k-th kv and pull it up
			auto key = std::move(curr->kvs[k].key);
			prev->kvs.emplace_back(curr->kvs[k].value.child);
			curr->kvs.erase(curr->kvs.begin(), curr->kvs.begin() + k + 1);
			prev->numKvs = (int)prev->kvs.size();
			curr->numKvs = (int)curr->kvs.size();
			auto res = KeyValue(std::move(key), prev);
			for (auto& kv : curr->kvsToInsert) {
				if (compareKeyValue(kv, res)) {
					prev->kvsToInsert.emplace_back(std::move(kv.key), kv.value.child);
					kv.value.child = nullptr;
				}
			}
			for (auto& kv : curr->kvsToRemove) {
				if (compareKeyValue(kv, res)) {
					prev->kvsToRemove.emplace_back(std::move(kv.key), kv.value.child);
					kv.value.child = nullptr;
				}
			}
			return Result{ .kvToInsert = std::move(res) };
		}
	}

	return {};
}

void Index::maintainRoot(Result&& res) {
	if (res.hasMerged) {
		assert(!res.kvToInsert.has_value());
		root->numKvs--;
	}
	if (res.kvToInsert.has_value()) {
		assert(!res.hasMerged);
		auto node = new Node(false, maxBranchingFactor, maxLazySize);
		node->kvs.push_back(std::move(res.kvToInsert.value()));
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
	if (curr == nullptr)
		return;
	if (!curr->isLeaf) {
		for (auto& kv : curr->kvs)
			clean(kv.value.child);
		for (auto& kv : curr->kvsUnsorted)
			clean(kv.value.child);
	}
	delete curr;
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
	os << "kvsUnsorted = ";
	dump(curr->kvsUnsorted, curr->isLeaf, os);
	os << "kvsToInsert = ";
	dump(curr->kvsToInsert, true, os);
	os << "kvsToRemove = ";
	dump(curr->kvsToRemove, true, os);
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

void Index::dump(const std::vector<KeyValue>& kvs, bool printsRID, std::ostream& os)
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
		if (printsRID)
			os << kv.value.rid;
		else
			os << kv.value.child;
		os << "),";
	}
	os << "]\n";
}

void Index::checkIntegrity()
{
	checkIntegrity(root, PackedData(), false, PackedData());
}

void Index::checkIntegrity(Node* curr, const PackedData& lb, bool existsLB, const PackedData& ub)
{
	if (!curr->isLeaf) {
		bool existsPrev = false;
		bool reachedLast = false;
		PackedData prevKey;
		std::vector<KeyValue> sorted;
		for (auto& kv : curr->kvs) {
			if (kv.value.child == nullptr)
				continue;
			if (kv.key.get() == nullptr)
				sorted.push_back(KeyValue(kv.value.child));
			else
				sorted.push_back(KeyValue(kv.key, kv.value.child));
		}
		for (auto& kv : curr->kvsUnsorted) {
			if (kv.value.child == nullptr)
				continue;
			sorted.push_back(KeyValue(kv.key, kv.value.child));
		}
		std::sort(sorted.begin(), sorted.end(), [this](const KeyValue& kv1, const KeyValue& kv2) {return compareKeyValue(kv1, kv2); });
		for (auto& kv : sorted) {
			assert(kv.key.get() == nullptr || kv.value.child == nullptr ||
				(comparePackData(kv.key, ub) < 0 && (!existsLB || comparePackData(kv.key, lb) >= 0)));
			assert(!reachedLast);
			if (kv.value.child != nullptr)
				checkIntegrity(kv.value.child, prevKey, existsPrev, kv.key);
			if (kv.key.get() == nullptr)
				reachedLast = true;
			else if (kv.value.child != nullptr) {
				if (existsPrev)
					assert(comparePackData(prevKey, kv.key) < 0);
				existsPrev = true;
				prevKey = kv.key;
			}
		}
		for (auto& kv : curr->kvsToInsert) {
			assert(kv.value.child == nullptr ||
				(comparePackData(kv.key, ub) < 0 && (!existsLB || comparePackData(kv.key, lb) >= 0)));
		}
		for (auto& kv : curr->kvsToRemove) {
			assert(kv.value.child == nullptr ||
				(comparePackData(kv.key, ub) < 0 && (!existsLB || comparePackData(kv.key, lb) >= 0)));
		}
	}
	else {
		bool existsPrev = false;
		PackedData prevKey;
		std::vector<KeyValue> sorted;
		for (auto& kv : curr->kvs) {
			if (kv.value.rid == -1)
				continue;
			sorted.push_back(KeyValue(kv.key, kv.value.child));
		}
		for (auto& kv : curr->kvsUnsorted) {
			if(kv.value.rid == -1)
				continue;
			sorted.push_back(KeyValue(kv.key, kv.value.child));
		}
		std::sort(sorted.begin(), sorted.end(), [this](const KeyValue& kv1, const KeyValue& kv2) {return compareKeyValue(kv1, kv2); });
		for (auto& kv : sorted) {
			assert(kv.key.get() != nullptr);
			assert(kv.value.rid == -1 ||
				(comparePackData(kv.key, ub) < 0 && (!existsLB || comparePackData(kv.key, lb) >= 0)));
			if (kv.value.rid != -1) {
				if (existsPrev)
					assert(comparePackData(prevKey, kv.key) < 0);
				existsPrev = true;
				prevKey = kv.key;
			}
		}
		for (auto& kv : curr->kvsToInsert) {
			assert(kv.value.rid == -1 ||
				(comparePackData(kv.key, ub) < 0 && (!existsLB || comparePackData(kv.key, lb) >= 0)));
		}
		for (auto& kv : curr->kvsToRemove) {
			assert(kv.value.rid == -1 ||
				(comparePackData(kv.key, ub) < 0 && (!existsLB || comparePackData(kv.key, lb) >= 0)));
		}
	}
}

int Index::computeBranchingFactor(const std::vector<DataType>& types, int size)
{
	int keySize = PackedData::computeSize(types);
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

