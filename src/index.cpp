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
	maxLazySize((int)sqrt(maxBranchingFactor)), // (13.3) - Then, non-static data members are initialized in the order they were declared in the class definition (again regardless of the order of the mem-initializers).
	root(new Node(true, maxBranchingFactor, maxLazySize))
{
}

Index::Index(Index&& other) noexcept :
	types(other.types), names(other.names), allowsDuplicate(other.allowsDuplicate),
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
	assert(rid != INVALID_RID);

	auto internalKey = makeInternalKey(key, rid);

	if(checksIntegrity && !allowsDuplicate && !select(internalKey).empty())
		return false;

	std::vector<KeyValue> temp;
	temp.push_back(KeyValue(std::move(internalKey), rid));
	auto res = insert(root, std::move(temp));
	maintainRoot(std::move(res));

	return true;
}

bool Index::insert(const std::vector<PackedData>& keys, const std::vector<Int64>& rids, bool checksIntegtrity) {
	// todo: inserts multiple keys at once
	// Result must be changed so that it can contain multiple kvs, and maintainRoot() must be changed as well

	assert(false);
	return true;
}

Index::Result Index::insert(Node* curr, std::vector<KeyValue>&& tempKvs)
{
	if (tempKvs.empty())
		return {};

	curr->kvsToInsert.insert(curr->kvsToInsert.end(),
		std::make_move_iterator(tempKvs.begin()),
		std::make_move_iterator(tempKvs.end()));

	return maintain(curr);
}

bool Index::remove(const PackedData& key, Int64 rid, bool checksIntegrity)
{
	assert(rid != INVALID_RID);

	auto internalKey = makeInternalKey(key, rid);

	if (checksIntegrity && !select(key, rid))
		return false;

	std::vector<KeyValue> temp;
	temp.push_back(KeyValue(std::move(internalKey), rid));
	auto res = remove(root, std::move(temp));
	maintainRoot(std::move(res));

	return true;
}

bool Index::remove(const std::vector<PackedData>& keys, const std::vector<Int64>& rids, bool checksIntegtrity) {
	// todo: remove multiple keys at once
	assert(false);
	return true;
}

Index::Result Index::remove(Node* curr, std::vector<KeyValue>&& tempKvs)
{
	if (tempKvs.empty())
		return {};

	curr->kvsToRemove.insert(curr->kvsToRemove.end(),
		std::make_move_iterator(tempKvs.begin()),
		std::make_move_iterator(tempKvs.end()));

	return maintain(curr);
}

bool Index::select(const PackedData& key, Int64 rid)
{
	auto res = select(makeInternalKey(key, rid), makeInternalKey(key, rid));
	assert(res.size() <= 1);
	return !res.empty();
}

std::vector<int> Index::select(const PackedData& key)
{
	return select(makeInternalKey(key, MIN_RID), makeInternalKey(key, MAX_RID));
}

std::vector<int> Index::selectRange(const PackedData& loKey, const PackedData& hiKey)
{
	return select(makeInternalKey(loKey, MIN_RID), makeInternalKey(hiKey, MAX_RID));
}

std::vector<int> Index::select(const PackedData& loKey, const PackedData& hiKey) {
	// for a key, consider the balance:
	// 1) +1 for keys in kvs and kvsUnsorted of a leaf node
	// 2) +1 for keys in kvsToInsert of any node
	// 3) -1 for keys in kvsToRemove of any node
	// -- the balance must be 1 or 0

	std::vector<int> plus, minus;
	select(root, loKey, hiKey, plus, minus);

	std::sort(plus.begin(), plus.end());
	std::sort(minus.begin(), minus.end());
	auto itp = plus.begin();
	auto itm = minus.begin();

	std::vector<int> res;
	while (itp != plus.end()) {
		if (itm == minus.end() || *itp < *itm) {
			res.push_back(*itp);
			itp++;
		}
		else if (*itp == *itm) {
			itp++;
			itm++;
		}
		else {
			assert(false);
		}
	}
	assert(itm == minus.end());
	return res;
}

void Index::select(Node* curr, const PackedData& loKey, const PackedData& hiKey, std::vector<int>& plus, std::vector<int>& minus)
{
	// todo: make some statistics for select to enhance the searching time
	// e.g., if # invalid call exceeds a certain number
	// just sort the whole kvs so as to remove unsorted kvs and invalid kvs

	if (curr->isLeaf) {
		auto from = lowerBound(curr, loKey);
		auto to = upperBound(curr, hiKey, from - curr->kvs.begin());
		for (auto it = from; it != to; it++) {
			if (isInvalid(*it))
				continue;
			plus.push_back(it->value.rid);
		}
		for (auto& kv : curr->kvsUnsorted) {
			if (isInvalid(kv))
				continue;
			if (comparePackData(kv.key, loKey) >= 0 && comparePackData(kv.key, hiKey) <= 0)
				plus.push_back(kv.value.rid);
		}
	}
	else {
		auto from = upperBound(curr, loKey);
		assert(from != curr->kvs.end());
		auto to = upperBound(curr, hiKey, from - curr->kvs.begin());
		assert(to != curr->kvs.end());
		to++;
		for (auto it = from; it != to; it++) {
			if (isInvalid(*it))
				continue;
			select(it->value.child, loKey, hiKey, plus, minus);
		}
		to--;
		for (auto& kv : curr->kvsUnsorted) {
			if (isInvalid(kv))
				continue;
			// hiKey < kvs[to] < kv.key -> kv's interval is larger than kvs[to]
			if (comparePackData(kv.key, to->key) > 0)
				continue;
			if (comparePackData(kv.key, loKey) > 0)
				select(kv.value.child, loKey, hiKey, plus, minus);
		}
	}

	for (auto& kv : curr->kvsToInsert) {
		if (isInvalid(kv))
			continue;
		if (comparePackData(kv.key, loKey) >= 0 && comparePackData(kv.key, hiKey) <= 0)
			plus.push_back(kv.value.rid);
	}
	for (auto& kv : curr->kvsToRemove) {
		if (isInvalid(kv))
			continue;
		if (comparePackData(kv.key, loKey) >= 0 && comparePackData(kv.key, hiKey) <= 0)
			minus.push_back(kv.value.rid);
	}
}

void Index::sortKvs(Node* curr)
{
	if (curr->kvsUnsorted.empty() && curr->kvs.size() == curr->numKvs)
		return;

	auto cmp = std::bind(&Index::compareKeyValue, this, std::placeholders::_1, std::placeholders::_2);
	std::sort(curr->kvs.begin(), curr->kvs.end(), cmp);

	// merge kvsUnsorted into kvs
	if (!curr->kvsUnsorted.empty()) {
		int k = (int)curr->kvs.size();
		std::sort(curr->kvsUnsorted.begin(), curr->kvsUnsorted.end(), cmp);
		curr->kvs.insert(curr->kvs.end(),
			std::make_move_iterator(curr->kvsUnsorted.begin()),
			std::make_move_iterator(curr->kvsUnsorted.end()));
		std::inplace_merge(curr->kvs.begin(), curr->kvs.begin() + k, curr->kvs.end(), cmp);
		curr->kvsUnsorted.clear();
	}

	// pull out invalid kvs
	while (!curr->kvs.empty() && isInvalid(curr->kvs.back())){
		curr->kvs.pop_back();
	}

	if (!curr->isLeaf) {
		for (auto it = curr->kvs.begin(); it != curr->kvs.end(); it++) {
			assert(!isInvalid(*it));
			it->value.child->parentIt = it;
		}
	}

	assert(curr->kvs.size() == curr->numKvs);
	assert(curr->kvsUnsorted.size() == 0);
}

void Index::removeDuplicate(std::vector<KeyValue>& kvs1, std::vector<KeyValue>& kvs2)
{
	invalidateDuplicate(kvs1, kvs2);

	auto removeInvalidated = [](std::vector<KeyValue>& kvs) {
		auto it = kvs.begin();
		auto it2 = kvs.begin();
		while (it2 != kvs.end()) {
			while (it2 != kvs.end() && isInvalid(*it2))
				it2++;
			if (it2 == kvs.end())
				break;
			if (it != it2)
				std::swap(*it, *it2);
			it++;
			it2++;
		}
	};
	while (!kvs1.empty() && isInvalid(kvs1.back()))
		kvs1.pop_back();
	while (!kvs2.empty() && isInvalid(kvs2.back()))
		kvs2.pop_back();
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
		if (isInvalid(*it1)){
			it1++;
			continue;
		}
		if (isInvalid(*it2)) {
			it2++;
			continue;
		}
		int cmp = comparePackData(it1->key, it2->key);
		if (cmp == 0) {
			it1->value.rid = INVALID_RID;
			it2->value.rid = INVALID_RID;
			it1++;
			it2++;
		}
		else if (cmp < 0)
			it1++;
		else
			it2++;
	}
}

void Index::pushInsert(Node* curr)
{
	push(curr, true);
}

void Index::pushRemove(Node* curr)
{
	push(curr, false);
}
void Index::push(Node* curr, bool forInsert)
{
	// pushdown kvsToInsert / kvsToRemove in the correct children
	sortKvs(curr);
	std::vector<KeyValue>& kvsToPush = forInsert ? curr->kvsToInsert : curr->kvsToRemove;
	auto itToPush = kvsToPush.begin();
	auto it = curr->kvs.begin();

	std::vector<KeyValue> pulledUp;

	while (it != curr->kvs.end()) {
		if (isInvalid(*it)) {
			it++;
			continue;
		}

		std::vector<KeyValue> pd;
		while (itToPush != kvsToPush.end()) {
			if (isInvalid(*itToPush)) {
				itToPush++;
				continue;
			}
			auto res = compareKeyValue(*itToPush, *it);
			if (!res)
				break;
			// *itToInsert < *it
			pd.push_back(std::move(*itToPush));
			itToPush++;
		}
		auto res = forInsert ?
			insert(it->value.child, std::move(pd)) :
			remove(it->value.child, std::move(pd));
		if (res.countMerged) {
			if (res.countMerged > 1) {
				res.countMerged = res.countMerged;
			}
			curr->numKvs -= res.countMerged;
		}
		if (res.kvToInsert.has_value())
			pulledUp.push_back(std::move(res.kvToInsert.value()));
		it++;
	}
	assert(itToPush == kvsToPush.end()); // since the last key is always null, greater than anything else
	kvsToPush.clear();

	curr->numKvs += (int)pulledUp.size();
	curr->kvsUnsorted.insert(curr->kvsUnsorted.end(),
		std::make_move_iterator(pulledUp.begin()),
		std::make_move_iterator(pulledUp.end()));
	for (auto it = curr->kvsUnsorted.end() - pulledUp.size(); it != curr->kvsUnsorted.end(); it++)
		it->value.child->parentIt = it;
}

Index::Result Index::maintain(Node* curr)
{
	// todo: check if the below is true
	// these conditions do not hold anymore
	//assert(curr == root || curr->prev == nullptr || curr->prev->parentIt->key.get() == nullptr
	//	|| curr->numKvs >= (maxBranchingFactor + 1) / 2);
	//assert(curr->numKvs <= maxBranchingFactor);

	if (curr->kvs.size() > maxBranchingFactor || curr->kvsUnsorted.size() > maxLazySize)
		sortKvs(curr);

	if (curr->kvsToInsert.size() <= maxLazySize && curr->kvsToRemove.size() <= maxLazySize)
		return {};

	removeDuplicate(curr->kvsToInsert, curr->kvsToRemove);

	if (!curr->isLeaf) {
		if (curr->kvsToInsert.size() > maxLazySize)
			pushInsert(curr);
		if (curr->kvsToRemove.size() > maxLazySize)
			pushRemove(curr);
	}
	else {
		// finally stop pushing down at a leaf node
	
		// handle kvsToInsert
		curr->numKvs += (int)curr->kvsToInsert.size();
		for (auto& kv : curr->kvsToInsert)
			if(isInvalid(kv))
				curr->numKvs--;
		if(curr->kvsUnsorted.empty())
			std::swap(curr->kvsUnsorted, curr->kvsToInsert);
		else {
			curr->kvsUnsorted.insert(curr->kvsUnsorted.end(),
				std::make_move_iterator(curr->kvsToInsert.begin()),
				std::make_move_iterator(curr->kvsToInsert.end()));
			curr->kvsToInsert.clear();
		}
		sortKvs(curr);

		// handle kvsToRemove
		curr->numKvs -= (int)curr->kvsToRemove.size();
		for (auto& kv : curr->kvsToRemove)
			if(isInvalid(kv))
				curr->numKvs++;
		invalidateDuplicate(curr->kvs, curr->kvsToRemove);
		for (auto& kv : curr->kvsToRemove)
			assert(isInvalid(kv));
		sortKvs(curr);
		curr->kvsToRemove.clear();
	}

	if (curr->kvs.size() > maxBranchingFactor || curr->kvsUnsorted.size() > maxLazySize)
		sortKvs(curr);

	if (curr != root && curr->numKvs < (maxBranchingFactor + 1) / 2) {
		// regard the first kv as special and allow small numKvs
		// -- this doesn't affect much overall with large enough branching factor
		if (curr->prev == nullptr || curr->prev->parentIt->key.get() == nullptr) {
			if (curr->numKvs > 0)
				return {};

			// if curr is a leaf, pending insertions and removals were already handled above
			assert(!curr->isLeaf || (curr->kvsToInsert.empty() && curr->kvsToRemove.empty()));

			auto prev = curr->prev;
			auto next = curr->next;
			if (prev != nullptr)
				prev->next = next;
			if (next != nullptr) {
				next->prev = prev;
				if (curr->parentIt->key.get() == nullptr) {
					// curr is the only child of its parent
					assert(curr->kvsToInsert.empty() && curr->kvsToRemove.empty());
				}
				else {
					next->kvsToInsert.insert(next->kvsToInsert.end(),
						std::make_move_iterator(curr->kvsToInsert.begin()),
						std::make_move_iterator(curr->kvsToInsert.end()));
					next->kvsToRemove.insert(next->kvsToRemove.end(),
						std::make_move_iterator(curr->kvsToRemove.begin()),
						std::make_move_iterator(curr->kvsToRemove.end()));
				}
			}
			curr->parentIt->value.child = INVALID_NODE;
			delete curr;
			return Result{ .countMerged = 1 };
		}

		auto prev = curr->prev;
		sortKvs(curr);
		sortKvs(prev);

		if (prev->numKvs + curr->numKvs <= maxBranchingFactor) {
			// merge with prev
			// invalidate and delete curr
			int k = curr->numKvs;
			prev->next = curr->next;
			if (curr->next != nullptr)
				curr->next->prev = prev;
			if (!prev->isLeaf) {
				assert(!isInvalid(*prev->parentIt));
				prev->kvs.back().key = prev->parentIt->key;
			}
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
			curr->parentIt->value.child = INVALID_NODE;
			prev->numKvs += k;
			delete curr;

			auto res = maintain(prev);
			if (res.countMerged) {
				res.countMerged = res.countMerged;
			}
			res.countMerged++;
			return res;
		}
		else {
			// redistribute with prev
			int k = (prev->numKvs + curr->numKvs) / 2 - curr->numKvs;
			assert(k > 0);
			prev->numKvs -= k;
			curr->numKvs += k;
			if(!prev->isLeaf)
				prev->kvs.back().key = prev->parentIt->key;
			prev->parentIt->key = prev->isLeaf?
				(prev->kvs.end() - k)->key :
				findSmallestKey((prev->kvs.end() - k)->value.child);
			curr->kvsUnsorted.insert(curr->kvsUnsorted.end(),
				std::make_move_iterator(prev->kvs.end() - k),
				std::make_move_iterator(prev->kvs.end()));
			if (!curr->isLeaf) {
				for (auto it = curr->kvsUnsorted.end() - k; it != curr->kvsUnsorted.end(); it++)
					it->value.child->parentIt = it;
			}
			prev->kvs.erase(prev->kvs.end() - k, prev->kvs.end());
			if(!prev->isLeaf)
				prev->kvs.back().key.reset();
			assert(prev->kvsUnsorted.empty());
			for (auto& kv : prev->kvsToInsert) {
				if (compareKeyValue(kv, *prev->parentIt))
					continue;
				if (!curr->isLeaf) {
					curr->kvsToInsert.emplace_back(std::move(kv.key), kv.value.child);
					kv.value.child = INVALID_NODE;
				}
				else {
					curr->kvsToInsert.emplace_back(std::move(kv.key), kv.value.rid);
					kv.value.rid = INVALID_RID;
				}
			}
			for (auto& kv : prev->kvsToRemove) {
				if (compareKeyValue(kv, *prev->parentIt))
					continue;
				if (!curr->isLeaf) {
					curr->kvsToRemove.emplace_back(std::move(kv.key), kv.value.child);
					kv.value.child = INVALID_NODE;
				}
				else {
					curr->kvsToRemove.emplace_back(std::move(kv.key), kv.value.rid);
					kv.value.rid = INVALID_RID;
				}
			}
			return {};
		}
	}

	if (curr->numKvs > maxBranchingFactor) {
		sortKvs(curr);
		
		// TODO: change here to support mass-insertion
		// m = ceil(numKvs / maxBranchingFactor)
		// split numKvs / m elements from curr per loop

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
		if (!curr->isLeaf) {
			// erase the k-th kv and pull it up
			auto key = std::move(curr->kvs[k].key);
			prev->kvs.emplace_back(curr->kvs[k].value.child);
			curr->kvs.erase(curr->kvs.begin(), curr->kvs.begin() + k + 1);
			for (auto it = prev->kvs.begin(); it != prev->kvs.end(); it++)
				it->value.child->parentIt = it;
			for (auto it = curr->kvs.begin(); it != curr->kvs.end(); it++)
				it->value.child->parentIt = it;
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
		else {
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
	}

	return {};
}

void Index::maintainRoot(Result&& res) {
	if (res.countMerged) {
		if (res.countMerged > 1) {
			res.countMerged = res.countMerged;
		}
		root->numKvs -= res.countMerged;
	}
	if (res.kvToInsert.has_value()) {
		auto node = new Node(false, maxBranchingFactor, maxLazySize);
		node->kvs.push_back(std::move(res.kvToInsert.value()));
		node->kvs.emplace_back(root);
		node->kvs.front().value.child->parentIt = node->kvs.begin();
		node->kvs.back().value.child->parentIt = node->kvs.begin() + 1;
		node->numKvs = 2;
		root = node;
	}
	while(root->numKvs == 1 && !root->isLeaf) {
		removeDuplicate(root->kvsToInsert, root->kvsToRemove);
		pushInsert(root);
		pushRemove(root);
		sortKvs(root);
		if (root->numKvs == 1) {
			auto node = root->kvs.front().value.child;
			delete root;
			root = node;
		}
	}
}

PackedData Index::findSmallestKey(Node* curr) {
	assert(curr != nullptr);
	assert(curr->numKvs > 0);
	PackedData* smallestKey = nullptr;
	Node* leftmost = nullptr;
	for (auto& kv : curr->kvs) {
		if (isInvalid(kv))
			continue;
		smallestKey = &kv.key;
		leftmost = kv.value.child;
		break;
	}
	for (auto& kv : curr->kvsUnsorted) {
		if (isInvalid(kv))
			continue;
		if (smallestKey != nullptr && comparePackData(kv.key, *smallestKey) < 0) {
			smallestKey = &kv.key;
			leftmost = kv.value.child;
		}
	}
	for (auto& kv : curr->kvsToInsert) {
		if (isInvalid(kv))
			continue;
		if (comparePackData(kv.key, *smallestKey) < 0)
			smallestKey = &kv.key;
	}
	for (auto& kv : curr->kvsToRemove) {
		if (isInvalid(kv))
			continue;
		if (comparePackData(kv.key, *smallestKey) < 0)
			smallestKey = &kv.key;
	}
	assert(smallestKey != nullptr);
	if (curr->isLeaf)
		return PackedData(*smallestKey);
	auto res = findSmallestKey(leftmost);
	if (comparePackData(*smallestKey, res) < 0)
		return PackedData(*smallestKey);
	else
		return res;
}

std::vector<Index::KeyValue>::iterator Index::lowerBound(Node* curr, const PackedData& key, int hintPos)
{
	int lo = hintPos;
	int hi = curr->kvs.size();
	for (int i = lo; i < hi; i++) {
		if (isInvalid(curr->kvs[i]))
			continue;
		if (comparePackData(curr->kvs[i].key, key) >= 0)
			return curr->kvs.begin() + i;
		else {
			lo = i;
			break;
		}
	}

	// kvs[lo] < key <= kvs[hi]
	while (lo < hi - 1) {
		int i = (lo + hi) / 2;
		int j = i;
		while (j >= lo && isInvalid(curr->kvs[j]))
			j--;
		if (j <= lo) {
			lo = i;
			continue;
		}
		if (comparePackData(curr->kvs[j].key, key) >= 0)
			hi = j;
		else
			lo = j;
	}
	return curr->kvs.begin() + hi;
}

std::vector<Index::KeyValue>::iterator Index::upperBound(Node* curr, const PackedData& key, int hintPos)
{
	int lo = hintPos;
	int hi = curr->kvs.size();
	for (int i = lo; i < hi; i++) {
		if (isInvalid(curr->kvs[i]))
			continue;
		if (comparePackData(curr->kvs[i].key, key) > 0)
			return curr->kvs.begin() + i;
		else {
			lo = i;
			break;
		}
	}

	// kvs[lo] <= key < kvs[hi]
	while (lo < hi - 1) {
		int i = (lo + hi) / 2;
		int j = i;
		while (j >= lo && isInvalid(curr->kvs[j]))
			j--;
		if (j <= lo) {
			lo = i;
			continue;
		}
		if (comparePackData(curr->kvs[j].key, key) > 0)
			hi = j;
		else
			lo = j;
	}
	return curr->kvs.begin() + hi;
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
	bool existsPrev = false;
	bool reachedLast = false;
	PackedData prevKey;
	std::vector<KeyValue> sorted;
	for (auto& kv : curr->kvs) {
		if(isInvalid(kv))
			continue;
		if (kv.key.get() == nullptr) {
			assert(!reachedLast);
			reachedLast = true;
			sorted.push_back(KeyValue(kv.value.child));
		}
		else
			sorted.push_back(KeyValue(kv.key, kv.value.child));
	}
	reachedLast = false;
	for (auto& kv : curr->kvsUnsorted) {
		if (isInvalid(kv))
			continue;
		sorted.push_back(KeyValue(kv.key, kv.value.child));
	}
	std::sort(sorted.begin(), sorted.end(), [this](const KeyValue& kv1, const KeyValue& kv2) {return compareKeyValue(kv1, kv2); });
	for (auto& kv : sorted) {
		assert(kv.key.get() == nullptr || isInvalid(kv) ||
			(comparePackData(kv.key, ub) < 0 && (!existsLB || comparePackData(kv.key, lb) >= 0)));
		assert(!reachedLast);
		if(!curr->isLeaf && kv.value.child != nullptr)
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

PackedData Index::makeInternalKey(const PackedData& key, Int64 rid)
{
	return allowsDuplicate ? PackedData::combine(key, rid) : key;
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
	if (isInvalid(kv1))
		return false;
	if (isInvalid(kv2))
		return true;
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

bool Index::isInvalid(const KeyValue& kv)
{
	assert((kv.value.child == INVALID_NODE) == (kv.value.rid == INVALID_RID));
	return kv.value.child == INVALID_NODE;
}