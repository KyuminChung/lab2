#include "memdb.h"

#include <map>
#include <unordered_set>
#include <utility>

// SkipList를 사용하여 Out-of-place update를 진행하는 InMemoryDB
// 핵심 동작
// 1) Put/Delete는 항상 현재 mutable memtable에 기록
// 2) mutable memtable이 가득 차면 immutable로 전환
// 3) Get/RangeScan은 모든 memtable을 최신순으로 검색
// 4) Delete는 실제 제거가 아니라 tombstone 삽입

InMemoryDB::MemTable::MemTable(const MemDBOptions& options)
//MemTable은 하나의 SkipList와 현재 사용 중인 크기 정보를 가짐
//size_bytes는 mutable memtable이 가득 찼는지 판단할 때 사용
    : list(options.skiplist_max_height, options.skiplist_p),
      size_bytes(0),
      immutable(false) {}

// DB 생성 시에는 비어 있는 mutable memtable 하나만 존재
InMemoryDB::InMemoryDB(const MemDBOptions& options)
    : options_(options), mutable_(std::make_unique<MemTable>(options_)) {}

void InMemoryDB::Put(int key, const std::string& value) {
  // 새 엔트리 들어가기 전
  // 현재 mutable memtable에 용량이 충분한지 먼저 판단
  size_t entry_bytes = EntryBytes(key, value);
  EnsureMutableCapacity(entry_bytes);

  //실제 삽입, mutable memtable의 skip list에 수행
  //새 seq가 부여되므로 같은 key의 새 버전이 생성됨
  mutable_->list.Put(key, value);
  mutable_->size_bytes += entry_bytes;
}

bool InMemoryDB::Get(int key, std::string* out_value) const {
  SkipList::RangeEntry entry;

  // 가장 최근 memtable부터 조회
  // mutable memtable 검사
  if (mutable_ != nullptr && mutable_->list.GetLatestEntry(key, &entry)) {
    // tombstone이면 not found
    if (entry.tombstone) return false;
    if (out_value != nullptr) *out_value = entry.value;
    return true;
  }

  // reverse iterator 사용, 최신 immutable 부터 검사
  for (auto it = immutables_.rbegin(); it != immutables_.rend(); ++it) {
    if ((*it)->list.GetLatestEntry(key, &entry)) {
      // 해당 memtable에서 찾은 최신 entry가 tombstone이면 삭제 상태
      if (entry.tombstone) return false;
      if (out_value != nullptr) *out_value = entry.value;
      return true;
    }
  }
  // 모든 memtable에서 key를 찾지 못함
  return false;
}

void InMemoryDB::Delete(int key) {
  // tombstone entry하나 삽입
  // 용량 미리 계산
  size_t entry_bytes = EntryBytes(key, "");
  EnsureMutableCapacity(entry_bytes);

  // tombstone 버전을 mutable memtable에 삽입
  mutable_->list.Delete(key);
  mutable_->size_bytes += entry_bytes;
}

std::vector<std::pair<int, std::string>>
InMemoryDB::RangeScan(int start_key, int end_key) const {
  std::vector<std::pair<int, std::string>> out;
  if (start_key > end_key) return out;

  // visible: 사용자에게 보여줄 key-value 결과 보관
  // map 사용해 key 기준 오름차순 정렬 상태
  std::map<int, std::string> visible;

  // decided: 어떤 key가 이미 더 최신 memtable에서 결정되었는지 기록
  std::unordered_set<int> decided;

  // memtable에서 범위 내 최신 엔트리들 수집
  auto collect = [&](const MemTable* table) {
    auto entries = table->list.RangeScanEntries(start_key, end_key);

    for (const auto& entry : entries) {
      // 더 최신 memtable에서 이미 같은 키가 결정됨 -> 무시
      if (!decided.insert(entry.key).second) {
        continue;
      }
      // 최신 엔트리가 tombstone이 아님 -> visible
      if (!entry.tombstone) {
        visible[entry.key] = entry.value;
      }
    }
  };

  // 최신 memtable 검사, mutable 부터
  if (mutable_ != nullptr) {
    collect(mutable_.get());
  }
  // 그 후 최신 immutable부터 
  for (auto it = immutables_.rbegin(); it != immutables_.rend(); ++it) {
    collect(it->get());
  }
  // map에 저장된 최종 결과 
  for (const auto& kv : visible) {
    out.push_back(kv);
  }

  return out;
}

void InMemoryDB::EnsureMutableCapacity(size_t entry_bytes) {
  // mutable_가 없으면 새로 생성
  if (mutable_ == nullptr) {
    mutable_ = std::make_unique<MemTable>(options_);
    return;
  }

  // 비어 있는 mutable memtable은 우선 현재 엔트리를 수용하게 둠
  if (mutable_->size_bytes == 0) {
    return;
  }

  // 현재 엔트리를 넣으면 최대 용량을 초과하는 경우, 기존 mutable -> immutable
  // 새 mutable 만듬
  if (mutable_->size_bytes + entry_bytes > options_.max_memtable_bytes) {
    mutable_->immutable = true;
    immutables_.push_back(std::move(mutable_));
    mutable_ = std::make_unique<MemTable>(options_);
  }
}

// 필요시 사용
// 현재 immutable memtable 개수를 반환
size_t InMemoryDB::ImmutableCount() const { return immutables_.size(); }
// 현재 mutable memtable의 누적 사용 바이트 수를 반환
size_t InMemoryDB::MutableSizeBytes() const { return mutable_->size_bytes; }
// memtable 크기 계산용 단순 추정치
size_t InMemoryDB::EntryBytes(int key, const std::string& value) const {
  // key 크기 + value 길이를 엔트리 저장 비용으로 본다.
  // Put: value를 포함한 엔트리 크기
  // Delete: tombstone이므로 value는 빈 문자열
  return sizeof(key) + value.size();
}