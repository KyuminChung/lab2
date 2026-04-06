#include "skiplist.h"

#include <algorithm>
#include <limits>
#include <random>

namespace {
// head 노드는 실제 데이터보다 항상 앞에 와야 함
// key는 최소값, seq는 최대값으로 설정
// 같은 key 비교 시 seq가 큰 값이 먼저 오기 때문에,
// head의 seq를 크게 잡으면 head가 항상 맨 앞에 유지됨
constexpr int kHeadKey = std::numeric_limits<int>::min();
constexpr int64_t kHeadSeq = std::numeric_limits<int64_t>::max();

// 검색 시 "해당 key의 가장 최신 버전"을 찾고 싶을 때 사용하는 seq.
// 같은 key에서 seq가 큰 값이 앞에 있으므로,
// 큰 seq를 기준으로 찾으면 그 key의 첫 번째 노드가 최신 버전이 된다.
constexpr int64_t kSeekNewestSeq = std::numeric_limits<int64_t>::max();
}

bool SkipList::Less(int a_key, int64_t a_seq, int b_key, int64_t b_seq) {
  // 기본 정렬은 key 오름차순
  if (a_key != b_key) return a_key < b_key;

  // 같은 key 안에서는 seq가 큰 값이 "더 앞"에 와야 최신 버전을 빠르게 찾을 수 있음
  // 내림차순으로 정렬
  return a_seq > b_seq;
}

// SkipList Constructor
// skip list는 여러 레벨의 연결 리스트로 구성
SkipList::SkipList(int max_level, float p)
    : head_(nullptr), max_level_(std::max(1, max_level)), p_(p), next_seq_(1) {
  // 각 레벨마다 sentinel head를 만들고 down 포인터로 연결
  // 마지막에 head_는 최상단 head를 가리킴
  Node* down = nullptr;

  for (int i = 0; i < max_level_; ++i) {
    Node* head = new Node{kHeadKey, kHeadSeq, "", false, nullptr, down};
    down = head;
  }
  head_ = down;
}
// SkipList Destructor
// 각 레벨의 연결 리스트를 전부 순회하며 메모리를 해제한다.
SkipList::~SkipList() {
  // 위 레벨과 아래 레벨은 각각 별도의 Node를 사용
  // 레벨별로 next를 따라가며 삭제
  Node* level = head_;
  while (level != nullptr) {
    Node* next_level = level->down;

    Node* cur = level;
    while (cur != nullptr) {
      Node* next = cur->next;
      delete cur;
      cur = next;
    }
    level = next_level;
  }
}

// Put operation시 높이 설정 함수
int SkipList::RandomLevel() {
  // p가 비정상 값일 때
  if (p_ <= 0.0f) return 1;
  if (p_ >= 1.0f) return max_level_;

  // 각 단계마다 확률 p로 한 층 더 올라감
  static thread_local std::mt19937 gen(std::random_device{}());
  std::bernoulli_distribution coin(p_);

  int level = 1;
  while (level < max_level_ && coin(gen)) {
    ++level;
  }
  return level;
}

SkipList::Node* SkipList::FindGreaterOrEqual(int key, int64_t seq,
                                             std::vector<Node*>* update) const {
  Node* cur = head_;
  Node* bottom_prev = nullptr;
  std::vector<Node*> path_top_down;

  // 위 레벨부터 시작해서 오른쪽으로 최대한 이동한 뒤,
  // 더 이상 갈 수 없으면 아래 레벨로 내려감.
  while (cur != nullptr) {
    while (cur->next != nullptr &&
           Less(cur->next->key, cur->next->seq, key, seq)) {
      cur = cur->next;
    }

    // 삽입 시에는 각 레벨에서 "삽입 직전 노드"가 필요, 저장.
    if (update != nullptr) {
      path_top_down.push_back(cur);
    }

    bottom_prev = cur;
    cur = cur->down;
  }

  // path_top_down은 위에서 아래 순서로 쌓였으므로,
  // 실제 삽입은 bottom-up으로 하기 위해 뒤집어서 넘김.
  if (update != nullptr) {
    update->assign(path_top_down.rbegin(), path_top_down.rend());
  }

  // bottom_prev->next가 (key, seq) 이상인 첫 실제 노드
  return (bottom_prev == nullptr) ? nullptr : bottom_prev->next;
}

void SkipList::InsertInternal(int key, const std::string& value, bool tombstone) {
  // 새 버전 삽입이므로 sequence number를 새로 부여.
  // 같은 key라도 seq가 다르면 별개의 버전으로 저장.
  const int64_t seq = next_seq_++;

  // 각 레벨에서 새 노드가 연결될 predecessor를 저장.
  std::vector<Node*> update;
  update.reserve(max_level_);
  FindGreaterOrEqual(key, seq, &update);

  // 새 엔트리가 올라갈 높이를 확률적으로 정함.
  int level = RandomLevel();
  Node* down = nullptr;

  // 가장 아래 레벨부터 위쪽으로 차례로 노드를 만듬.
  // 각 레벨의 같은 엔트리는 down 포인터로 연결됨.
  for (int i = 0; i < level; ++i) {
    Node* prev = update[i];
    Node* node = new Node{key, seq, value, tombstone, prev->next, down};
    prev->next = node;
    down = node;
  }
}

// SkipList에 새로운 Key-value 삽입, sequence number 필요
void SkipList::Put(int key, const std::string& value) {
  // Put은 기존 값을 덮어쓰는 것이 아님.
  // 더 큰 seq를 가진 새 버전을 삽입하는 out-of-place update.
  InsertInternal(key, value, false);
}

bool SkipList::GetLatestEntry(int key, RangeEntry* out_entry) const {
  // (key, 큰 seq)를 기준으로 찾으면
  // 해당 key의 가장 앞 노드, 즉 최신 버전을 얻을 수 있다.
  Node* node = FindGreaterOrEqual(key, kSeekNewestSeq, nullptr);

  // 찾은 노드가 없거나 key가 다르면 현재 skip list에는 해당 key가 없다.
  if (node == nullptr || node->key != key) {
    return false;
  }

  // value뿐 아니라 tombstone 여부까지 함께 반환.
  if (out_entry != nullptr) {
    out_entry->key = node->key;
    out_entry->value = node->value;
    out_entry->tombstone = node->tombstone;
  }
  return true;
}

// SkipList에서 key에 해당하는 value 찾기
// 있으면 true, 없으면 tombstone고려, false, value는 out_value에 저장
bool SkipList::Get(int key, std::string* out_value) const {
  // 최신 엔트리를 먼저 찾고,
  // 그 최신 엔트리가 tombstone이면 삭제된 것으로 간주, not found.
  RangeEntry entry;
  if (!GetLatestEntry(key, &entry)) {
    return false;
  }

  if (entry.tombstone) {
    return false;
  }

  if (out_value != nullptr) {
    *out_value = entry.value;
  }
  return true;
}

// SkipList Delete operation.
bool SkipList::Delete(int key) {
  // Delete는 실제 노드 제거가 아니라 tombstone 삽입.
  // 다만 반환값을 위해 "삭제 직전 최신 상태가 보이는 값이었는지" 먼저 확인.
  RangeEntry latest;
  bool existed_and_visible = false;

  if (GetLatestEntry(key, &latest) && !latest.tombstone) {
    existed_and_visible = true;
  }

  // 새 tombstone 버전을 삽입하면 이후 Get에서는 이 tombstone이 최신 버전이 됨.
  InsertInternal(key, "", true);
  return existed_and_visible;
}

std::vector<SkipList::RangeEntry> SkipList::RangeScanEntries(int start_key,
                                                             int end_key) const {
  std::vector<RangeEntry> out;
  if (start_key > end_key) return out;

  // 시작 key 이상인 첫 노드부터 순회.
  Node* cur = FindGreaterOrEqual(start_key, kSeekNewestSeq, nullptr);

  while (cur != nullptr && cur->key <= end_key) {
    int current_key = cur->key;

    // 같은 key는 seq 내림차순으로 정렬되어 있으므로
    // 지금 보고 있는 첫 노드가 이 key의 최신 버전.
    // tombstone도 그대로 담아서 상위(memdb) 병합 시 삭제 여부를 보존.
    out.push_back(RangeEntry{cur->key, cur->value, cur->tombstone});

    // 같은 key의 더 오래된 버전들은 건너뜀.
    while (cur != nullptr && cur->key == current_key) {
      cur = cur->next;
    }
  }

  return out;
}

// SkipList range scan operation, 해당 노드 vector에 모아 반환
std::vector<std::pair<int, std::string>>
SkipList::RangeScan(int start_key, int end_key) const {
  std::vector<std::pair<int, std::string>> out;
  auto entries = RangeScanEntries(start_key, end_key);

  // 사용자에게 반환할 RangeScan 결과에서는 tombstone을 제외.
  // 즉, 삭제된 key는 결과에서 보이지 않아야 함.
  for (const auto& entry : entries) {
    if (!entry.tombstone) {
      out.emplace_back(entry.key, entry.value);
    }
  }

  return out;
}