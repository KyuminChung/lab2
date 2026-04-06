#ifndef LAB2_SKIPLIST_H
#define LAB2_SKIPLIST_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

// RangeScanEntries()에서 사용
class SkipList {
public:
  struct RangeEntry {
    int key;
    std::string value;
    // 단순 key-value만이 아니라 tombstone 여부 함께 전달
    //  > 여러 memtable 병합할 때 삭제 여부 반영
    bool tombstone;
  };

  // max_level: skiplist 최대 높이, p: 노드가 상위 레벨로 승격될 확률 
  explicit SkipList(int max_level = 16, float p = 0.5f);
  ~SkipList();

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  //Put
  //기존 값을 덮어쓰지 않고, 새 sequence number가진 새 엔트리 삽입
  void Put(int key, const std::string& value);

  //Get
  //해당 키의 가장 최신 버전 찾아 value 반환, tombstone 이면 false 반환
  bool Get(int key, std::string* out_value) const;

  //Delete
  //실제 노드 삭제가 아니라 tombstone 엔트리 삽입
  //삭제 직전 최신 엔트리가 실제 값이었는가를 반환
  bool Delete(int key);

  //RangeScan
  //[start_key, end_key] 범위에 포함되는 key들의 최신 유효 key-value를 반환
  //key(tombstone)는 결과에 포함되지 않는다.
  std::vector<std::pair<int, std::string>>
  RangeScan(int start_key,
            int end_key) const;
// memdb.cc에서 사용하는 helper 함수
  bool GetLatestEntry(int key, RangeEntry* out_entry) const;
  std::vector<RangeEntry> RangeScanEntries(int start_key, int end_key) const;
                                
private:
  //각 노드는 (key, seq, value, tombstone)
  //같은 엔트리가 여러 층에 존재할 수 있으므로 down 포인터
  struct Node {
    int key;
    int64_t seq; // sequence number
    std::string value;  // 저장된 값
    bool tombstone; 
    Node* next; // 같은 레벨 오른쪽 
    Node* down; // 아래 레벨의 같은 entry
  };

  // 확률적으로 새 노드의 높이를 결정 > 평균적으로 O(log n) 탐색 성능
  int RandomLevel();
  void InsertInternal(int key, const std::string& value, bool tombstone);
  // (key, seq) 이상인 첫 번째 노드를 찾기
  // update -> 각 레벨에서 삽입 직전 predecessor를 저장
  Node* FindGreaterOrEqual(int key, int64_t seq,
                           std::vector<Node*>* update) const;
  
  // 정렬 기준 1) key오름차순 2) 같은key면 seq 내림차순
  // -> 같은 key의 최신 버전이 앞쪽에 위치
  static bool Less(int a_key, int64_t a_seq, int b_key, int64_t b_seq);

  Node* head_;  // 최상단 sentinel head
  int max_level_; // 최대 레벨 수
  float p_; // 상위 레벨 승격 확률
  int64_t next_seq_;  // 다음 삽입에 사용할 sequence number
};

#endif // LAB2_SKIPLIST_H
