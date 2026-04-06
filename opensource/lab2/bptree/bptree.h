#ifndef LAB2_BPTREE_H
#define LAB2_BPTREE_H

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

// B+Tree 구현
// 1) 모든 key-value 데이터는 leaf node 에만 저장
// 2) internal node는 탐색을 위한 key와 child pointer만 저장
// 3) leaf node끼리는 next 포인터로 연결, RangeScan 수행
// 4) SkipList 버전과 달리 in-place update 사용
//    -> 같은 key가 다시 들어오면 새 버전을 추가하지 않고 기존 값을 직접 수정
class BPlusTree {
 public:
  explicit BPlusTree(int degree = 4);
  ~BPlusTree();

  BPlusTree(const BPlusTree&) = delete;
  BPlusTree& operator=(const BPlusTree&) = delete;

  // key-value 삽입
  // 동일 key가 이미 있으면 값을 직접 갱신.
  void Put(int key, const std::string& value);

  // key 조회
  bool Get(int key, std::string* value) const;

  // key 삭제
  // B+Tree는 in-place update 구조이므로 tombstone이 아니라 실제 삭제.
  bool Delete(int key);

  // [start_key, end_key] 범위의 key-value를 오름차순으로 반환
  std::vector<std::pair<int, std::string>> RangeScan(int start_key,
                                                     int end_key) const;

 private:
  // B+Tree 노드 구조
  struct Node {
    bool is_leaf;                     // leaf node 여부
    std::vector<int> keys;            // 정렬된 key 목록
    std::vector<std::string> values;  // leaf 전용 value 목록
    std::vector<Node*> children;      // internal 전용 child 포인터
    Node* parent;                     // 부모 포인터
    Node* next;                       // leaf node 연결용 포인터

    explicit Node(bool leaf) : is_leaf(leaf), parent(nullptr), next(nullptr) {}
  };

  // 특정 key가 속해야 하는 leaf node를 찾음.
  Node* FindLeaf(int key) const;

  // leaf split 또는 internal split 후 부모에 새 child를 연결.
  void InsertIntoParent(Node* left, int separator_key, Node* right);

  // leaf node overflow 처리
  void SplitLeaf(Node* leaf);

  // internal node overflow 처리
  void SplitInternal(Node* node);

  // 삭제 후 underflow를 해결.
  void RebalanceAfterDelete(Node* node);

  // internal node의 keys를 children 기준으로 다시 계산.
  void RebuildInternalKeys(Node* node);

  // 특정 노드부터 루트 방향으로 올라가며 separator key를 갱신.
  void UpdateKeysUpward(Node* node);

  // 어떤 subtree의 가장 작은 key를 구함.
  // internal separator key를 만들 때 사용.
  int FirstKey(const Node* node) const;

  // 트리 전체 메모리 해제
  void Destroy(Node* node);

  Node* root_;   // 루트 노드
  int degree_;   // B+Tree 차수(M)
};

#endif // LAB2_BPTREE_H
