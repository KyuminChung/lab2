#include "bptree.h"

#include <algorithm>

// degree는 최소 3 이상으로 보정한다.
// degree = 한 internal node가 가질 수 있는 최대 child 수로 사용한다.
BPlusTree::BPlusTree(int degree)
    : root_(nullptr), degree_(std::max(3, degree)) {}

// 소멸 시 트리 전체 노드를 재귀적으로 해제한다.
BPlusTree::~BPlusTree() { Destroy(root_); }

void BPlusTree::Destroy(Node* node) {
  if (node == nullptr) return;

  // internal node는 모든 child를 먼저 삭제
  if (!node->is_leaf) {
    for (Node* child : node->children) {
      Destroy(child);
    }
  }

  delete node;
}

// subtree의 가장 작은 key를 구하는 함수
// internal node의 separator key를 재구성할 때 사용한다.
int BPlusTree::FirstKey(const Node* node) const {
  const Node* cur = node;

  // leaf가 나올 때까지 가장 왼쪽 child로 내려간다.
  while (cur != nullptr && !cur->is_leaf) {
    if (cur->children.empty()) return 0;
    cur = cur->children.front();
  }

  if (cur == nullptr || cur->keys.empty()) return 0;
  return cur->keys.front();
}

// internal node의 keys를 children 정보만으로 다시 계산한다.
// B+Tree에서 internal node의 key는 "오른쪽 child subtree의 최소 key" 의미로 관리한다.
void BPlusTree::RebuildInternalKeys(Node* node) {
  if (node == nullptr || node->is_leaf) return;

  node->keys.clear();
  for (size_t i = 1; i < node->children.size(); ++i) {
    node->keys.push_back(FirstKey(node->children[i]));
  }
}

// 특정 노드부터 부모 방향으로 올라가며 internal separator key를 갱신한다.
void BPlusTree::UpdateKeysUpward(Node* node) {
  for (Node* cur = node; cur != nullptr; cur = cur->parent) {
    if (!cur->is_leaf) {
      RebuildInternalKeys(cur);
    }
  }
}

// key가 들어 있어야 할 leaf node를 찾는다.
// internal node의 keys를 기준으로 적절한 child를 내려간다.
BPlusTree::Node* BPlusTree::FindLeaf(int key) const {
  Node* cur = root_;
  if (cur == nullptr) return nullptr;

  while (!cur->is_leaf) {
    size_t idx = 0;
    while (idx < cur->keys.size() && key >= cur->keys[idx]) {
      ++idx;
    }
    cur = cur->children[idx];
  }

  return cur;
}

// key 조회
// leaf에서 lower_bound로 key를 찾는다.
bool BPlusTree::Get(int key, std::string* value) const {
  Node* leaf = FindLeaf(key);
  if (leaf == nullptr) return false;

  auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
  if (it == leaf->keys.end() || *it != key) return false;

  size_t idx = static_cast<size_t>(it - leaf->keys.begin());
  if (value != nullptr) {
    *value = leaf->values[idx];
  }
  return true;
}

// Put:
// 1) 트리가 비어 있으면 root leaf 생성
// 2) 해당 leaf를 찾아 삽입 위치 결정
// 3) 동일 key 존재 시 in-place update
// 4) overflow면 split 수행
void BPlusTree::Put(int key, const std::string& value) {
  // 첫 삽입: root leaf 생성
  if (root_ == nullptr) {
    root_ = new Node(true);
    root_->keys.push_back(key);
    root_->values.push_back(value);
    return;
  }

  Node* leaf = FindLeaf(key);
  auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
  size_t idx = static_cast<size_t>(it - leaf->keys.begin());

  // B+Tree는 in-place update 사용:
  // 같은 key가 있으면 기존 값을 직접 수정
  if (it != leaf->keys.end() && *it == key) {
    leaf->values[idx] = value;
    return;
  }

  // 새로운 key 삽입
  leaf->keys.insert(it, key);
  leaf->values.insert(leaf->values.begin() + static_cast<long>(idx), value);

  // leaf가 최대 key 수(M-1)를 넘으면 split
  if (static_cast<int>(leaf->keys.size()) > degree_ - 1) {
    SplitLeaf(leaf);
  } else {
    // split이 없더라도 separator key가 바뀔 수 있으므로 갱신
    UpdateKeysUpward(leaf->parent);
  }
}

// leaf split
// leaf의 key-value를 좌/우로 나누고, leaf linked list도 유지한다.
// 부모에는 오른쪽 leaf의 첫 key가 separator 역할로 반영된다.
void BPlusTree::SplitLeaf(Node* leaf) {
  Node* right = new Node(true);
  right->parent = leaf->parent;
  right->next = leaf->next;
  leaf->next = right;

  const int total = static_cast<int>(leaf->keys.size());

  // 왼쪽 leaf가 하나 더 많이 갖도록 분할
  const int left_size = (total + 1) / 2;

  right->keys.assign(leaf->keys.begin() + left_size, leaf->keys.end());
  right->values.assign(leaf->values.begin() + left_size, leaf->values.end());

  leaf->keys.erase(leaf->keys.begin() + left_size, leaf->keys.end());
  leaf->values.erase(leaf->values.begin() + left_size, leaf->values.end());

  // 부모에 right child를 연결
  InsertIntoParent(leaf, right->keys.front(), right);
}

// split 결과를 부모에 연결하는 공통 함수
void BPlusTree::InsertIntoParent(Node* left, int separator_key, Node* right) {
  (void)separator_key;

  // left가 root였다면 새 root 생성
  if (left->parent == nullptr) {
    Node* new_root = new Node(false);
    new_root->children.push_back(left);
    new_root->children.push_back(right);
    left->parent = new_root;
    right->parent = new_root;

    RebuildInternalKeys(new_root);
    root_ = new_root;
    return;
  }

  Node* parent = left->parent;
  auto it = std::find(parent->children.begin(), parent->children.end(), left);
  size_t idx = static_cast<size_t>(it - parent->children.begin());

  // left 바로 오른쪽에 새 child 삽입
  parent->children.insert(parent->children.begin() + static_cast<long>(idx + 1),
                          right);
  right->parent = parent;

  RebuildInternalKeys(parent);

  // internal node overflow:
  // child 수가 degree_를 넘으면 split
  if (static_cast<int>(parent->children.size()) > degree_) {
    SplitInternal(parent);
  } else {
    UpdateKeysUpward(parent->parent);
  }
}

// internal split
// child 배열을 좌/우로 나누고, separator key는 children 기반으로 다시 계산한다.
void BPlusTree::SplitInternal(Node* node) {
  Node* right = new Node(false);
  right->parent = node->parent;

  const int total_children = static_cast<int>(node->children.size());

  // 왼쪽이 하나 더 많이 갖도록 분할
  const int left_children = (total_children + 1) / 2;

  right->children.assign(node->children.begin() + left_children,
                         node->children.end());
  node->children.erase(node->children.begin() + left_children,
                       node->children.end());

  for (Node* child : right->children) {
    child->parent = right;
  }

  RebuildInternalKeys(node);
  RebuildInternalKeys(right);

  const int separator_key = FirstKey(right);
  InsertIntoParent(node, separator_key, right);
}

// Delete:
// 1) leaf에서 key 제거
// 2) root 특수 처리
// 3) separator key 갱신
// 4) underflow면 rebalance
bool BPlusTree::Delete(int key) {
  Node* leaf = FindLeaf(key);
  if (leaf == nullptr) return false;

  auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
  if (it == leaf->keys.end() || *it != key) return false;

  size_t idx = static_cast<size_t>(it - leaf->keys.begin());
  leaf->keys.erase(it);
  leaf->values.erase(leaf->values.begin() + static_cast<long>(idx));

  // root leaf가 비면 트리 전체가 비게 된다.
  if (leaf == root_) {
    if (leaf->keys.empty()) {
      delete root_;
      root_ = nullptr;
    }
    return true;
  }

  UpdateKeysUpward(leaf->parent);
  RebalanceAfterDelete(leaf);
  return true;
}

// 삭제 후 underflow 처리
// 우선 형제에게서 빌릴 수 있는지 보고,
// 불가능하면 병합한다.
void BPlusTree::RebalanceAfterDelete(Node* node) {
  if (node == nullptr) return;

  // root 특수 처리:
  // internal root의 child가 하나만 남으면 root를 한 단계 내려도 된다.
  if (node == root_) {
    if (!root_->is_leaf && root_->children.size() == 1) {
      Node* old_root = root_;
      root_ = root_->children.front();
      root_->parent = nullptr;
      delete old_root;
    } else if (!root_->is_leaf) {
      RebuildInternalKeys(root_);
    }
    return;
  }

  // 과제 설명 기준 degree=M일 때
  // leaf 최소 key 수 = floor(M/2)
  // internal 최소 child 수 = floor(M/2)
  const int min_leaf_keys = std::max(1, degree_ / 2);
  const int min_children = std::max(2, degree_ / 2);

  bool underflow = false;
  if (node->is_leaf) {
    underflow = static_cast<int>(node->keys.size()) < min_leaf_keys;
  } else {
    underflow = static_cast<int>(node->children.size()) < min_children;
  }

  // underflow가 아니면 separator key만 갱신하면 끝
  if (!underflow) {
    UpdateKeysUpward(node->parent);
    return;
  }

  Node* parent = node->parent;
  auto it = std::find(parent->children.begin(), parent->children.end(), node);
  int idx = static_cast<int>(it - parent->children.begin());

  Node* left = (idx > 0) ? parent->children[idx - 1] : nullptr;
  Node* right =
      (idx + 1 < static_cast<int>(parent->children.size()))
          ? parent->children[idx + 1]
          : nullptr;

  if (node->is_leaf) {
    // 1) 왼쪽 형제에게서 하나 빌리기
    if (left != nullptr &&
        static_cast<int>(left->keys.size()) > min_leaf_keys) {
      node->keys.insert(node->keys.begin(), left->keys.back());
      node->values.insert(node->values.begin(), left->values.back());
      left->keys.pop_back();
      left->values.pop_back();

      UpdateKeysUpward(parent);
      return;
    }

    // 2) 오른쪽 형제에게서 하나 빌리기
    if (right != nullptr &&
        static_cast<int>(right->keys.size()) > min_leaf_keys) {
      node->keys.push_back(right->keys.front());
      node->values.push_back(right->values.front());
      right->keys.erase(right->keys.begin());
      right->values.erase(right->values.begin());

      UpdateKeysUpward(parent);
      return;
    }

    // 3) 왼쪽 형제와 병합
    if (left != nullptr) {
      left->keys.insert(left->keys.end(), node->keys.begin(), node->keys.end());
      left->values.insert(left->values.end(),
                          node->values.begin(), node->values.end());
      left->next = node->next;

      parent->children.erase(parent->children.begin() + idx);
      delete node;

      RebuildInternalKeys(parent);
      RebalanceAfterDelete(parent);
      return;
    }

    // 4) 오른쪽 형제와 병합
    if (right != nullptr) {
      node->keys.insert(node->keys.end(), right->keys.begin(), right->keys.end());
      node->values.insert(node->values.end(),
                          right->values.begin(), right->values.end());
      node->next = right->next;

      parent->children.erase(parent->children.begin() + (idx + 1));
      delete right;

      RebuildInternalKeys(parent);
      RebalanceAfterDelete(parent);
      return;
    }
  } else {
    // internal node underflow

    // 1) 왼쪽 형제에게서 child 하나 빌리기
    if (left != nullptr &&
        static_cast<int>(left->children.size()) > min_children) {
      Node* borrowed = left->children.back();
      left->children.pop_back();

      borrowed->parent = node;
      node->children.insert(node->children.begin(), borrowed);

      RebuildInternalKeys(left);
      RebuildInternalKeys(node);
      UpdateKeysUpward(parent);
      return;
    }

    // 2) 오른쪽 형제에게서 child 하나 빌리기
    if (right != nullptr &&
        static_cast<int>(right->children.size()) > min_children) {
      Node* borrowed = right->children.front();
      right->children.erase(right->children.begin());

      borrowed->parent = node;
      node->children.push_back(borrowed);

      RebuildInternalKeys(right);
      RebuildInternalKeys(node);
      UpdateKeysUpward(parent);
      return;
    }

    // 3) 왼쪽 형제와 병합
    if (left != nullptr) {
      for (Node* child : node->children) {
        child->parent = left;
        left->children.push_back(child);
      }

      parent->children.erase(parent->children.begin() + idx);
      delete node;

      RebuildInternalKeys(left);
      RebuildInternalKeys(parent);
      RebalanceAfterDelete(parent);
      return;
    }

    // 4) 오른쪽 형제와 병합
    if (right != nullptr) {
      for (Node* child : right->children) {
        child->parent = node;
        node->children.push_back(child);
      }

      parent->children.erase(parent->children.begin() + (idx + 1));
      delete right;

      RebuildInternalKeys(node);
      RebuildInternalKeys(parent);
      RebalanceAfterDelete(parent);
      return;
    }
  }
}

// RangeScan:
// 시작 key가 속한 leaf를 찾은 뒤,
// leaf linked list(next)를 따라가며 end_key까지 순차 조회한다.
std::vector<std::pair<int, std::string>> BPlusTree::RangeScan(
    int start_key, int end_key) const {
  std::vector<std::pair<int, std::string>> out;
  if (root_ == nullptr || start_key > end_key) return out;

  Node* leaf = FindLeaf(start_key);
  if (leaf == nullptr) return out;

  while (leaf != nullptr) {
    for (size_t i = 0; i < leaf->keys.size(); ++i) {
      int key = leaf->keys[i];

      if (key < start_key) continue;
      if (key > end_key) return out;

      out.emplace_back(key, leaf->values[i]);
    }
    leaf = leaf->next;
  }

  return out;
}