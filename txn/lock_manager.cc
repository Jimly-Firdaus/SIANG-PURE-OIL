
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

// Total wasted hours: 16

#include "txn/lock_manager.h"
using namespace std;


LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  //
  // Implement this method!
  return true;
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  //
  // Implement this method!
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  //
  // Implement this method!
  return UNLOCKED;
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  // cout << "REQUESTED WRITE LOOOOOOOCK" << endl;
  //
  // Implement this method!

  // Write lock is EXCLUSIVE type lock

  // Check if the key exists in the lock table.
  if (lock_table_.find(key) != lock_table_.end()) {
    // cout << "WAITING FOR XL" << endl;
    // If the key is locked by any transaction (SHARED or EXCLUSIVE), enqueue the request and return false.
    // if (!lock_table_[key]->empty()) {
      // lock_table_[key]->push_back(LockRequest(EXCLUSIVE, txn));
      unordered_map<Key, deque<LockRequest> *>::iterator pointer = lock_table_.find(key);
      // Create a new lock then push it to lock table
      LockRequest lock = LockRequest(EXCLUSIVE, txn);
      deque<LockRequest> *lock_queue = pointer->second;
      lock_queue->push_back(lock);

      txn_waits_[txn]++;
      return false;
    // }
  }

  // If the key is not in the lock table or is not locked, grant the lock.
  LockRequest *new_lock = new LockRequest(EXCLUSIVE, txn);
  deque<LockRequest> *lock_queue = new deque<LockRequest>();
  lock_queue->push_back(*new_lock);
  lock_table_.insert({key, lock_queue});
  // cout << "WRITE LOCK GRANTED" << endl;
  return true;
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  // Implement this method!

  // unordered_map<Key, deque<LockRequest> *>::iterator pointer = lock_table_.find(key);

  if (lock_table_.find(key) != lock_table_.end()) {
      // find the queue lock for this key
      unordered_map<Key, deque<LockRequest> *>::iterator pointer = lock_table_.find(key);
      // Create a new lock then push it to lock table
      LockRequest lock = LockRequest(SHARED, txn);
      deque<LockRequest> *lock_queue = pointer->second;
      lock_queue->push_back(lock);

      txn_waits_[txn]++;
      return false;
  }

  // If the key is not in the lock table or is not locked exclusively by another transaction, grant the lock.
  // Create a new lock then push it to lock table
  LockRequest *new_lock = new LockRequest(SHARED, txn);
  deque<LockRequest> *lock_queue = new deque<LockRequest>();
  lock_queue->push_back(*new_lock);
  lock_table_.insert({key, lock_queue});
  return true;



  // // Single search in the map
  // auto pointer = lock_table_.find(key);
  // if (pointer != lock_table_.end()) {
  //     // Create a new lock then push it to lock table
  //     LockRequest lock = LockRequest(SHARED, txn);
  //     pointer->second->push_back(lock);
  //     txn_waits_[txn]++;
  //     return false;
  // }

  // // If the key is not in the lock table or is not locked exclusively by another transaction, grant the lock.
  // // Use smart pointers for automatic memory management
  // auto new_lock = make_shared<LockRequest>(SHARED, txn);
  // auto lock_queue = make_shared<deque<LockRequest>>();
  // lock_queue->push_back(*new_lock);
  // lock_table_.insert({key, lock_queue.get()});
  // return true;
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  // Implement this method!
  
  // Check if the key exists in the lock table.
  if (lock_table_.find(key) != lock_table_.end()) {
    deque<LockRequest>& requests = *lock_table_[key];

    // Find the LockRequest for this transaction.
    LockRequest* found_request = nullptr;
    for (auto& request : requests) {
        if (request.txn_ == txn) {
            found_request = &request;
            break;
        }
    }

    // If the transaction has a lock or a pending request on the key, remove the LockRequest.
    if (found_request != nullptr) {
      bool wasShared = found_request->mode_ == SHARED;
      for (auto it = requests.begin(); it != requests.end();) {
        if (it->txn_ == found_request->txn_) {
          it = requests.erase(it);
          break;
        } else {
          ++it;
        }
      }
      
      // If the transaction was holding a lock and there are other transactions waiting for the lock,
      // try to grant the lock to the next transaction(s) in the queue.
      if (!requests.empty()) {
        // If the next transaction requested an exclusive lock, grant it.
        if (requests.front().mode_ == EXCLUSIVE) {
          txn_waits_[requests.front().txn_]--;
          ready_txns_->push_back(requests.front().txn_);
        } else if (!wasShared) {
          // If the released lock was exclusive lock, grant the shared lock to all transactions at the front of the deque
          // that requested a shared lock.
          for (const LockRequest& request : requests) {
            if (request.mode_ == SHARED) {
              txn_waits_[request.txn_]--;
              if (txn_waits_[request.txn_] == 0) {
                ready_txns_->push_back(request.txn_);
                txn_waits_.erase(request.txn_);
              }
            } else {
              break;
            }
          }
        }
      }

      // Decrement the wait count for this transaction.
      txn_waits_[txn]--;
    }
  }
}

// void LockManagerB::Release(Txn* txn, const Key& key) {
//   // Check if the key exists in the lock table.
//   if (lock_table_.find(key) != lock_table_.end()) {
//     deque<LockRequest>* requests = lock_table_[key];

//     // Find the LockRequest for this transaction.
//     auto found_request = requests->end();
//     for (auto it = requests->begin(); it != requests->end(); ++it) {
//         if (it->txn_ == txn) {
//             found_request = it;
//             break;
//         }
//     }

//     // If the transaction has a lock or a pending request on the key, remove the LockRequest.
//     if (found_request != requests->end()) {
//       bool wasShared = found_request->mode_ == SHARED;
//       requests->erase(found_request);
      
//       // If the transaction was holding a lock and there are other transactions waiting for the lock,
//       // try to grant the lock to the next transaction(s) in the queue.
//       if (!requests->empty()) {
//         // If the next transaction requested an exclusive lock, grant it.
//         if (requests->front().mode_ == EXCLUSIVE) {
//           txn_waits_[requests->front().txn_]--;
//           ready_txns_->push_back(requests->front().txn_);
//         } else if (!wasShared) {
//           // If the released lock was exclusive lock, grant the shared lock to all transactions at the front of the deque
//           // that requested a shared lock.
//           for (const LockRequest& request : *requests) {
//             if (request.mode_ == SHARED) {
//               txn_waits_[request.txn_]--;
//               if (txn_waits_[request.txn_] == 0) {
//                 ready_txns_->push_back(request.txn_);
//                 txn_waits_.erase(request.txn_);
//               }
//             } else {
//               break;
//             }
//           }
//         }
//       }

//       // Decrement the wait count for this transaction.
//       txn_waits_[txn]--;
//     }
//   }
// }


LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  //
  // Implement this method!

  // Empty owners to reinit value
  owners->clear();

  // Check if the key exists in the lock table.
  if (lock_table_.find(key) != lock_table_.end()) {
    deque<LockRequest>& requests = *lock_table_[key];

    // If there are no requests for the key, the lock is UNLOCKED.
    if (requests.empty()) {
      return UNLOCKED;
    }

    // Add the transaction(s) currently holding the lock to 'owners'.
    for (const LockRequest& request : requests) {
      owners->push_back(request.txn_);

      // If we encounter a request for an EXCLUSIVE lock, no other transactions can hold the lock.
      if (request.mode_ == EXCLUSIVE) {
        if (owners->size() > 1) {
          owners->pop_back();
        }
        break;
      }
    }

    // Return the current LockMode of the lock.
    return requests.front().mode_;
  } else {
    // If the key is not in the lock table, the lock is UNLOCKED.
    return UNLOCKED;
  }
}

