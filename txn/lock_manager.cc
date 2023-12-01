
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

// Total wasted hours: 29

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

  // Check if the key exists in the lock table.
  if (lock_table_.find(key) != lock_table_.end()) {
    deque<LockRequest>* requests = lock_table_[key];

    // Check if empty deque
    if (!requests->empty()) {

      // If the current transaction is requesting an Exclusive lock, has previously requested an Exclusive lock, and is at the front of the lock request queue, then return true.
      LockRequest front = lock_table_[key]->front();
      if (front.mode_ == EXCLUSIVE && front.txn_ == txn) {
          return true;
      }

      // If previously already requested a Shared lock and is in front of the deque, check if eligible for upgrade
      LockRequest& front_p = lock_table_[key]->front();
      if (front_p.mode_ == SHARED && front_p.txn_ == txn) {
          bool onlyThisTxn = true;
          for (const auto& lr : *lock_table_[key]) {
              if (lr.txn_ != txn) {
                  onlyThisTxn = false;
                  break;
              }
          }

          // If the deque only contains this txn, upgrade to lock EXCLUSIVE
          if (onlyThisTxn) {
              front_p.mode_ = EXCLUSIVE;
              return true;
          }
      }
    }
      
    // If deque is empty then no need to wait and return true, false otherwise
    bool emptyDeque = false;
    if (requests->empty()) {
      emptyDeque = true;
    } else {
      txn_waits_[txn]++;
    }

    // Create a new request for Exclusive lock
    unordered_map<Key, deque<LockRequest> *>::iterator pointer = lock_table_.find(key);
    // Create a new lock then push it to lock table
    LockRequest lock = LockRequest(EXCLUSIVE, txn);
    deque<LockRequest> *lock_queue = pointer->second;
    lock_queue->push_back(lock);
    
    return emptyDeque;
  }

  // If the key is not in the lock table or is not locked, grant the lock.
  LockRequest *new_lock = new LockRequest(EXCLUSIVE, txn);
  deque<LockRequest> *lock_queue = new deque<LockRequest>();
  lock_queue->push_back(*new_lock);
  lock_table_.insert({key, lock_queue});
  return true;
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {

  // Check if the key exists in the lock table.
  if (lock_table_.find(key) != lock_table_.end()) {

    // Check for empty deque
    deque<LockRequest>* requests = lock_table_[key];
    if (!requests->empty()) {

      // If the current transaction is requesting an Shared lock, has previously requested an Exclusive lock, and is at the front of the lock request queue, then return true (use Exclusive lock to read).
      LockRequest front = lock_table_[key]->front();
      if (front.mode_ == EXCLUSIVE && front.txn_ == txn) {
          return true;
      }

      bool is_last_exclusive = false;
      // Find the queue lock for this key
      unordered_map<Key, deque<LockRequest> *>::iterator pointer = lock_table_.find(key);
      // Create a new lock then push it to lock table
      LockRequest lock = LockRequest(SHARED, txn);
      deque<LockRequest> *lock_queue = pointer->second;

      // If there are any exclusive lock that are not requested by txn then txn must wait
      for (const auto& el : *lock_queue) {
          if (el.mode_ == EXCLUSIVE) {
              is_last_exclusive = true;
              txn_waits_[txn]++;
              break;
          }
      }

      lock_queue->push_back(lock);
      return !is_last_exclusive;
    } else {
      // If empty deque
      unordered_map<Key, deque<LockRequest> *>::iterator pointer = lock_table_.find(key);
      // Create a new lock then push it to lock table
      LockRequest lock = LockRequest(SHARED, txn);
      deque<LockRequest> *lock_queue = pointer->second;
      lock_queue->push_back(lock);

      return true;
    }
      
  }

  // If the key is not in the lock table or is not locked exclusively by another transaction, grant the lock.
  // Create a new lock then push it to lock table
  LockRequest *new_lock = new LockRequest(SHARED, txn);
  deque<LockRequest> *lock_queue = new deque<LockRequest>();
  lock_queue->push_back(*new_lock);
  lock_table_.insert({key, lock_queue});
  return true;
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  // Check if the key exists in the lock table.
  if (lock_table_.find(key) != lock_table_.end()) {
    deque<LockRequest>& requests = *lock_table_[key];
    if (!requests.empty()) {

      // Find the LockRequest for this txn
      LockRequest* found_request = nullptr;
      for (auto& request : requests) {
          if (request.txn_ == txn) {
              found_request = &request;
              break;
          }
      }

      // If txn has a lock or a pending request on the key, remove the LockRequest.
      if (found_request != nullptr) {
        bool wasShared = found_request->mode_ == SHARED;
        bool lock_holder = requests.front().txn_ == txn;
        int txn_index = 0;
        for (auto it = requests.begin(); it != requests.end();) {
          if (it->txn_ == found_request->txn_) {
            it = requests.erase(it);
            break;
          } else {
            ++it;
            txn_index++;
          }
        }
        
        // If txn was holding a lock and there are other transactions waiting for the lock,
        // try to grant the lock to the next transaction(s) in the queue.
        if (!requests.empty() && lock_holder) { 
          if (requests.front().mode_ == EXCLUSIVE) {

            if (txn_waits_[requests.front().txn_] > 0) { // Extra safeguard
              txn_waits_[requests.front().txn_]--;
            }
            bool found = false;
            if (txn_waits_[requests.front().txn_] == 0) {
              for (auto &ready_txn : *ready_txns_) {
                if (ready_txn == requests.front().txn_) {
                  found = true;
                  break;
                }
              }
              if (!found) {
                ready_txns_->push_back(requests.front().txn_);
              }
              if (txn_waits_.find(requests.front().txn_) != txn_waits_.end()) {
                txn_waits_.erase(requests.front().txn_);
              }
            } 
            
          } 
          // If txn was holding an Exclusive lock and its blocking the next txn that requested Shared lock, try to grant it
          else if (!wasShared) { 
            for (const LockRequest& request : requests) {
              if (request.mode_ == SHARED) {

                if (txn_waits_[request.txn_] > 0) { // Extra safeguard
                  txn_waits_[request.txn_]--;
                }
                bool is_inside_ready_queue = false;
                for (auto &ready_txn : *ready_txns_) {
                  if (ready_txn == request.txn_) {
                    is_inside_ready_queue = true;
                    break;
                  }
                }
                if (txn_waits_[request.txn_] == 0 && !is_inside_ready_queue) {
                  ready_txns_->push_back(request.txn_);
                  txn_waits_.erase(request.txn_);
                }
              } else {
                break;
              }
            }
          }
        } 
        // If txn requested an Exclusive lock that blocks the next txn that requested Shared lock, try to grant it
        else if (!requests.empty() && !wasShared) {
          int cnt = 0; 
          for (const LockRequest& request : requests) {
              if (request.mode_ == EXCLUSIVE && cnt >= txn_index) {
                break;
              }
              if (request.mode_ == SHARED && cnt >= txn_index) {
                if (txn_waits_[request.txn_] > 0) { // Extra safeguard
                  txn_waits_[request.txn_]--;
                }
                bool is_inside_ready_queue = false;
                for (auto &ready_txn : *ready_txns_) {
                  if (ready_txn == request.txn_) {
                    is_inside_ready_queue = true;
                    break;
                  }
                }
                if (txn_waits_[request.txn_] == 0 && !is_inside_ready_queue) {
                  ready_txns_->push_back(request.txn_);
                  txn_waits_.erase(request.txn_);
                }
              }
              cnt++;
            }
        }

        // Decrement the wait count for this transaction.
        if (txn_waits_[txn] > 0) { // Extra safeguard
          txn_waits_[txn]--;
        }
        bool found = false;
        if (lock_holder && txn_waits_[txn] == 0) {
          for (auto &ready_txn : *ready_txns_) {
            if (ready_txn == txn) {
              found = true;
              break;
            }
          }
          if (!found) {
            ready_txns_->push_back(txn);
          }
          if (txn_waits_.find(txn) != txn_waits_.end()) {
            txn_waits_.erase(txn);
          }
        }
      }
    }
  }
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  // Empty owners to reinit value
  owners->clear();

  // Check if the key exists in the lock table.
  if (lock_table_.find(key) != lock_table_.end()) {
    deque<LockRequest>& requests = *lock_table_[key];

    // If there are no requests for the key, the lock is Unlocked.
    if (requests.empty()) {
      return UNLOCKED;
    }

    // Add the transaction(s) currently holding the lock to 'owners'.
    for (const LockRequest& request : requests) {
      owners->push_back(request.txn_);

      // If we encounter a request for an Exclusive lock, no other transactions can hold the lock.
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
    // If the key is not in the lock table, the lock is Unlocked.
    return UNLOCKED;
  }
}

