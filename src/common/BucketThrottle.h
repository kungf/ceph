
#ifndef CEPH_RBD_THROTTLE_H
#define CEPH_RBD_THROTTLE_H

#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "common/Timer.h"

#include <list>
#include <map>
#include <iostream>
#include <condition_variable>
#include <chrono>
#include "include/atomic.h"
#include "include/Context.h"

class CephContext;

class SafeTimer;

class Bucket {
  CephContext *cct;
  const std::string name;
  ceph::atomic_t remain, max;
  Mutex lock;
  list<Cond*> cond;

public:
  Bucket(CephContext *cct, const std::string& n, uint64_t m = 0);
  ~Bucket();

private:
  void _reset_max(uint64_t m);
  bool _should_wait(uint64_t c) const {
    uint64_t cur = remain.read();
    return (cur <= 0);
  }

  bool _wait(uint64_t c);

public:
  uint64_t get_current() const {
    return remain.read();
  }

  uint64_t get_max() const { return max.read(); }
  void set_max(uint64_t m);

  /**
   * set the new max number, and wait until the number of taken slots drains
   * and drops below this limit.
   *
   * @param m the new max number
   * @returns true if this method is blocked, false it it returns immediately
   */
  bool wait();

  /**
   * take the specified number of slots from the stock regardless the throttling
   * @param c number of slots to take
   * @returns the total number of taken slots
   */
  uint64_t take(uint64_t c = 1);

  /**
   * get the specified amount of slots from the stock, but will wait if the
   * total number taken by consumer would exceed the maximum number.
   * @param c number of slots to get
   * @param m new maximum number to set, ignored if it is 0
   * @returns true if this request is blocked due to the throttling, false 
   * otherwise
   */
  bool get(uint64_t c = 1);

  /**
   * the unblocked version of @p get()
   * @returns true if it successfully got the requested amount,
   * or false if it would block.
   */
  bool get_or_fail(uint64_t c = 1);

  /**
   * put slots back to the stock
   * @param c number of slots to return
   * @returns number of requests being hold after this
   */
  uint64_t put(uint64_t c = 1);
   /**
   * reset the zero to the stock
   */
  void reset(uint64_t m = 0);

  bool should_wait(uint64_t c) const {
    return _should_wait(c);
  }
  void reset_max(uint64_t m) {
    Mutex::Locker l(lock);
    _reset_max(m);
  }
};


class TokenBucketThrottle {
  CephContext *m_cct;
  Bucket throttle;
  uint64_t m_avg;
  SafeTimer *m_timer;
  Mutex m_timer_lock;
  Mutex m_lock;

public:
  TokenBucketThrottle(CephContext *cct,
		      uint64_t capacity, uint64_t avg)
  : m_cct(cct), throttle(m_cct, "token_bucket_throttle", capacity),
    m_avg(avg), m_timer_lock("TokenBucket::timer_lock"),
    m_lock("TokenBucket::m_lock") {
      m_timer = new SafeTimer(m_cct, m_timer_lock, true);
      m_timer->init();
      {
	Mutex::Locker timer_locker(m_timer_lock);
	add_tokens();
      }
  }
  ~TokenBucketThrottle(){
    {
      Mutex::Locker timer_locker(m_timer_lock);
      m_timer->shutdown();
    }
    delete m_timer;
  }

private:
  void add_tokens();

public:
  int get(uint64_t c);
  void set_max(uint64_t m);
  void set_avg(uint64_t avg);
};

#endif
