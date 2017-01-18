
#include <errno.h>
#include <thread>

#include <boost/bind.hpp>
#include "librbd/Throttle.h"
#include "common/dout.h"
#include "common/ceph_context.h"
#include "common/perf_counters.h"

#define dout_subsys ceph_subsys_throttle

#undef dout_prefix
#define dout_prefix *_dout << "BucketThrottle(" << (void*)this << ") "

void TokenBucketThrottle::add_tokens() {
    //Mutex::Locker l(m_lock);
    uint64_t current = throttle.get_current();
    uint64_t max = throttle.get_max();
    if (current + m_avg <= max) {
      throttle.put(m_avg);
    } else {
      throttle.put(max - current);
    }
    ldout(m_cct, 5) << "want to put: " << m_avg << " and remain " << throttle.get_current() << dendl;

    FunctionContext *ctx = new FunctionContext(
      boost::bind(&TokenBucketThrottle::add_tokens, this));
    m_timer->add_event_after(1, ctx);
    return;
}

int TokenBucketThrottle::get(uint64_t c) {
  ldout(m_cct, 5) << "want to get: " << c << " and remain " << throttle.get_current() << dendl;
  return throttle.get(c);
}

void TokenBucketThrottle::set_max(uint64_t m) {
  throttle.set_max(m);
}

void TokenBucketThrottle::set_avg(uint64_t avg) {
  m_avg = avg;
}

Bucket::Bucket(CephContext *cct, const std::string& n, uint64_t m)
  : cct(cct), name(n),
    remain(m), max(m),
    lock("Bucket::lock")
{
}

Bucket::~Bucket()
{
  while (!cond.empty()) {
    Cond *cv = cond.front();
    delete cv;
    cond.pop_front();
  }

}

bool Bucket::_wait(uint64_t c)
{
  utime_t start;
  bool waited = false;
  if (_should_wait(c) || !cond.empty()) { // always wait behind other waiters.
    Cond *cv = new Cond;
    cond.push_back(cv);
    waited = true;
    ldout(cct, 2) << "_wait waiting..." << dendl;

    do {
      cv->Wait(lock);
    } while (_should_wait(c) || cv != cond.front());

    ldout(cct, 2) << "_wait finished waiting" << dendl;

    delete cv;
    cond.pop_front();

    // wake up the next guy
    if (!cond.empty())
      cond.front()->SignalOne();
  }
  return waited;
}

bool Bucket::wait()
{
  if (0 == max.read()) {
    return false;
  }

  Mutex::Locker l(lock);
  ldout(cct, 10) << "wait" << dendl;
  return _wait(0);
}

uint64_t Bucket::take(uint64_t c)
{
  if (0 == max.read()) {
    return 0;
  }
  ldout(cct, 10) << "take " << c << dendl;
  {
    Mutex::Locker l(lock);
    remain.sub(c);
  }
  return remain.read();
}

bool Bucket::get(uint64_t c)
{
  if (0 == max.read()) {
    return false;
  }

  assert(c >= 0);
  bool waited = false;
  {
    Mutex::Locker l(lock);
    waited = _wait(c);
    remain.sub(c);
  }
  return waited;
}

/* Returns true if it successfully got the requested amount,
 * or false if it would block.
 */
bool Bucket::get_or_fail(uint64_t c)
{
  if (0 == max.read()) {
    return true;
  }

  assert (c >= 0);
  Mutex::Locker l(lock);
  if (_should_wait(c) || !cond.empty()) {
    ldout(cct, 10) << "get_or_fail " << c << " failed" << dendl;
    return false;
  } else {
    remain.sub(c);
    return true;
  }
}

uint64_t Bucket::put(uint64_t c)
{
  if (0 == max.read()) {
    return 0;
  }

  assert(c >= 0);
  Mutex::Locker l(lock);
  if (c) {
    uint64_t current = remain.read();
    if ((current + c) <= (uint64_t)max.read()) {
      remain.add(c);
    } else {
      remain.set(max.read());
    }
    if (!cond.empty())
      cond.front()->SignalOne();
  }
  //lderr(m_cct) << "want to put: " << c << " and remain " << remain.read() << dendl;
  return remain.read();
}

void Bucket::reset(uint64_t m)
{
  Mutex::Locker l(lock);
  if (!cond.empty())
    cond.front()->SignalOne();
  if (m == 0) {
    remain.set(max.read());
  } else {
    max.set(m);
    remain.set(m);
  }
}

void Bucket::set_max(uint64_t m)
{
  Mutex::Locker l(lock);
  max.set(m);
}
