// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/QosRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::QosRequest: "

namespace librbd {
namespace operation {

using util::create_async_context_callback;
using util::create_context_callback;
using util::create_rados_ack_callback;

template <typename I>
QosRequest<I>::QosRequest(I &image_ctx,
			  Context *on_finish,
			  uint64_t iops_burst, uint64_t iops_avg,
			  uint64_t bps_burst, uint64_t bps_avg, std::string& type)
  : Request<I>(image_ctx, on_finish), m_iops_burst(iops_burst), m_iops_avg(iops_avg),
    m_bps_burst(bps_burst), m_bps_avg(bps_avg), m_type(type) {
}

template <typename I>
void QosRequest<I>::send_op() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  assert(image_ctx.owner_lock.is_locked());

  ldout(cct, 20) << this << " " << __func__ << dendl;
  //send_prepare_lock();
  send_qos_request();
}

template <typename I>
bool QosRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }
  return true;
}

template <typename I>
void QosRequest<I>::send_prepare_lock() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  image_ctx.state->prepare_lock(create_async_context_callback(
    image_ctx, create_context_callback<
    QosRequest<I>,
    &QosRequest<I>::handle_prepare_lock>(this)));
}

template <typename I>
Context *QosRequest<I>::handle_prepare_lock(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to lock image: " << cpp_strerror(*result) << dendl;
    return this->create_context_finisher(*result);
  }

  send_block_writes();
  return nullptr;
}

template <typename I>
void QosRequest<I>::send_block_writes() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  //RWLock::WLocker locker(image_ctx.owner_lock);
  assert(image_ctx.owner_lock.is_locked());
  image_ctx.aio_work_queue->block_writes(create_context_callback<
    QosRequest<I>,
    &QosRequest<I>::handle_block_writes>(this));
}

template <typename I>
Context *QosRequest<I>::handle_block_writes(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to block writes: " << cpp_strerror(*result) << dendl;
    return handle_finish(*result);
  }
  m_writes_blocked = true;

  send_qos_request();
  return nullptr;
}

template <typename I>
void QosRequest<I>::send_qos_request() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  {
    RWLock::RLocker md_locker(image_ctx.md_lock);

    librados::ObjectWriteOperation op;
    cls_client::qos_set(&op, m_iops_burst, m_iops_avg, m_bps_burst, m_bps_avg, m_type);

    using klass = QosRequest<I>;
    librados::AioCompletion *rados_completion = create_rados_ack_callback<klass,
			     &klass::handle_qos_request>(this);
    int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid, rados_completion,
					 &op);
    assert(r == 0);
    rados_completion->release();
  }
}

template <typename I>
Context *QosRequest<I>::handle_qos_request(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to set qos: "
               << cpp_strerror(*result) << dendl;
    return handle_finish(*result);
  }

  send_notify_update();
  return nullptr;
}

template <typename I>
void QosRequest<I>::send_notify_update() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  Context *ctx = create_context_callback<
    QosRequest<I>,
    &QosRequest<I>::handle_notify_update>(this);

  image_ctx.notify_update(ctx);
}

template <typename I>
Context *QosRequest<I>::handle_notify_update(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  return handle_finish(*result);
}

template <typename I>
Context *QosRequest<I>::handle_finish(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << r << dendl;

  {
    //RWLock::WLocker locker(image_ctx.owner_lock);
    assert(image_ctx.owner_lock.is_locked());

    if (image_ctx.exclusive_lock != nullptr && m_requests_blocked) {
      image_ctx.exclusive_lock->unblock_requests();
    }
    if (m_writes_blocked) {
      image_ctx.aio_work_queue->unblock_writes();
    }
  }
  image_ctx.state->handle_prepare_lock_complete();

  return this->create_context_finisher(r);
}

} // namespace operation
} // namespace librbd

template class librbd::operation::QosRequest<librbd::ImageCtx>;
