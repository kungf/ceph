// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_QOS_REQUEST_H
#define CEPH_LIBRBD_OPERATION_QOS_REQUEST_H

#include "librbd/operation/Request.h"
#include <iosfwd>
#include <string>

class Context;

namespace librbd {

class ImageCtx;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class QosRequest : public Request<ImageCtxT> {
public:
  QosRequest(ImageCtxT &image_ctx, Context *on_finish,
		       uint64_t iops_burst, uint64_t iops_avg,
		       uint64_t bps_burst, uint64_t bps_avg, std::string& type);

protected:
  virtual void send_op();
  virtual bool should_complete(int r);

  virtual journal::Event create_event(uint64_t op_tid) const {
    return journal::QosSetEvent(op_tid, m_iops_burst, m_iops_avg, m_bps_burst, m_bps_avg, m_type);
  }

private:
  uint64_t m_iops_burst;
  uint64_t m_iops_avg;
  uint64_t m_bps_burst;
  uint64_t m_bps_avg;
  std::string m_type;

  bool m_requests_blocked = false;
  bool m_writes_blocked = false;

  void send_prepare_lock();
  Context *handle_prepare_lock(int *result);

  void send_block_writes();
  Context *handle_block_writes(int *result);

  void send_qos_request();
  Context *handle_qos_request(int *result);

  void send_notify_update();
  Context *handle_notify_update(int *result);

  Context *handle_finish(int r);
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::QosRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_QOS_REQUEST_H
