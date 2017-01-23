// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/types.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/TextTable.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace qos {

namespace at = argument_types;
namespace po = boost::program_options;

int do_qos_set(librbd::Image& image, uint64_t iops_burst, uint64_t iops_avg,
				    uint64_t bps_burst, uint64_t bps_avg, std::string& type)
{
  uint64_t old_iops_burst, old_iops_avg, old_bps_burst, old_bps_avg;
  std::string old_type = type;
  int r = 0;

  r = image.qos_get(&old_iops_burst, &old_iops_avg, &old_bps_burst, &old_bps_avg, &old_type);
  if (r < 0) {
    std::cerr << "failed to get qos: " << cpp_strerror(r) << std::endl;
    return r;
  }

  if (iops_burst == 0)
    iops_burst = old_iops_burst;
  if (iops_avg == 0)
    iops_avg = old_iops_avg;
  if (bps_burst == 0)
    bps_burst = old_bps_burst;
  if (bps_avg == 0)
    bps_avg = old_bps_avg;

  return image.qos_set(iops_burst, iops_avg, bps_burst, bps_avg, type);

}

int do_set_iops(librbd::Image& image, uint64_t burst, uint64_t avg, std::string& type)
{
  return do_qos_set(image, burst, avg, 0, 0, type);
}

int do_clear_iops(librbd::Image& image, std::string& type)
{
  return do_qos_set(image, UINT64_MAX, UINT64_MAX, 0, 0, type);
}

void get_iops_arguments(po::options_description *pos,
			po::options_description *opt) {
  at::add_image_spec_options(pos, opt, at::ARGUMENT_MODIFIER_NONE);
  opt->add_options()
    ("burst", po::value<uint64_t>(), "burst of iops we allow.")
    ("avg", po::value<uint64_t>(), "average of iops we allow.")
    ("type", po::value<std::string>(), "type of iops we allow.");
}

int execute_iops(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  uint64_t burst;
  uint64_t avg;
  std::string type;

  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);

  if (vm.count("burst")) {
    burst = vm["burst"].as<uint64_t>();
  } else {
    burst = 0;
  }

  if (vm.count("avg")) {
    avg = vm["avg"].as<uint64_t>();
  } else {
    std::cerr << "no average limit set" << std::endl;
    return -EINVAL;
  }

  if (vm.count("type")){
    type = vm["type"].as<std::string>();
  }else{
    type = "all";
  }

  if (avg > burst) {
    std::cerr << "average iops should smaller than burst value" << std::endl;
    return -EINVAL;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", false, &rados,
				 &io_ctx, &image);
  if (r < 0) {
      return r;
  }

  r = do_set_iops(image, burst, avg, type);

  if (r < 0) {
    std::cerr << "rbd: setting iops limit failed: " << cpp_strerror(r)
	      << std::endl;
    return r;
  }
  return 0;
}

Shell::Action action_list(
  {"qos", "iops"}, {}, "Set the iops limit on RBD.", "",
  &get_iops_arguments, &execute_iops);
} // namespace qos
} // namespace action
} // namespace rbd
