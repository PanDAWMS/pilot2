# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Giampaolo Rodola, g.rodola@gmail.com, 2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017

import os
import collections

_ntuple_diskusage = collections.namedtuple('usage', 'total used free')

if hasattr(os, 'statvfs'):  # POSIX
    def disk_usage(path):
        st = os.statvfs(path)
        free = st.f_bavail * st.f_frsize
        total = st.f_blocks * st.f_frsize
        used = (st.f_blocks - st.f_bfree) * st.f_frsize
        return _ntuple_diskusage(total, used, free)

else:
    def disk_usage(path):
        return _ntuple_diskusage(0, 0, 0)
#    raise NotImplementedError("platform not supported")

disk_usage.__doc__ = """
Return disk usage statistics about the given path as a (total, used, free)
namedtuple.  Values are expressed in bytes.
"""
