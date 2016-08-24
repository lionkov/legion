-- Copyright 2016 Stanford University
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- runs-with:
-- []

-- FIXME:

import "regent"

local c = regentlib.c

task f(s0 : region(int), s1 : region(int))
where
  reads writes simultaneous(s0),
  reads writes simultaneous(s1)
do
end

task main()
  var s = region(ispace(ptr, 5), int)
  var y0 = new(ptr(int, s))
  var y1 = new(ptr(int, s))
  var y2 = new(ptr(int, s))

  var rc = c.legion_coloring_create()
  c.legion_coloring_add_point(rc, 0, __raw(y0))
  c.legion_coloring_add_point(rc, 1, __raw(y1))
  c.legion_coloring_add_point(rc, 2, __raw(y2))
  var p1 = partition(disjoint, s, rc)
  var p2 = partition(aliased, s, rc)
  c.legion_coloring_destroy(rc)

  __demand(__parallel)
  for idx = 0, 1 do
    f(p1[0], p1[1])
  end

  __demand(__parallel)
  for idx = 0, 1 do
    f(p2[0], p2[1])
  end
end
regentlib.start(main)
