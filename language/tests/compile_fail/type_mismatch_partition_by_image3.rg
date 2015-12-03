-- Copyright 2015 Stanford University
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

-- fails-with:
-- type_mismatch_partition_by_image3.rg:28: type mismatch in argument 3: expected region but got int32
--   var q = image(p, s, 0)
--               ^

import "regent"

local int1d = index_type(int, "ind1d")

task f()
  var r = region(ispace(ptr, 5), int)
  var s = region(ispace(ptr, 5), ptr(int, r))
  var p = partition(equal, s, ispace(int1d, 3))
  var q = image(p, s, 0)
end
f:compile()