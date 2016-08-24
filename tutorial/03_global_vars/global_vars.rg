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

import "regent"

local c = terralib.includec("stdio.h")

-- Global constants are simply Lua variables, defined at the top
-- scope. (Regent does not support mutable global variables.)
local global_constant = 4

-- Function pointers (such as to printf) may vary between nodes and runs.

task main()
  c.printf("The value of global_constant %d will always be the same\n", global_constant)
  c.printf("The function pointer to printf %p may be different on different processors\n", c.printf)
end
regentlib.start(main)
