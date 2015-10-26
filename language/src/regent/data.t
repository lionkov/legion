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

-- Data Structures

-- Elliott: The more I write in Lua, the more I feel like I'm
-- reimplementing Python piece by piece. Several limitations in Lua
-- (e.g. no __hash metamethod) can bite hard if you try to go without
-- properly engineered data structures. (Exercise for the reader: try
-- to write a fully general purpose memoize function in plain Lua.)
--
-- Note: In the code below, whenever an interface exists in plain Lua
-- (and can be hijacked with existing Lua metamethods), those
-- metamethods are used. In all other cases, the metatable is left
-- alone, and regular functions and methods are used instead. So for
-- example:
--
--     x + y        -- addition (via __add)
--     x .. y       -- concatenation (via __concat)
--     x:get(k)     -- key lookup (NOT via __index, since it's unreliable)
--     x:put(k, v)  -- key assignment (NOT via __newindex, again: unreliable)
--     x:items()    -- key, value iterator (NO __pairs in LuaJIT)
--     x:keys()     -- key iterator
--     x:values()   -- value iterator
--     data.hash(x) -- calls x:hash() if supported, otherwise returns x
--
-- This means that, for example the [] operator, and the pairs
-- function, may or may not work with a given data structure, since it
-- depends on the internal representation of that data structure.

local data = {}

-- #####################################
-- ## Hashing
-- #################

function data.hash(x)
  if type(x) == "table" and x.hash then
    return x:hash()
  else
    return x
  end
end

-- #####################################
-- ## Numbers
-- #################

function data.min(a, b)
  if a < b then
    return a
  else
    return b
  end
end

function data.max(a, b)
  if a > b then
    return a
  else
    return b
  end
end

-- #####################################
-- ## Lists
-- #################

-- The following methods work on Terra lists, or regular Lua tables
-- with 1-based consecutive numeric indices.

function data.any(list)
  for _, elt in ipairs(list) do
    if elt then
      return true
    end
  end
  return false
end

function data.all(list)
  for _, elt in ipairs(list) do
    if not elt then
      return false
    end
  end
  return true
end

function data.range(start, stop) -- zero-based, exclusive (as in Python)
  if stop == nil then
    stop = start
    start = 0
  end
  local result = terralib.newlist()
  for i = start, stop - 1, 1 do
    result:insert(i)
  end
  return result
end

function data.filter(fn, list)
  local result = terralib.newlist()
  for _, elt in ipairs(list) do
    if fn(elt) then
      result:insert(elt)
    end
  end
  return result
end

function data.filteri(fn, list)
  local result = terralib.newlist()
  for i, elt in ipairs(list) do
    if fn(elt) then
      result:insert(i)
    end
  end
  return result
end

function data.reduce(fn, list, init)
  local result = init
  for i, elt in ipairs(list) do
    if i == 1 and result == nil then
      result = elt
    else
      result = fn(result, elt)
    end
  end
  return result
end

function data.zip(...)
  local lists = terralib.newlist({...})
  local len = data.reduce(
    data.min,
    lists:map(function(list) return #list or 0 end)) or 0
  local result = terralib.newlist()
  for i = 1, len do
    result:insert(lists:map(function(list) return list[i] end))
  end
  return result
end

function data.dict(list)
  local result = {}
  for _, pair in ipairs(list) do
    result[pair[1]] = pair[2]
  end
  return result
end

-- #####################################
-- ## Tuples
-- #################

data.tuple = {}
setmetatable(data.tuple, { __index = terralib.list })
data.tuple.__index = data.tuple

function data.tuple.__eq(a, b)
  if getmetatable(a) ~= data.tuple or getmetatable(b) ~= data.tuple then
    return false
  end
  if #a ~= #b then
    return false
  end
  for i, v in ipairs(a) do
    if v ~= b[i] then
      return false
    end
  end
  return true
end

function data.tuple.__concat(a, b)
  assert(data.is_tuple(a) and (not b or data.is_tuple(b)))
  local result = data.newtuple()
  result:insertall(a)
  if not b then
    return result
  end
  result:insertall(b)
  return result
end

function data.tuple:slice(start --[[ inclusive ]], stop --[[ inclusive ]])
  local result = data.newtuple()
  for i = start, stop do
    result:insert(self[i])
  end
  return result
end

function data.tuple:starts_with(t)
  assert(data.is_tuple(t))
  return self:slice(1, data.min(#self, #t)) == t
end

function data.tuple:__tostring()
  return "<" .. self:hash() .. ">"
end

function data.tuple:hash()
  return self:mkstring(".")
end

function data.newtuple(...)
  return setmetatable({...}, data.tuple)
end

function data.is_tuple(x)
  return getmetatable(x) == data.tuple
end

return data
