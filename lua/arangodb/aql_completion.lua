--- Omni completion for AQL query buffers.
local M = {}

local function word_list(text)
  return vim.split(text, "%s+", { trimempty = true })
end

-- Keyword lists mirror syntax/aql.vim (aqlKeyword, aqlModification, aqlBoolean, aqlOperator).
local keywords = word_list([[
  FOR LET FILTER SEARCH SORT ASC DESC LIMIT COLLECT AGGREGATE RETURN DISTINCT WINDOW WITH INTO KEEP COUNT
  OPTIONS PRUNE GRAPH SHORTEST_PATH K_SHORTEST_PATH K_PATHS ALL_SHORTEST_PATHS OUTBOUND INBOUND
  INSERT UPDATE REPLACE REMOVE UPSERT
  true false null
  AND OR NOT IN LIKE ALL NONE ANY AT LEAST
]])

-- Built-in AQL function names of ArangoDB 3.12, grouped like the reference documentation.
local functions = word_list([[
  CHAR_LENGTH CONCAT CONCAT_SEPARATOR CONTAINS CRC32 ENCODE_URI_COMPONENT FIND_FIRST FIND_LAST FNV64
  IPV4_FROM_NUMBER IPV4_TO_NUMBER IS_IPV4 JSON_PARSE JSON_STRINGIFY LEFT LENGTH LEVENSHTEIN_DISTANCE LIKE LOWER
  LTRIM MD5 NGRAM_POSITIONAL_SIMILARITY NGRAM_SIMILARITY RANDOM_TOKEN REGEX_MATCHES REGEX_REPLACE REGEX_SPLIT
  REGEX_TEST REPEAT REVERSE RIGHT RTRIM SHA1 SHA256 SHA512 SOUNDEX SPLIT STARTS_WITH SUBSTITUTE SUBSTRING
  SUBSTRING_BYTES TO_BASE64 TO_CHAR TO_HEX TOKENS TRIM UPPER UUID
  ABS ACOS ASIN ATAN ATAN2 AVERAGE AVG CEIL COS COSINE_SIMILARITY DECAY_EXP DECAY_GAUSS DECAY_LINEAR DEGREES EXP
  EXP2 FLOOR L1_DISTANCE L2_DISTANCE LOG LOG2 LOG10 MAX MEDIAN MIN PERCENTILE PI POW PRODUCT RADIANS RAND RANDOM
  RANGE ROUND SIN SQRT STDDEV STDDEV_POPULATION STDDEV_SAMPLE SUM TAN VARIANCE VARIANCE_POPULATION VARIANCE_SAMPLE
  APPEND CONTAINS_ARRAY COUNT_DISTINCT COUNT_UNIQUE FIRST FLATTEN INTERLEAVE INTERSECTION JACCARD LAST MINUS NTH
  OUTERSECTION POP POSITION PUSH REMOVE_NTH REMOVE_VALUE REMOVE_VALUES REPLACE_NTH SHIFT SLICE SORTED SORTED_UNIQUE
  UNION UNION_DISTINCT UNIQUE UNSHIFT
  ATTRIBUTES ENTRIES HAS IS_SAME_COLLECTION KEEP KEEP_RECURSIVE KEYS MATCHES MERGE MERGE_RECURSIVE PARSE_COLLECTION
  PARSE_IDENTIFIER PARSE_KEY TRANSLATE UNSET UNSET_RECURSIVE VALUE VALUES ZIP
  DATE_ADD DATE_COMPARE DATE_DAY DATE_DAYOFWEEK DATE_DAYOFYEAR DATE_DAYS_IN_MONTH DATE_DIFF DATE_FORMAT DATE_HOUR
  DATE_ISO8601 DATE_ISOWEEK DATE_ISOWEEKYEAR DATE_LEAPYEAR DATE_LOCALTOUTC DATE_MILLISECOND DATE_MINUTE DATE_MONTH
  DATE_NOW DATE_QUARTER DATE_ROUND DATE_SECOND DATE_SUBTRACT DATE_TIMESTAMP DATE_TIMEZONE DATE_TIMEZONES DATE_TRUNC
  DATE_UTCTOLOCAL DATE_YEAR
  TO_ARRAY TO_BOOL TO_LIST TO_NUMBER TO_STRING IS_ARRAY IS_BOOL IS_DATESTRING IS_DOCUMENT IS_KEY IS_LIST IS_NULL
  IS_NUMBER IS_OBJECT IS_STRING TYPENAME
  DISTANCE GEO_AREA GEO_CONTAINS GEO_DISTANCE GEO_EQUALS GEO_INTERSECTS GEO_IN_RANGE GEO_LINESTRING
  GEO_MULTILINESTRING GEO_MULTIPOINT GEO_MULTIPOLYGON GEO_POINT GEO_POLYGON IS_IN_POLYGON NEAR WITHIN
  WITHIN_RECTANGLE
  ANALYZER BM25 BOOST EXISTS IN_RANGE LEVENSHTEIN_MATCH MINHASH_MATCH MIN_MATCH NGRAM_MATCH OFFSET_INFO PHRASE TFIDF
  FULLTEXT
  APPLY ASSERT CALL CHECK_DOCUMENT COLLECTION_COUNT COLLECTIONS CURRENT_DATABASE CURRENT_USER DECODE_REV DOCUMENT
  FAIL FIRST_DOCUMENT FIRST_LIST HASH MINHASH MINHASH_COUNT MINHASH_ERROR NOOPT NOT_NULL PASSTHRU SCHEMA_GET
  SCHEMA_VALIDATE SHARD_ID SLEEP V8 VERSION WARN
]])

-- Collection names loaded asynchronously per connection, so completion never waits on the network.
-- Every call revalidates through the client, whose own cache expires and is invalidated by writes.
local collections = {}
local pending = {}

local function scope_key(config)
  return table.concat({
    tostring(config.scheme or ""),
    tostring(config.host or ""),
    tostring(config.port or ""),
    tostring(config.database or ""),
    tostring(config.user or ""),
  }, "\1")
end

local function cached_collections(config)
  local key = scope_key(config)
  if pending[key] then
    return collections[key]
  end
  pending[key] = true
  local ok = pcall(require("arangodb.client").list_collections_async, config, function(err, listed)
    pending[key] = nil
    if not err and type(listed) == "table" then
      collections[key] = listed
    end
  end)
  if not ok then
    pending[key] = nil
  end
  return collections[key]
end

local function session_config(buf)
  local session = require("arangodb.aql").session(buf)
  if session and type(session.config) == "table" then
    return session.config
  end
end

local function identifier_start(line, col)
  local start = col - 1
  while start > 0 do
    if not line:sub(start, start):match("[%w_]") then
      break
    end
    start = start - 1
  end
  return start
end

--- Complete AQL keywords, built-in functions, and collection names of the session database.
function M.omnifunc(findstart, base)
  if findstart == 1 then
    return identifier_start(vim.api.nvim_get_current_line(), vim.fn.col("."))
  end

  local buf = vim.api.nvim_get_current_buf()
  if vim.b[buf].arangodb_aql_bind_vars == true then
    return {}
  end

  local prefix = (base or ""):lower()
  local items = {}
  local seen = {}
  local function add(word, kind)
    if seen[word] then
      return
    end
    seen[word] = true
    if prefix == "" or word:lower():sub(1, #prefix) == prefix then
      items[#items + 1] = { word = word, kind = kind }
    end
  end

  for _, word in ipairs(keywords) do
    add(word, "k")
  end
  for _, word in ipairs(functions) do
    add(word, "f")
  end

  local config = session_config(buf)
  local names = config and cached_collections(config)
  if names then
    for _, name in ipairs(names) do
      if prefix == "" or name:lower():sub(1, #prefix) == prefix then
        items[#items + 1] = { word = name, kind = "c" }
      end
    end
  end

  return items
end

return M
