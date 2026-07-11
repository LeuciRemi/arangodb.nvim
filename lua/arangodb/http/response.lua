--- HTTP response parsing helpers shared by the available transports.
local M = {}

local function read_line(text, offset)
  local crlf = text:find("\r\n", offset, true)
  local lf = text:find("\n", offset, true)

  if crlf and (not lf or crlf < lf) then
    return text:sub(offset, crlf - 1), crlf + 2
  end
  if lf then
    return text:sub(offset, lf - 1), lf + 1
  end
end

local function decode_chunked(body)
  local chunks = {}
  local offset = 1

  while true do
    local line, next_offset = read_line(body, offset)
    if not line then
      error("Invalid chunked response from ArangoDB")
    end

    local size_text = line:match("^%s*([0-9A-Fa-f]+)%s*$") or line:match("^%s*([0-9A-Fa-f]+)%s*;.*$")
    local size = size_text and tonumber(size_text, 16) or nil
    if not size then
      error("Invalid chunk size in ArangoDB response")
    end

    offset = next_offset
    if size == 0 then
      break
    end

    local chunk = body:sub(offset, offset + size - 1)
    if #chunk < size then
      error("Truncated chunked response from ArangoDB")
    end

    chunks[#chunks + 1] = chunk
    offset = offset + size

    if body:sub(offset, offset + 1) == "\r\n" then
      offset = offset + 2
    elseif body:sub(offset, offset) == "\n" then
      offset = offset + 1
    else
      error("Invalid chunk separator in ArangoDB response")
    end
  end

  return table.concat(chunks)
end

--- Parse a raw HTTP response into status, headers, and body fields.
function M.parse(raw)
  if type(raw) ~= "string" then
    error("Invalid HTTP response from ArangoDB")
  end

  local status
  local headers
  local body

  while true do
    local header_end = raw:find("\r\n\r\n", 1, true)
    local separator_len = 4

    if not header_end then
      header_end = raw:find("\n\n", 1, true)
      separator_len = 2
    end

    if not header_end then
      error("Invalid HTTP response from ArangoDB")
    end

    local head = raw:sub(1, header_end - 1)
    body = raw:sub(header_end + separator_len)
    local delimiter = head:find("\r\n", 1, true) and "\r\n" or "\n"
    local lines = vim.split(head, delimiter, { plain = true, trimempty = false })
    status = tonumber((lines[1] or ""):match("^HTTP/%d+%.?%d*%s+(%d%d%d)"))

    if not status then
      error("Invalid HTTP status line from ArangoDB: " .. (lines[1] or ""))
    end

    headers = {}
    for index = 2, #lines do
      local name, value = lines[index]:match("^([^:]+):%s*(.*)$")
      if name then
        headers[name:lower()] = value
      end
    end

    if status < 100 or status >= 200 or status == 101 then
      break
    end

    raw = body
  end

  local transfer_encoding = headers["transfer-encoding"]
  if transfer_encoding and transfer_encoding:lower():find("chunked", 1, true) then
    body = decode_chunked(body)
  else
    local content_length_header = headers["content-length"]
    local content_length = tonumber(content_length_header)
    if content_length_header ~= nil then
      if not content_length or content_length < 0 or content_length % 1 ~= 0 then
        error("Invalid HTTP content length from ArangoDB")
      end
      if #body < content_length then
        error("Truncated HTTP response from ArangoDB")
      end
      body = body:sub(1, content_length)
    end
  end

  return {
    status = status,
    headers = headers,
    body = body,
  }
end

return M
