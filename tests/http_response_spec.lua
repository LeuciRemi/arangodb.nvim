local h = require("tests.helpers")
local response = require("arangodb.http.response")

return {
  h.test("HTTP responses honor content length", function()
    local parsed = response.parse("HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-Test: yes\r\n\r\nhelloignored")
    h.eq(200, parsed.status)
    h.eq("yes", parsed.headers["x-test"])
    h.eq("hello", parsed.body)
  end),

  h.test("HTTP responses decode chunks and extensions", function()
    local parsed = response.parse(
      "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5;name=value\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"
    )
    h.eq("hello world", parsed.body)
  end),

  h.test("HTTP parser skips informational responses", function()
    local parsed = response.parse("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\n{}")
    h.eq(201, parsed.status)
    h.eq("{}", parsed.body)
  end),

  h.test("HTTP parser rejects truncated bodies", function()
    h.fails("Truncated HTTP response", function()
      response.parse("HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort")
    end)
  end),

  h.test("HTTP parser rejects malformed framing", function()
    h.fails("Invalid chunk size", function()
      response.parse("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5garbage\r\nhello\r\n0\r\n\r\n")
    end)
    h.fails("Invalid HTTP content length", function()
      response.parse("HTTP/1.1 200 OK\r\nContent-Length: -1\r\n\r\n")
    end)
  end),
}
