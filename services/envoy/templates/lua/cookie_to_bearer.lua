-- Moves the HttpOnly access-token cookie set by the Auth bridge into an
-- Authorization header for the token-exchange filter. Rendered by envsubst.
local ACCESS_COOKIE = "${AUTH_ACCESS_COOKIE}"
local COOKIE_PREFIX = "${AUTH_COOKIE_PREFIX}"
local APP_ORIGIN = "${APP_ORIGIN}"
local SAFE_METHODS = { GET = true, HEAD = true, OPTIONS = true }

-- Returns whether any Auth cookie is present and the access token value.
local function read_cookies(header)
  local has_auth_cookie, token = false, nil
  for pair in string.gmatch(header, "[^;]+") do
    local name, value = string.match(pair, "^%s*([^=%s]+)=(.*)$")
    if name then
      if string.sub(name, 1, #COOKIE_PREFIX) == COOKIE_PREFIX then has_auth_cookie = true end
      if name == ACCESS_COOKIE then token = value end
    end
  end
  return has_auth_cookie, token
end

function envoy_on_request(handle)
  local headers = handle:headers()
  local cookie = headers:get("cookie")
  local has_auth_cookie, token = false, nil
  if cookie then has_auth_cookie, token = read_cookies(cookie) end
  -- Cookies never reach the gRPC services.
  headers:remove("cookie")

  if headers:get("authorization") then
    if has_auth_cookie then
      handle:respond({ [":status"] = "400" }, "ambiguous_credentials")
    end
    return
  end
  -- Without a token the token-exchange filter answers 401, which triggers the SPA refresh.
  if not token or token == "" then return end

  -- Cross-origin pages cannot send a custom header without a CORS preflight, and none is allowed.
  if not SAFE_METHODS[headers:get(":method")]
    and (headers:get("origin") ~= APP_ORIGIN or (headers:get("x-csrf-token") or "") == "") then
    handle:respond({ [":status"] = "403" }, "csrf_failed")
    return
  end
  headers:replace("authorization", "Bearer " .. token)
end
