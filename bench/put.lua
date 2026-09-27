wrk.method = "PUT"
wrk.body = string.rep("x", 5760)
wrk.headers["Content-Type"] = "application/octet-stream"
