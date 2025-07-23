wrk.method = "POST"
wrk.body = '{"correlationId": "4a7901b8-7d26-4d9d-aa19-4dc1c7cf60b3", "amount": 19.90}'
wrk.headers["Content-Type"] = "application/json"

-- Log responses
response = function(status, headers, body)
	if status ~= 200 then
		print("Status: " .. status)
		print("Body: " .. body)
		print("---")
	end
end

-- Log request details
request = function()
	print("Sending request ")
	return wrk.format(wrk.method, wrk.path, wrk.headers, wrk.body)
end
