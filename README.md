# fnf-orderpoller
fusspot and foodie order poller

Executed as an AWS Lambda, polls a GMail Inbox periodically to look for new order confirmation mails from customers.
Persists the raw message into an AWS Maria DB instance
