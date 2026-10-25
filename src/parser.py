import json
from typing import Any


def parse_regular_log(log: dict[str, Any]) -> dict[str, Any]:
    parsed_log = {
        "audit_id": log.get("AuditID"),
        "action": log.get("Action"),
        "source": log.get("sourceofdata"),
        "severity": log.get("Severity"),
        "timestamp": log.get("timestamp"),
        "client_ip": log.get("ClientIP") or log.get("ipadress"),
        "message": log.get("Messages") or log.get("details"),
        "browser": log.get("Browser"),
        "os": log.get("os"),
        "success": log.get("succses"),
        "error_message": log.get("error_message"),
    }
    return parsed_log


def parse_syslog(syslog: str) -> dict[str, Any]:
    index = syslog.find('"AuditID"')
    header = syslog[:index].strip()
    parts = header.split()
    source = parts[-2]
    severity = parts[-1].rstrip(":").lower()
    payload_string = "{" + syslog[index:] + "}"
    payload = json.loads(payload_string)
    parsed_log = parse_regular_log(payload)
    parsed_log["source"] = source
    parsed_log["severity"] = severity
    return parsed_log
