import json
from typing import Any
from abc import ABC, abstractmethod


class ParserInterface(ABC):
    @abstractmethod
    def parse(self, log: Any) -> dict[str, Any]:
        raise NotImplementedError()


class RegularLogParser(ParserInterface):
    def parse(self, log: dict[str, Any]) -> dict[str, Any]:
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
            "error_message": log.get("error_message")
        }
        return parsed_log


class SysLogParser(ParserInterface):
    def __init__(self) -> None:
        self._regular_parser = RegularLogParser()

    def parse(self, log: str) -> dict[str, Any]:
        index = log.find('"AuditID"')

        # The log must contain AuditID.
        if index == -1:
            raise ValueError("AuditID not found")

        header = log[:index].strip()
        parts = header.split()

        if len(parts) < 2:
            raise ValueError("Invalid syslog header")

        source = parts[-2]
        severity = parts[-1].rstrip(":").lower()

        payload_string = "{" + log[index:] + "}"
        payload = json.loads(payload_string)

        parsed_log = self._regular_parser.parse(payload)
        parsed_log["source"] = source
        parsed_log["severity"] = severity

        return parsed_log
