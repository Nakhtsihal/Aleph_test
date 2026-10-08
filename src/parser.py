import json
def parse_regular_log(log):
    parsed_log = {
        "audit_id": log.get("AuditID"),
        "action": log.get("Action"),
        "timestamp": log.get("timestamp"),
        "client_ip": log.get("ipadress"),
        "browser": log.get("Browser"),
        "os": log.get("os"),
        "success": log.get("succses"),
        "error_message": log.get("error_message"),
    }
    return parsed_log

def parse_syslog(syslog):
    index = syslog.find('"AuditID"') # находим индекс, который разделяет файл на две части. Берём вторую часть и приводим данные в порядок
    payload_string = "{" + syslog[index:] + "}"
    payload = json.loads(payload_string)
    return parse_regular_log(payload)

# read json, take logs
with open("../data/ha_logs.json") as file:
    ha_logs = json.load(file)
    prsd_logs = parse_regular_log(ha_logs[0])
#read json, take syslogs
with open("../data/ha_syslogs.json") as file:
    ha_syslogs = json.load(file)
    syslog = ha_syslogs[0]




parsed_syslogs = []
for log in ha_syslogs:
    parsed_syslog = parse_syslog(log)
    parsed_syslogs.append(parsed_syslog)


parsed_logs = []
for log in ha_logs:
    parsed_log = parse_regular_log(log)
    parsed_logs.append(parsed_log)
# print(len(parsed_logs))
# print(parsed_logs[0])

parsed_syslog = parse_syslog(syslog)
parsed_syslog = parse_syslog(ha_syslogs[0])
