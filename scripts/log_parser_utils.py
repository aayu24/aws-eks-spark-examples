import re
from user_agents import parse

LOG_FORMAT = re.compile(r'^(?P<ip>\S+) \S+ \S+ \[(?P<datetime>.*?)\] "(?P<request>.*?)" (?P<status>\d{3}) (?P<size>\S+) "(?P<referrer>[^"]*)" "(?P<useragent>[^"]*)"')

def parse_log_line(logline):
    match = LOG_FORMAT.match(logline)
    if match is None:
        print(f"Given {logline} logline could not be parsed")
        return None
    return match.groupdict()

def extract_device(ua_string):
    user_agent = parse(ua_string)
    if user_agent.is_mobile:
        return "Mobile"
    elif user_agent.is_tablet:
        return "Tablet"
    elif user_agent.is_pc:
        return "PC"
    elif user_agent.is_bot:
        return "Bot"
    elif user_agent.is_email_client:
        return "EmailClient"
    else:
        return "NA"