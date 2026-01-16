from datetime import datetime, timedelta
from typing import List

def daterange(start: str, end: str, fmt: str = "%Y-%m-%d") -> List[str]:
    d0 = datetime.strptime(start, fmt).date()
    d1 = datetime.strptime(end, fmt).date()
    if d1 < d0:
        raise ValueError("partition-end anterior ao partition-start.")
    out, d = [], d0
    while d <= d1:
        out.append(d.strftime(fmt))
        d += timedelta(days=1)
    return out

def csv_to_list(s: str) -> List[str]:
    return [x.strip() for x in s.split(",") if x.strip()]
