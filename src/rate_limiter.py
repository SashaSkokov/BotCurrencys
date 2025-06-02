from collections import defaultdict, deque
from datetime import datetime, timedelta
import logging

class RateLimiter:
    def __init__(self, limit: int, period: int):
        self.limit = limit
        self.period = period
        self.requests = defaultdict(deque)
        self.logger = logging.getLogger(__name__)

    def can_process(self, user_id: int):
        now = datetime.now()
        
        while self.requests[user_id] and (now - self.requests[user_id][0]).total_seconds() > self.period:
            self.requests[user_id].popleft()

        if len(self.requests[user_id]) >= self.limit:
            first_request = self.requests[user_id][0]
            remaining_time = (first_request + timedelta(seconds=self.period)) - now
            seconds = remaining_time.total_seconds()
            minutes, seconds = divmod(int(seconds), 60)
            return False, f"{minutes} минут {seconds} секунд"

        self.logger.warning(self.requests[user_id])
        self.requests[user_id].append(now)
        return True, None
