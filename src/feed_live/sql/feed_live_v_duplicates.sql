select game_id, count(1)
from game_feed_live_v
group by 1
having count(1) > 1;