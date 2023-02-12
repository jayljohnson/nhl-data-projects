select abstract_game_state, count(1)
from game_feed_live_v
group by 1