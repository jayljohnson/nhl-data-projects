drop view if exists game_feed_live_v;

create view game_feed_live_v as
    select
        game_id,
        details ->> '$.link' as endpoint,
        details ->> '$.gameData.status.abstractGameState' as abstract_game_state
    from game_feed_live;
