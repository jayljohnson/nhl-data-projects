import pickle

seasons = range(2000, 2022)
goalie_pulled_count = 0
for season in seasons:
    with open(f'./{season}-linescore.pkl', 'rb') as f:
        games = pickle.load(f)
        for i, game in enumerate(games):
            t = game.get("teams")
            if t:
                for home_or_away, team in t.items():
                    goalie_pulled = team["goaliePulled"]
                    if goalie_pulled:
                        goalie_pulled_count += 1
                        print(f"season: {season}, game_id: {i}, home_or_away: {home_or_away}, goaliePulled: {goalie_pulled}")
print(goalie_pulled_count)