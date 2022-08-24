import requests
import pickle


with open('data/teams/teams.pkl', 'wb') as f:
    endpoint = "https://statsapi.web.nhl.com/api/v1/teams"
    result = requests.get(url=endpoint).json()
    pickle.dump(result, f, pickle.HIGHEST_PROTOCOL)

with open('data/teams/teams_single.pkl', 'wb') as f:
    team_ids = range(1,200)
    results = {}
    for id in team_ids:
        endpoint = f"https://statsapi.web.nhl.com/api/v1/teams/{id}"
        result = requests.get(url=endpoint).json()
        team = result.get("teams")
        if result == {'messageNumber': 10, 'message': 'Object not found'}:
            if id < 100:
                pass
            else:
                break
        elif team:
            results[id] = result.get("teams")[0]
        else:
            raise Exception("Unhandled teams scenario")
    print(f"Finished getting team info, id: {id}")

    pickle.dump(results, f, pickle.HIGHEST_PROTOCOL)
