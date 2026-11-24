#!/usr/bin/env python3
"""Generate the Fictional Baseball League (FBL) CSVs with PLANTED data-quality errors.

    python tools/make_fbl_data.py        -> data/fbl/teams.csv, players.csv, batting.csv

Deterministic (fixed seed). Every planted error is listed in docs/DQ_ANSWER_KEY.md.
Edit the CSVs freely afterwards: fix an error, add a new one, then re-run the pipeline.
"""
import csv
import random
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "data" / "fbl"
OUT.mkdir(parents=True, exist_ok=True)
rnd = random.Random(2024)

TEAMS = [  # team_id, name, city, league, founded, park, payroll cap (USD millions), mascot
    ("T01", "Harbor City Gulls", "Harbor City", "North", 2008, "Gull Park", 180.0, "Gus the Gull"),
    ("T02", "Ironvale Forgemen", "Ironvale", "North", 2008, "Anvil Field", 150.0, "Sparky"),
    ("T03", "Pinecrest Timberwolves", "Pinecrest", "North", 2010, "Timberline Park", 120.0, ""),
    ("T04", "Lakeshore Mariners", "Lakeshore", "North", 2012, "Mariner Yard", 140.0, "Captain Wave"),
    ("T05", "Sandhill Scorpions", "Sandhill", "South", 2008, "Dune Stadium", 160.0, "Stinger"),
    ("T06", "Redrock Coyotes", "Redrock", "South", 2010, "Canyon Park", 110.0, ""),
    ("T07", "Mesaverde Falcons", "Mesaverde", "South", 2012, "Falcon Heights", 130.0, "Talon"),
    ("T08", "Bayou Bullfrogs", "Bayou Town", "South", 2015, "Lily Pad Park", 100.0, "Ribbit"),
]
teams = [list(map(str, t)) for t in TEAMS]
teams.append(list(map(str, TEAMS[4])))                                              # T05 exact duplicate
teams.append(["T09", "Summit Peaks", "Summit", "North", "2016", "Peak Field", "125.0", "Yeti"])      # conflicting dupe #1
teams.append(["T09", "Summit Peaks", "Summit", "North", "2017", "Peak Field", "125.0", "Yeti"])      # conflicting dupe #2
teams.append(["T10", "", "Glenwood", "South", "2014", "Glen Park", "105.0", ""])                     # missing team_name
teams.append(["T11", "Future Fleet", "Newport", "North", "2099", "Wave Field", "115.0", ""])         # founded in the future
teams.append(["T12", "Coastal Kings", "Coastal", "XX", "2013", "King Field", "135.0", ""])           # invalid league
teams.append(["T13", "Broke Bears", "Bearton", "South", "2011", "Den Park", "-50.0", ""])            # negative payroll cap
teams[5][3] = "south"                                                                # T06 league lower-case (auto-correct)
with open(OUT / "teams.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["team_id", "team_name", "city", "league", "founded_year", "home_park", "payroll_cap_musd", "mascot"]); w.writerows(teams)

FIRST = ["Alex", "Blake", "Casey", "Drew", "Eli", "Finn", "Gray", "Hayden", "Ira", "Jules", "Kai", "Lane", "Max", "Noel", "Owen",
         "Parker", "Quinn", "Reese", "Sam", "Tate", "Uri", "Vic", "Wes", "Xan", "Yuri", "Zane"]
LAST = ["Armstrong", "Bishop", "Calloway", "Dalton", "Ellis", "Fowler", "Garrett", "Holloway", "Ingram", "Jennings", "Keller",
        "Lawson", "Mercer", "Norris", "Osborne", "Pruitt", "Quigley", "Rowan", "Sutton", "Thatcher", "Underhill", "Vaughn",
        "Whitfield", "Yates", "Zimmer", "Abbott", "Briggs", "Cullen", "Dunn", "Easton", "Frost", "Gibbs", "Hale", "Irwin",
        "Joyce", "Knox", "Lowell", "Marsh", "Nash", "Odell"]
POS = ["P", "C", "1B", "2B", "SS", "3B", "LF", "CF", "RF", "DH"]
base = []   # player_id, first, last, birth, bats, throws, team, pos, height, weight, nickname
for i in range(1, 41):
    pid = f"P{i:03d}"
    team = f"T{(i - 1) // 5 + 1:02d}"
    birth = f"{rnd.randint(1989, 2003)}-{rnd.randint(1, 12):02d}-{rnd.randint(1, 28):02d}"
    base.append([pid, FIRST[(i - 1) % 26], LAST[i - 1], birth, rnd.choice("LRRRB"), rnd.choice("LRRR"), team,
                 POS[(i - 1) % 10], str(rnd.randint(68, 78)), str(rnd.randint(165, 240)), ""])
base[2][10] = "Cannon"; base[9][10] = "Flash"; base[16][10] = "Doc"
by_id = {r[0]: r for r in base}
players = [list(r) for r in base]
def P(pid): return next(r for r in players if r[0] == pid)
players.append(list(P("P007")))                                    # P007 exact duplicate
P("P012")[4] = "r"                                                 # lower-case bats (auto-correct)
dup = list(P("P015")); dup[3] = "1996-07-19"; players.append(dup)  # P015 conflicting duplicate (different birth_date)
P("P023")[2] = ""                                                  # missing last_name
P("P027")[9] = "900"                                               # weight 900 (warning only)
P("P031")[4] = "Z"                                                 # invalid bats
P("P034")[8] = "12"                                                # height 12 in
P("P036")[3] = "2031-04-02"                                        # birth date in the future
P("P038")[6] = "T99"                                               # team not in team table
P("P040")[6] = "T09"                                               # team T09 is rejected (cascade)
P("P019")[9] = ""                                                  # missing weight
P("P021")[7] = "XX"                                                # invalid position
with open(OUT / "players.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["player_id", "first_name", "last_name", "birth_date", "bats", "throws", "team_id", "position", "height_in", "weight_lb", "nickname"]); w.writerows(players)

def line(pid, season, team):
    g = rnd.randint(100, 155); ab = int(g * rnd.uniform(3.0, 3.9)); h = int(ab * rnd.uniform(0.20, 0.33))
    hr = int(h * rnd.uniform(0.0, 0.25)); d = int(h * rnd.uniform(0.10, 0.25)); t = int(h * rnd.uniform(0.0, 0.05))
    return [pid, str(season), team, str(g), str(ab), str(h), str(d), str(t), str(hr), str(rnd.randint(hr, hr + 60)),
            str(rnd.randint(10, 80)), str(rnd.randint(40, 160)), str(rnd.randint(0, 25))]
bat = []
for r in base:
    for season in (2023, 2024):
        bat.append(line(r[0], season, r[6] if r[0] != "P040" else "T08"))
for pid in ("P019",): pass
def B(pid, season): return next(r for r in bat if r[0] == pid and r[1] == str(season))
# columns: 0 pid 1 season 2 team 3 g 4 ab 5 h 6 doubles 7 triples 8 hr 9 rbi 10 bb 11 so 12 sb
extra = [list(B("P001", 2024))]                                    # exact duplicate
c = list(B("P002", 2023)); c[5] = str(int(c[5]) + 7); extra.append(c)  # conflicting duplicate (different hits)
r = B("P003", 2024); r[5] = str(int(r[4]) + 5)                     # hits > at bats
r = B("P004", 2023); r[4] = "N/A"                                  # non-numeric at bats
r = B("P005", 2024); r[8] = str(int(r[5]) + 3)                     # home runs > hits (also extra-base > hits)
r = B("P006", 2023); r[8] = str(int(r[5]) // 4); r[6] = str(int(r[5]) - int(r[8]) + 2); r[7] = "0"  # extra-base hits > hits only
r = B("P008", 2024); r[3] = "190"                                  # 190 games
r = B("P009", 2023); r[3] = "10"; r[4] = "100"                     # 100 AB in 10 games
r = B("P010", 2024); r[12] = "-4"                                  # negative stolen bases
r = B("P011", 2023); r[1] = "1999"                                 # season before league existed
orphan_player = line("P999", 2024, "T01"); extra.append(orphan_player)  # player not in player table
orphan_team = line("P014", 2022, "T77"); orphan_team[1] = "2024"; extra.append(orphan_team)  # team not in team table
bat.extend(extra)
with open(OUT / "batting.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["player_id", "season", "team_id", "g", "ab", "h", "doubles", "triples", "hr", "rbi", "bb", "so", "sb"]); w.writerows(bat)
print(f"teams {len(teams)}, players {len(players)}, batting {len(bat)} rows written to {OUT}")
