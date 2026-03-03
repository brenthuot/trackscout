"""
Roster Hometown Scraper v2
Scrapes official D1 college roster pages for real athlete hometowns,
then updates Supabase — filling null/empty values (or all with --all).

Uses Playwright (headless Chromium) since many Sidearm sites lazy-render via JS.

Usage:
    python roster_scraper.py                     # all schools, skip existing
    python roster_scraper.py --all               # overwrite existing hometowns
    python roster_scraper.py --conf ACC          # one conference only
    python roster_scraper.py --school Syracuse   # one school only
    python roster_scraper.py --dry-run           # parse + match, no DB writes
    python roster_scraper.py --limit 5           # first N pages only (testing)
"""

import os, re, time, logging, argparse, unicodedata
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

PAGE_DELAY   = 1.0   # between page fetches (sleep inside scrape_page adds 3.5s+)
SCHOOL_DELAY = 0.5   # extra pause between schools

STATE_ABBR = {
    "Alabama":"AL","Alaska":"AK","Arizona":"AZ","Arkansas":"AR","California":"CA",
    "Colorado":"CO","Connecticut":"CT","Delaware":"DE","Florida":"FL","Georgia":"GA",
    "Hawaii":"HI","Idaho":"ID","Illinois":"IL","Indiana":"IN","Iowa":"IA","Kansas":"KS",
    "Kentucky":"KY","Louisiana":"LA","Maine":"ME","Maryland":"MD","Massachusetts":"MA",
    "Michigan":"MI","Minnesota":"MN","Mississippi":"MS","Missouri":"MO","Montana":"MT",
    "Nebraska":"NE","Nevada":"NV","New Hampshire":"NH","New Jersey":"NJ",
    "New Mexico":"NM","New York":"NY","North Carolina":"NC","North Dakota":"ND",
    "Ohio":"OH","Oklahoma":"OK","Oregon":"OR","Pennsylvania":"PA","Rhode Island":"RI",
    "South Carolina":"SC","South Dakota":"SD","Tennessee":"TN","Texas":"TX","Utah":"UT",
    "Vermont":"VT","Virginia":"VA","Washington":"WA","West Virginia":"WV",
    "Wisconsin":"WI","Wyoming":"WY","District of Columbia":"DC",
    **{v:v for v in [
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID",
        "IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS",
        "MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK",
        "OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV",
        "WI","WY","DC"]},
}

# Subset of STATE_ABBR: full state name → 2-letter code (used for pattern building)
FULL_TO_ABBR = {k: v for k, v in STATE_ABBR.items() if len(k) > 2}

EXPAND_ABBR = {
    "Ala":"Alabama","Ariz":"Arizona","Ark":"Arkansas","Calif":"California",
    "Colo":"Colorado","Conn":"Connecticut","Del":"Delaware","Fla":"Florida",
    "Ga":"Georgia","Ill":"Illinois","Ind":"Indiana","Kan":"Kansas","Ky":"Kentucky",
    "La":"Louisiana","Mass":"Massachusetts","Md":"Maryland","Mich":"Michigan",
    "Minn":"Minnesota","Miss":"Mississippi","Mo":"Missouri","Mont":"Montana",
    "Neb":"Nebraska","Nev":"Nevada","N.H":"New Hampshire","N.J":"New Jersey",
    "N.M":"New Mexico","N.Y":"New York","N.C":"North Carolina","N.D":"North Dakota",
    "Okla":"Oklahoma","Ore":"Oregon","Pa":"Pennsylvania","R.I":"Rhode Island",
    "S.C":"South Carolina","S.D":"South Dakota","Tenn":"Tennessee","Tex":"Texas",
    "Va":"Virginia","Wash":"Washington","W.Va":"West Virginia","Wis":"Wisconsin",
    "Wyo":"Wyoming","D.C":"District of Columbia",
}

ROSTERS = [
    {"school":"Boston College",   "conf":"ACC","gender":"M","url":"https://bceagles.com/sports/mens-track-and-field/roster"},
    {"school":"Boston College",   "conf":"ACC","gender":"F","url":"https://bceagles.com/sports/womens-track-and-field/roster"},
    {"school":"California",       "conf":"ACC","gender":"M","url":"https://calbears.com/sports/track-and-field/roster#men"},
    {"school":"California",       "conf":"ACC","gender":"F","url":"https://calbears.com/sports/track-and-field/roster#women"},
    {"school":"Clemson",          "conf":"ACC","gender":"B","url":"https://clemsontigers.com/sports/track-field/roster/"},
    {"school":"Duke",             "conf":"ACC","gender":"B","url":"https://goduke.com/sports/track-and-field/roster"},
    {"school":"Florida State",    "conf":"ACC","gender":"M","url":"https://seminoles.com/sports/mens-track-and-field/roster"},
    {"school":"Florida State",    "conf":"ACC","gender":"F","url":"https://seminoles.com/sports/womens-track-and-field/roster"},
    {"school":"Georgia Tech",     "conf":"ACC","gender":"M","url":"https://ramblinwreck.com/sports/m-track/roster/"},
    {"school":"Georgia Tech",     "conf":"ACC","gender":"F","url":"https://ramblinwreck.com/sports/w-track/roster/"},
    {"school":"Louisville",       "conf":"ACC","gender":"B","url":"https://gocards.com/sports/track-and-field/roster"},
    {"school":"Miami",            "conf":"ACC","gender":"B","url":"https://miamihurricanes.com/sports/track/roster/"},
    {"school":"NC State",         "conf":"ACC","gender":"B","url":"https://gopack.com/sports/track-and-field/roster"},
    {"school":"North Carolina",   "conf":"ACC","gender":"B","url":"https://goheels.com/sports/track-and-field/roster"},
    {"school":"Notre Dame",       "conf":"ACC","gender":"B","url":"https://und.com/sports/track/roster/"},
    {"school":"Pittsburgh",       "conf":"ACC","gender":"B","url":"https://pittsburghpanthers.com/sports/track-and-field/roster"},
    {"school":"SMU",              "conf":"ACC","gender":"F","url":"https://smumustangs.com/sports/womens-track-and-field/roster"},
    {"school":"Stanford",         "conf":"ACC","gender":"B","url":"https://gostanford.com/sports/track-field/roster"},
    {"school":"Syracuse",         "conf":"ACC","gender":"M","url":"https://cuse.com/sports/mens-track-and-field/roster"},
    {"school":"Syracuse",         "conf":"ACC","gender":"F","url":"https://cuse.com/sports/womens-track-and-field/roster"},
    {"school":"Virginia",         "conf":"ACC","gender":"B","url":"https://virginiasports.com/sports/xctrack/roster"},
    {"school":"Virginia Tech",    "conf":"ACC","gender":"B","url":"https://hokiesports.com/sports/track-field/roster"},
    {"school":"Wake Forest",      "conf":"ACC","gender":"B","url":"https://godeacs.com/sports/track-and-field/roster"},
    {"school":"Illinois",         "conf":"Big Ten","gender":"M","url":"https://fightingillini.com/sports/mens-track-and-field/roster"},
    {"school":"Illinois",         "conf":"Big Ten","gender":"F","url":"https://fightingillini.com/sports/womens-track-and-field/roster"},
    {"school":"Indiana",          "conf":"Big Ten","gender":"B","url":"https://iuhoosiers.com/sports/track-and-field/roster"},
    {"school":"Iowa",             "conf":"Big Ten","gender":"M","url":"https://hawkeyesports.com/sports/mtrack/roster"},
    {"school":"Iowa",             "conf":"Big Ten","gender":"F","url":"https://hawkeyesports.com/sports/wtrack/roster"},
    {"school":"Maryland",         "conf":"Big Ten","gender":"B","url":"https://umterps.com/sports/track-and-field/roster"},
    {"school":"Michigan",         "conf":"Big Ten","gender":"M","url":"https://mgoblue.com/sports/mens-track-and-field/roster"},
    {"school":"Michigan",         "conf":"Big Ten","gender":"F","url":"https://mgoblue.com/sports/womens-track-and-field/roster"},
    {"school":"Michigan State",   "conf":"Big Ten","gender":"B","url":"https://msuspartans.com/sports/track-and-field/roster"},
    {"school":"Minnesota",        "conf":"Big Ten","gender":"M","url":"https://gophersports.com/sports/mens-track-and-field/roster"},
    {"school":"Minnesota",        "conf":"Big Ten","gender":"F","url":"https://gophersports.com/sports/womens-track-and-field/roster"},
    {"school":"Nebraska",         "conf":"Big Ten","gender":"B","url":"https://huskers.com/sports/track-and-field/roster"},
    {"school":"Northwestern",     "conf":"Big Ten","gender":"F","url":"https://nusports.com/sports/womens-cross-country/roster"},
    {"school":"Ohio State",       "conf":"Big Ten","gender":"M","url":"https://ohiostatebuckeyes.com/sports/mens-track-field/roster"},
    {"school":"Ohio State",       "conf":"Big Ten","gender":"F","url":"https://ohiostatebuckeyes.com/sports/womens-track-field/roster"},
    {"school":"Oregon",           "conf":"Big Ten","gender":"B","url":"https://goducks.com/sports/track-and-field/roster"},
    {"school":"Penn State",       "conf":"Big Ten","gender":"B","url":"https://gopsusports.com/sports/track-field/roster"},
    {"school":"Purdue",           "conf":"Big Ten","gender":"B","url":"https://purduesports.com/sports/track-field/roster"},
    {"school":"Rutgers",          "conf":"Big Ten","gender":"B","url":"https://scarletknights.com/sports/track-and-field/roster"},
    {"school":"UCLA",             "conf":"Big Ten","gender":"B","url":"https://uclabruins.com/sports/track-and-field/roster"},
    {"school":"USC",              "conf":"Big Ten","gender":"B","url":"https://usctrojans.com/sports/track-and-field/roster"},
    {"school":"Washington",       "conf":"Big Ten","gender":"B","url":"https://gohuskies.com/sports/track-and-field/roster"},
    {"school":"Wisconsin",        "conf":"Big Ten","gender":"M","url":"https://uwbadgers.com/sports/mens-track-and-field/roster"},
    {"school":"Wisconsin",        "conf":"Big Ten","gender":"F","url":"https://uwbadgers.com/sports/womens-track-and-field/roster"},
    {"school":"Arizona",          "conf":"Big 12","gender":"B","url":"https://arizonawildcats.com/sports/track-and-field/roster"},
    {"school":"Arizona State",    "conf":"Big 12","gender":"B","url":"https://thesundevils.com/sports/track-field/roster"},
    {"school":"Baylor",           "conf":"Big 12","gender":"B","url":"https://baylorbears.com/sports/track-and-field/roster"},
    {"school":"BYU",              "conf":"Big 12","gender":"M","url":"https://byucougars.com/sports/mens-track-and-field/roster"},
    {"school":"BYU",              "conf":"Big 12","gender":"F","url":"https://byucougars.com/sports/womens-track-and-field/roster"},
    {"school":"UCF",              "conf":"Big 12","gender":"F","url":"https://ucfknights.com/sports/track-and-field/roster"},
    {"school":"Cincinnati",       "conf":"Big 12","gender":"B","url":"https://gobearcats.com/sports/track-field/roster"},
    {"school":"Colorado",         "conf":"Big 12","gender":"B","url":"https://cubuffs.com/sports/track-and-field/roster"},
    {"school":"Houston",          "conf":"Big 12","gender":"B","url":"https://uhcougars.com/sports/track-and-field/roster"},
    {"school":"Iowa State",       "conf":"Big 12","gender":"B","url":"https://cyclones.com/sports/track-and-field/roster"},
    {"school":"Kansas",           "conf":"Big 12","gender":"B","url":"https://kuathletics.com/sports/track-and-field/roster"},
    {"school":"Kansas State",     "conf":"Big 12","gender":"B","url":"https://kstatesports.com/sports/track-and-field/roster"},
    {"school":"Oklahoma State",   "conf":"Big 12","gender":"M","url":"https://okstate.com/sports/mxct/roster"},
    {"school":"Oklahoma State",   "conf":"Big 12","gender":"F","url":"https://okstate.com/sports/womens-cross-country-track/roster"},
    {"school":"TCU",              "conf":"Big 12","gender":"M","url":"https://gofrogs.com/sports/mens-track-and-field/roster"},
    {"school":"TCU",              "conf":"Big 12","gender":"F","url":"https://gofrogs.com/sports/womens-track-and-field/roster"},
    {"school":"Texas Tech",       "conf":"Big 12","gender":"B","url":"https://texastech.com/sports/track-and-field/roster"},
    {"school":"Utah",             "conf":"Big 12","gender":"B","url":"https://utahutes.com/sports/track-and-field/roster"},
    {"school":"West Virginia",    "conf":"Big 12","gender":"F","url":"https://wvusports.com/sports/womens-track-and-field/roster"},
    {"school":"Alabama",          "conf":"SEC","gender":"B","url":"https://rolltide.com/sports/xctrack/roster"},
    {"school":"Arkansas",         "conf":"SEC","gender":"M","url":"https://arkansasrazorbacks.com/sport/m-track/roster/"},
    {"school":"Arkansas",         "conf":"SEC","gender":"F","url":"https://arkansasrazorbacks.com/sport/w-track/roster/"},
    {"school":"Auburn",           "conf":"SEC","gender":"B","url":"https://auburntigers.com/sports/xctrack/roster"},
    {"school":"Florida",          "conf":"SEC","gender":"B","url":"https://floridagators.com/sports/track-and-field/roster"},
    {"school":"Georgia",          "conf":"SEC","gender":"B","url":"https://georgiadogs.com/sports/track-and-field/roster"},
    {"school":"Kentucky",         "conf":"SEC","gender":"B","url":"https://ukathletics.com/sports/track/roster"},
    {"school":"LSU",              "conf":"SEC","gender":"B","url":"https://lsusports.net/sports/tf/roster/"},
    {"school":"Ole Miss",         "conf":"SEC","gender":"B","url":"https://olemisssports.com/sports/track-and-field/roster"},
    {"school":"Mississippi State","conf":"SEC","gender":"B","url":"https://hailstate.com/sports/track-and-field/roster"},
    {"school":"Missouri",         "conf":"SEC","gender":"B","url":"https://mutigers.com/sports/track-and-field/roster"},
    {"school":"Oklahoma",         "conf":"SEC","gender":"B","url":"https://soonersports.com/sports/track-and-field/roster"},
    {"school":"South Carolina",   "conf":"SEC","gender":"B","url":"https://gamecocksonline.com/sports/track/roster/"},
    {"school":"Tennessee",        "conf":"SEC","gender":"B","url":"https://utsports.com/sports/track-and-field/roster"},
    {"school":"Texas",            "conf":"SEC","gender":"B","url":"https://texassports.com/sports/track-and-field/roster"},
    {"school":"Texas A&M",        "conf":"SEC","gender":"B","url":"https://12thman.com/sports/track-and-field/roster"},
    {"school":"Vanderbilt",       "conf":"SEC","gender":"F","url":"https://vucommodores.com/sports/wtrack/roster/"},
    {"school":"Brown",            "conf":"Ivy League","gender":"M","url":"https://brownbears.com/sports/mens-track-and-field/roster"},
    {"school":"Brown",            "conf":"Ivy League","gender":"F","url":"https://brownbears.com/sports/womens-track-and-field/roster"},
    {"school":"Columbia",         "conf":"Ivy League","gender":"B","url":"https://gocolumbialions.com/sports/track-and-field/roster"},
    {"school":"Cornell",          "conf":"Ivy League","gender":"M","url":"https://cornellbigred.com/sports/mens-track-and-field/roster"},
    {"school":"Cornell",          "conf":"Ivy League","gender":"F","url":"https://cornellbigred.com/sports/womens-track-and-field/roster"},
    {"school":"Dartmouth",        "conf":"Ivy League","gender":"M","url":"https://dartmouthsports.com/sports/mens-track-and-field/roster"},
    {"school":"Dartmouth",        "conf":"Ivy League","gender":"F","url":"https://dartmouthsports.com/sports/womens-track-and-field/roster"},
    {"school":"Harvard",          "conf":"Ivy League","gender":"B","url":"https://gocrimson.com/sports/mens-track-and-field/roster"},
    {"school":"Pennsylvania",     "conf":"Ivy League","gender":"M","url":"https://pennathletics.com/sports/mens-track-and-field/roster"},
    {"school":"Pennsylvania",     "conf":"Ivy League","gender":"F","url":"https://pennathletics.com/sports/womens-track-and-field/roster"},
    {"school":"Princeton",        "conf":"Ivy League","gender":"M","url":"https://goprincetontigers.com/sports/mens-track-and-field/roster"},
    {"school":"Princeton",        "conf":"Ivy League","gender":"F","url":"https://goprincetontigers.com/sports/womens-track-and-field/roster"},
    {"school":"Yale",             "conf":"Ivy League","gender":"M","url":"https://yalebulldogs.com/sports/mens-track-and-field/roster"},
    {"school":"Yale",             "conf":"Ivy League","gender":"F","url":"https://yalebulldogs.com/sports/womens-track-and-field/roster"},
    {"school":"Butler",           "conf":"Big East","gender":"M","url":"https://butlersports.com/sports/mens-track-and-field/roster"},
    {"school":"Butler",           "conf":"Big East","gender":"F","url":"https://butlersports.com/sports/womens-track-and-field/roster"},
    {"school":"Creighton",        "conf":"Big East","gender":"M","url":"https://gocreighton.com/sports/mens-cross-country/roster"},
    {"school":"Creighton",        "conf":"Big East","gender":"F","url":"https://gocreighton.com/sports/womens-cross-country/roster"},
    {"school":"DePaul",           "conf":"Big East","gender":"B","url":"https://depaulbluedemons.com/sports/track-and-field/roster"},
    {"school":"Georgetown",       "conf":"Big East","gender":"M","url":"https://guhoyas.com/sports/mens-track-and-field-xc/roster"},
    {"school":"Georgetown",       "conf":"Big East","gender":"F","url":"https://guhoyas.com/sports/womens-track-and-field/roster"},
    {"school":"Marquette",        "conf":"Big East","gender":"B","url":"https://gomarquette.com/sports/track-and-field/roster"},
    {"school":"Providence",       "conf":"Big East","gender":"M","url":"https://friars.com/sports/mens-track-and-field/roster"},
    {"school":"Providence",       "conf":"Big East","gender":"F","url":"https://friars.com/sports/womens-track-and-field/roster"},
    {"school":"St. John's",       "conf":"Big East","gender":"M","url":"https://redstormsports.com/sports/mens-track-and-field/roster"},
    {"school":"St. John's",       "conf":"Big East","gender":"F","url":"https://redstormsports.com/sports/womens-track-and-field/roster"},
    {"school":"Seton Hall",       "conf":"Big East","gender":"M","url":"https://shupirates.com/sports/mens-track-and-field/roster"},
    {"school":"Seton Hall",       "conf":"Big East","gender":"F","url":"https://shupirates.com/sports/womens-track-and-field/roster"},
    {"school":"Connecticut",      "conf":"Big East","gender":"M","url":"https://uconnhuskies.com/sports/mens-track-and-field/roster"},
    {"school":"Connecticut",      "conf":"Big East","gender":"F","url":"https://uconnhuskies.com/sports/womens-track-and-field/roster"},
    {"school":"Villanova",        "conf":"Big East","gender":"M","url":"https://villanova.com/sports/mens-track-and-field/roster"},
    {"school":"Villanova",        "conf":"Big East","gender":"F","url":"https://villanova.com/sports/womens-track-and-field/roster"},
    {"school":"Xavier",           "conf":"Big East","gender":"B","url":"https://goxavier.com/sports/track-and-field/roster"},
    {"school":"Air Force",        "conf":"Mountain West","gender":"B","url":"https://goairforcefalcons.com/sports/track-and-field/roster"},
    {"school":"Boise State",      "conf":"Mountain West","gender":"B","url":"https://broncosports.com/sports/track-and-field/roster"},
    {"school":"Colorado State",   "conf":"Mountain West","gender":"B","url":"https://csurams.com/sports/track-and-field/roster"},
    {"school":"Fresno State",     "conf":"Mountain West","gender":"B","url":"https://gobulldogs.com/sports/track-and-field/roster"},
    {"school":"Hawaii",           "conf":"Mountain West","gender":"F","url":"https://hawaiiathletics.com/sports/womens-track-and-field/roster"},
    {"school":"Nevada",           "conf":"Mountain West","gender":"F","url":"https://nevadawolfpack.com/sports/womens-track-and-field/roster"},
    {"school":"New Mexico",       "conf":"Mountain West","gender":"B","url":"https://golobos.com/sports/track/roster"},
    {"school":"San Diego State",  "conf":"Mountain West","gender":"F","url":"https://goaztecs.com/sports/track-and-field/roster"},
    {"school":"San Jose State",   "conf":"Mountain West","gender":"B","url":"https://sjsuspartans.com/sports/track-and-field/roster"},
    {"school":"UNLV",             "conf":"Mountain West","gender":"F","url":"https://unlvrebels.com/sports/womens-track-and-field/roster"},
    {"school":"Utah State",       "conf":"Mountain West","gender":"B","url":"https://utahstateaggies.com/sports/track-and-field/roster"},
    {"school":"Wyoming",          "conf":"Mountain West","gender":"B","url":"https://gowyo.com/sports/track-and-field/roster"},
    {"school":"Davidson",         "conf":"Atlantic 10","gender":"M","url":"https://davidsonwildcats.com/sports/mens-track-and-field/roster"},
    {"school":"Davidson",         "conf":"Atlantic 10","gender":"F","url":"https://davidsonwildcats.com/sports/womens-track-and-field/roster"},
    {"school":"Dayton",           "conf":"Atlantic 10","gender":"F","url":"https://daytonflyers.com/sports/womens-track-and-field/roster"},
    {"school":"Duquesne",         "conf":"Atlantic 10","gender":"M","url":"https://goduquesne.com/sports/mens-track-and-field/roster"},
    {"school":"Duquesne",         "conf":"Atlantic 10","gender":"F","url":"https://goduquesne.com/sports/womens-track-and-field/roster"},
    {"school":"Fordham",          "conf":"Atlantic 10","gender":"M","url":"https://fordhamsports.com/sports/mens-track-and-field/roster"},
    {"school":"Fordham",          "conf":"Atlantic 10","gender":"F","url":"https://fordhamsports.com/sports/womens-track-and-field/roster"},
    {"school":"George Mason",     "conf":"Atlantic 10","gender":"M","url":"https://gomason.com/sports/mens-track-and-field/roster"},
    {"school":"George Mason",     "conf":"Atlantic 10","gender":"F","url":"https://gomason.com/sports/womens-track-and-field/roster"},
    {"school":"George Washington","conf":"Atlantic 10","gender":"M","url":"https://gwsports.com/sports/mens-cross-country/roster"},
    {"school":"George Washington","conf":"Atlantic 10","gender":"F","url":"https://gwsports.com/sports/womens-cross-country/roster"},
    {"school":"La Salle",         "conf":"Atlantic 10","gender":"M","url":"https://goexplorers.com/sports/mens-track-and-field/roster"},
    {"school":"La Salle",         "conf":"Atlantic 10","gender":"F","url":"https://goexplorers.com/sports/womens-track-and-field/roster"},
    {"school":"Loyola Chicago",   "conf":"Atlantic 10","gender":"B","url":"https://loyolaramblers.com/sports/track-and-field/roster"},
    {"school":"Rhode Island",     "conf":"Atlantic 10","gender":"M","url":"https://gorhody.com/sports/mens-track-and-field/roster"},
    {"school":"Rhode Island",     "conf":"Atlantic 10","gender":"F","url":"https://gorhody.com/sports/womens-track-and-field/roster"},
    {"school":"Richmond",         "conf":"Atlantic 10","gender":"F","url":"https://richmondspiders.com/sports/womens-track-and-field/roster"},
    {"school":"St. Bonaventure",  "conf":"Atlantic 10","gender":"M","url":"https://gobonnies.com/sports/mens-track-and-field/roster"},
    {"school":"St. Bonaventure",  "conf":"Atlantic 10","gender":"F","url":"https://gobonnies.com/sports/womens-track-and-field/roster"},
    {"school":"Saint Joseph's",   "conf":"Atlantic 10","gender":"M","url":"https://sjuhawks.com/sports/mens-track-and-field/roster"},
    {"school":"Saint Joseph's",   "conf":"Atlantic 10","gender":"F","url":"https://sjuhawks.com/sports/womens-track-and-field/roster"},
    {"school":"Saint Louis",      "conf":"Atlantic 10","gender":"B","url":"https://slubillikens.com/sports/track-and-field/roster"},
    {"school":"VCU",              "conf":"Atlantic 10","gender":"M","url":"https://vcuathletics.com/sports/mens-track-and-field/roster"},
    {"school":"VCU",              "conf":"Atlantic 10","gender":"F","url":"https://vcuathletics.com/sports/womens-track-and-field/roster"},
    {"school":"UAB",              "conf":"American","gender":"F","url":"https://uabsports.com/sports/womens-track-and-field/roster"},
    {"school":"Charlotte",        "conf":"American","gender":"B","url":"https://charlotte49ers.com/sports/track-and-field/roster"},
    {"school":"East Carolina",    "conf":"American","gender":"B","url":"https://ecupirates.com/sports/track-and-field/roster"},
    {"school":"Florida Atlantic", "conf":"American","gender":"F","url":"https://fausports.com/sports/womens-track-and-field/roster"},
    {"school":"Memphis",          "conf":"American","gender":"M","url":"https://gotigersgo.com/sports/mens-track-and-field/roster"},
    {"school":"Memphis",          "conf":"American","gender":"F","url":"https://gotigersgo.com/sports/womens-track-and-field/roster"},
    {"school":"North Texas",      "conf":"American","gender":"B","url":"https://meangreensports.com/sports/track-and-field/roster"},
    {"school":"Rice",             "conf":"American","gender":"M","url":"https://riceowls.com/sports/mens-track-and-field/roster"},
    {"school":"Rice",             "conf":"American","gender":"F","url":"https://riceowls.com/sports/womens-track-and-field/roster"},
    {"school":"South Florida",    "conf":"American","gender":"B","url":"https://gousfbulls.com/sports/track-and-field/roster"},
    {"school":"Temple",           "conf":"American","gender":"M","url":"https://owlsports.com/sports/mens-track-and-field/roster"},
    {"school":"Temple",           "conf":"American","gender":"F","url":"https://owlsports.com/sports/womens-track-and-field/roster"},
    {"school":"Tulane",           "conf":"American","gender":"B","url":"https://tulanegreenwave.com/sports/track-and-field/roster"},
    {"school":"Tulsa",            "conf":"American","gender":"B","url":"https://tulsahurricane.com/sports/track-and-field/roster"},
    {"school":"UTSA",             "conf":"American","gender":"B","url":"https://goutsa.com/sports/track-fieldcross-country/roster"},
    {"school":"Wichita State",    "conf":"American","gender":"B","url":"https://goshockers.com/sports/track-and-field/roster"},
    {"school":"Eastern Washington","conf":"Big Sky","gender":"B","url":"https://goeags.com/sports/track-and-field/roster"},
    {"school":"Idaho",            "conf":"Big Sky","gender":"B","url":"https://govandals.com/sports/tfxc/roster"},
    {"school":"Idaho State",      "conf":"Big Sky","gender":"M","url":"https://isubengals.com/sports/mens-track-and-field/roster"},
    {"school":"Idaho State",      "conf":"Big Sky","gender":"F","url":"https://isubengals.com/sports/womens-track-and-field/roster"},
    {"school":"Montana",          "conf":"Big Sky","gender":"M","url":"https://gogriz.com/sports/mens-track-and-field/roster"},
    {"school":"Montana",          "conf":"Big Sky","gender":"F","url":"https://gogriz.com/sports/womens-track-and-field/roster"},
    {"school":"Montana State",    "conf":"Big Sky","gender":"M","url":"https://msubobcats.com/sports/mens-track-and-field/roster"},
    {"school":"Montana State",    "conf":"Big Sky","gender":"F","url":"https://msubobcats.com/sports/womens-track-and-field/roster"},
    {"school":"Northern Arizona", "conf":"Big Sky","gender":"B","url":"https://nauathletics.com/sports/track-and-field/roster"},
    {"school":"Northern Colorado","conf":"Big Sky","gender":"B","url":"https://uncbears.com/sports/track-and-field/roster"},
    {"school":"Portland State",   "conf":"Big Sky","gender":"B","url":"https://goviks.com/sports/track-and-field/roster"},
    {"school":"Sacramento State", "conf":"Big Sky","gender":"B","url":"https://hornetsports.com/sports/track/roster"},
    {"school":"Weber State",      "conf":"Big Sky","gender":"M","url":"https://weberstatesports.com/sports/mens-track-and-field/roster"},
    {"school":"Weber State",      "conf":"Big Sky","gender":"F","url":"https://weberstatesports.com/sports/track-and-field/roster"},
    {"school":"Oregon State",     "conf":"Pac-12","gender":"F","url":"https://osubeavers.com/sports/track-and-field/roster"},
    {"school":"Washington State", "conf":"Pac-12","gender":"B","url":"https://wsucougars.com/sports/track-and-field/roster"},
]


def normalize(name: str) -> str:
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = name.lower().strip()
    name = re.sub(r"\b(jr\.?|sr\.?|ii|iii|iv)\b", "", name)
    name = re.sub(r"[^a-z\s\-]", "", name)
    return re.sub(r"\s+", " ", name).strip()


def parse_hometown(raw: str) -> str | None:
    if not raw:
        return None
    # Strip trailing metadata
    raw = re.split(r'Last School|High School|Previous School|Prep School', raw, flags=re.I)[0].strip()
    raw = raw.splitlines()[0].strip()
    m = re.match(r"^([\w][^\,]{1,35}),\s*(.{2,25})$", raw.strip())
    if not m:
        return None
    city  = m.group(1).strip().rstrip(".")
    state = m.group(2).strip().rstrip(".").rstrip(",")
    state = re.sub(r'\s*\(?\s*USA\s*\)?', '', state, flags=re.I).strip().rstrip(".")
    # Expand abbreviated state names (e.g. "Fla." -> "Florida")
    for abbr, full in EXPAND_ABBR.items():
        if re.fullmatch(re.escape(abbr) + r'\.?', state, re.I):
            state = full
            break
    abbr = STATE_ABBR.get(state) or STATE_ABBR.get(state.title())
    if not abbr:
        return None  # international athlete — skip
    return f"{city}, {abbr}"



# ── State abbreviation helpers ─────────────────────────────────────────────

# Dotted abbreviations used on rosters → 2-letter postal codes
DOTTED_TO_STATE = {
    "Ala": "AL", "Ariz": "AZ", "Ark": "AR", "Calif": "CA", "Colo": "CO",
    "Conn": "CT", "Del": "DE", "Fla": "FL", "Ga": "GA", "Ill": "IL",
    "Ind": "IN", "Kan": "KS", "Ky": "KY", "La": "LA", "Md": "MD",
    "Mass": "MA", "Mich": "MI", "Minn": "MN", "Miss": "MS", "Mo": "MO",
    "Mont": "MT", "Neb": "NE", "Nev": "NV", "Okla": "OK", "Ore": "OR",
    "Pa": "PA", "Tenn": "TN", "Tex": "TX", "Va": "VA", "Vt": "VT",
    "Wash": "WA", "Wis": "WI", "Wyo": "WY",
    "N.H": "NH", "N.J": "NJ", "N.M": "NM", "N.Y": "NY",
    "N.C": "NC", "N.D": "ND", "R.I": "RI", "S.C": "SC", "S.D": "SD",
    "W.Va": "WV", "D.C": "DC",
}

# Build regex that matches any dotted abbreviation (longest first to avoid Pa vs Pa in N.D)
_DOT_PAT = "(" + "|".join(
    re.escape(k) for k in sorted(DOTTED_TO_STATE, key=len, reverse=True)
) + r")\.?"

SKIP_WORDS = {
    "Academic Year", "Hometown", "Last School", "High School", "Full Bio",
    "Expand", "Card View", "List View", "Table View", "Class", "Event",
    "Height", "Weight", "Previous School", "Men's Track", "Women's Track",
    "Track & Field", "Cross Country",
}

_FULL_STATE_PAT = "(" + "|".join(
    re.escape(s) for s in sorted(FULL_TO_ABBR.keys(), key=len, reverse=True)
) + r")"

_YEAR_RE = re.compile(
    r"^(?:Freshman|Sophomore|Junior|Senior|Graduate\s+Student|Graduate|"
    r"First\s+Year|Redshirt\s+\w+)\s+", re.I
)

NON_CITY_STARTS = {
    "college", "university", "school", "department", "sciences", "science",
    "arts", "art", "business", "engineering", "program", "studies",
    "management", "communications", "humanities", "education", "liberal",
    "applied", "natural", "political", "computer", "information",
    "environmental", "wharton", "kellogg", "haas", "stern",
}


def _find_ht_in_blob(blob: str):
    """
    Extract 'City, ST' from a mixed blob like:
        'Junior Harvey, Ill. Thornton Township'
        'Freshman College of Arts & Sciences Atlanta, Ga.'
        'Redshirt Junior McKinney, Texas Allen HS'
    Returns a formatted 'City, ST' string or None.
    """
    # Strip leading year/class word(s)
    blob = _YEAR_RE.sub("", blob).strip()

    def try_extract(before_comma: str, state_code: str):
        words = before_comma.strip().split()
        for n in [3, 2, 1]:
            if len(words) < n:
                continue
            city_words = words[-n:]
            if not all(re.match(r"[A-Z]", w) for w in city_words):
                continue
            if city_words[0].rstrip(".").lower() in NON_CITY_STARTS:
                continue
            city = " ".join(city_words).rstrip(".")
            r = parse_hometown(f"{city}, {state_code}")
            if r:
                return r
        return None

    # Dotted abbreviation: "Harvey, Ill."
    for m in re.finditer(r"(.+),\s+" + _DOT_PAT + r"(?:\s|/|$)", blob):
        state = DOTTED_TO_STATE.get(m.group(2))
        if state:
            r = try_extract(m.group(1), state)
            if r:
                return r

    # 2-letter postal code: "Walla Walla, WA"
    for m in re.finditer(r"(.+),\s+([A-Z]{2})(?:\s|/|$)", blob):
        state = FULL_TO_ABBR.get(m.group(2)) or (m.group(2) if m.group(2) in STATE_ABBR.values() else None)
        if state:
            r = try_extract(m.group(1), state)
            if r:
                return r

    # Full state name: "McKinney, Texas"
    for m in re.finditer(r"(.+),\s+" + _FULL_STATE_PAT + r"(?:\s|/|$)", blob, re.I):
        state = FULL_TO_ABBR.get(m.group(2)) or FULL_TO_ABBR.get(m.group(2).title())
        if state:
            r = try_extract(m.group(1), state)
            if r:
                return r

    return None


def _extract_name_before(lines, i):
    for j in range(i - 1, max(i - 15, -1), -1):
        cand = lines[j]
        if cand.startswith("### "):
            cand = cand[4:]
        if (re.match(r'[A-Z][a-z]', cand)
                and 2 <= len(cand.split()) <= 5
                and not any(s.lower() in cand.lower() for s in SKIP_WORDS)
                and not re.search(r'\d{4}|http|\.com|@', cand)):
            return cand.strip()
    return None


def _parse_tab_table(lines: list[str]) -> list[dict]:
    """
    Pattern C: Tab-separated table.
    Find header row containing 'HOMETOWN' column, then parse data rows.
    Examples:
        FULL NAME\tEVENTS\tYEAR\tHOMETOWN\tHIGH SCHOOL
        Name\tYr\tHt\tEvents\tHometown\tHigh School/Previous School
        FULL NAME\tEVENTS\tACADEMIC YEAR\tHOMETOWN / HIGH SCHOOL
    """
    results = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if "\t" in line and "hometown" in line.lower():
            # Found header - find which tab column is HOMETOWN
            cols = [c.strip().lower() for c in line.split("\t")]
            ht_col = next(
                (j for j, c in enumerate(cols)
                 if "hometown" in c and "school" not in c),
                None
            )
            # Fallback: column that starts with "hometown"
            if ht_col is None:
                ht_col = next(
                    (j for j, c in enumerate(cols) if c.startswith("hometown")),
                    None
                )
            name_col = 0  # name is always first column
            if ht_col is None:
                i += 1
                continue

            # Parse data rows until we hit a non-tab line or another header
            i += 1
            while i < len(lines):
                row_line = lines[i]
                if "\t" not in row_line:
                    break
                # Skip if this looks like another header
                if "hometown" in row_line.lower() and "name" in row_line.lower():
                    break
                cells = [c.strip() for c in row_line.split("\t")]
                if len(cells) <= ht_col:
                    i += 1
                    continue
                name = cells[name_col] if len(cells) > name_col else ""
                raw_ht = cells[ht_col]
                # Strip "/ High School name" suffix
                raw_ht = re.split(r'\s*/\s*', raw_ht)[0].strip()
                # Skip blank, international-looking
                if not name or not raw_ht or len(name) < 4:
                    i += 1
                    continue
                # Name might be ALL CAPS on some sites - title-case it
                if name.isupper():
                    name = name.title()
                hometown = _find_ht_in_blob(raw_ht)
                if hometown:
                    results.append({"name": name, "hometown": hometown})
                i += 1
            continue  # don't increment i again
        i += 1
    return results


def _parse_inline_cards(lines: list[str]) -> list[dict]:
    """
    Pattern D: Inline card format.
    Each athlete card renders as:
        Name (alone)
        [blank line stripped out]
        YearClass [Major] City, State. HighSchool
        Full Bio
    We anchor on 'Full Bio' lines and look backwards.
    """
    results = []
    for i, line in enumerate(lines):
        if line != "Full Bio":
            continue
        # Walk back to find the blob (year+city+hs) and the name
        blob = None
        name = None
        for j in range(i - 1, max(i - 6, -1), -1):
            cand = lines[j]
            if not blob:
                # First non-skip line back = blob
                if cand not in SKIP_WORDS and not re.match(
                    r'(?:Freshman|Sophomore|Junior|Senior|Graduate|First Year|'
                    r'Redshirt|Sr\.|Jr\.|So\.|Fr\.|Gr\.)',
                    cand,
                ) or re.search(r',', cand):
                    # Line contains year+city run together
                    blob = cand
                    continue
            else:
                # blob found — look for name
                if (re.match(r'[A-Z][a-z]', cand) or cand.isupper()):
                    words = cand.split()
                    if (2 <= len(words) <= 5
                            and not any(s.lower() in cand.lower() for s in SKIP_WORDS)
                            and not re.search(r'\d{4}|http|\.com|@', cand)):
                        name = cand.title() if cand.isupper() else cand
                        break
        if name and blob:
            hometown = _find_ht_in_blob(blob)
            if hometown:
                results.append({"name": name, "hometown": hometown})
    return results


def parse_page(text: str) -> list[dict]:
    """
    Extract (name, hometown) pairs from a Sidearm roster page.

    Handles four rendering patterns found across DI programs:
      A  Label-value cards:  "Hometown" alone, then "Hilliard, Ohio" on next line
      B  Inline label:       "Hometown Hilliard, Ohio" on one line
      C  Tab-separated table: FULL NAME\tEVENTS\tHOMETOWN\t...
      D  Inline card blob:   "Junior Harvey, Ill. Thornton Township" after name line
    """
    results = []
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # ── Pattern A & B (label-based) ──────────────────────────────────────────
    for i, line in enumerate(lines):
        if line == "Hometown" and i + 1 < len(lines):
            raw_ht = lines[i + 1].strip()
            if raw_ht not in SKIP_WORDS and len(raw_ht) > 3:
                hometown = parse_hometown(raw_ht)
                name = _extract_name_before(lines, i)
                if name and len(name) >= 4:
                    results.append({"name": name, "hometown": hometown})
        elif line.startswith("Hometown ") and len(line) > 12:
            raw_ht = line[9:].strip()
            hometown = parse_hometown(raw_ht)
            name = _extract_name_before(lines, i)
            if name and len(name) >= 4:
                results.append({"name": name, "hometown": hometown})

    # If Patterns A/B found athletes, we're done (avoid double-counting)
    if results:
        pass
    else:
        # ── Pattern C: tab-separated table ───────────────────────────────────
        results.extend(_parse_tab_table(lines))
        # ── Pattern D: inline card blobs ─────────────────────────────────────
        if not results:
            results.extend(_parse_inline_cards(lines))

    # Deduplicate by normalized name
    seen, out = set(), []
    for r in results:
        k = normalize(r["name"])
        if k not in seen:
            seen.add(k)
            out.append(r)
    return out


def scrape_page(page, url: str, school: str) -> list[dict]:
    try:
        page.goto(url, timeout=45000, wait_until="load")
    except PlaywrightTimeout:
        log.warning(f"  Timeout on load: {url}")
        return []
    except Exception as e:
        log.error(f"  Error fetching {url}: {e}")
        return []

    time.sleep(3.5)

    try:
        text = page.inner_text("body")
    except Exception as e:
        log.error(f"  Body error: {e}")
        return []

    athletes = parse_page(text)

    # Retry with scroll if nothing found
    if not athletes:
        time.sleep(4.0)
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(1.5)
            text = page.inner_text("body")
            athletes = parse_page(text)
        except Exception:
            pass

    # Debug: print a snippet so we can see what the page actually contains
    if not athletes:
        # Find the word "Hometown" in the text to see its context
        ht_idx = text.find("Hometown")
        if ht_idx != -1:
            log.warning(f"  'Hometown' found but not parsed. Context: {repr(text[max(0,ht_idx-50):ht_idx+150])}")
        else:
            log.warning(f"  No 'Hometown' text found at all. Page snippet: {repr(text[500:1000])}")

    log.info(f"  {len(athletes)} athletes parsed — {school}")
    return athletes


def build_index(db_rows: list[dict]) -> dict:
    idx = {}
    for r in db_rows:
        key = (normalize(r["name"]), normalize(r.get("college") or ""))
        idx.setdefault(key, []).append(r)
    return idx


def run(conf_filter=None, school_filter=None, limit=9999, dry_run=False, overwrite=False):
    log.info("=" * 60)
    log.info("Roster Hometown Scraper v2")
    log.info(f"  conf={conf_filter or 'ALL'}  school={school_filter or 'ALL'}"
             f"  dry_run={dry_run}  overwrite={overwrite}")
    log.info("=" * 60)

    # Load all DB athletes
    log.info("Loading athletes from Supabase...")
    all_db, offset = [], 0
    while True:
        batch = supabase.table("athletes").select(
            "id,name,college,conference,hometown"
        ).range(offset, offset + 999).execute().data or []
        all_db.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    log.info(f"  Loaded {len(all_db)} athletes")
    db_idx = build_index(all_db)

    # Filter roster list
    rosters = ROSTERS
    if conf_filter:
        rosters = [r for r in rosters if r["conf"].lower() == conf_filter.lower()]
    if school_filter:
        rosters = [r for r in rosters if school_filter.lower() in r["school"].lower()]
    rosters = rosters[:limit]
    log.info(f"  Processing {len(rosters)} roster pages")

    stats = {"pages": 0, "found": 0, "matched": 0, "updated": 0, "skipped": 0, "no_match": 0}
    pending = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ]
        )
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        # Hide the webdriver flag that bot-detection scripts check
        ctx.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        pg = ctx.new_page()
        pg.set_default_timeout(45000)
        last_school = None

        for entry in rosters:
            school, conf, url = entry["school"], entry["conf"], entry["url"]
            if school != last_school:
                if last_school:
                    time.sleep(SCHOOL_DELAY)
                log.info(f"\n── {school} [{conf}] ──────────────────────────")
                last_school = school

            time.sleep(PAGE_DELAY)
            stats["pages"] += 1
            athletes = scrape_page(pg, url, school)
            stats["found"] += len(athletes)

            for a in athletes:
                if not a["hometown"]:
                    continue

                nname    = normalize(a["name"])
                ncollege = normalize(school)
                key      = (nname, ncollege)
                matches  = db_idx.get(key, [])

                # Fuzzy: first+last only (handles middle names)
                if not matches:
                    parts = nname.split()
                    if len(parts) >= 3:
                        matches = db_idx.get((f"{parts[0]} {parts[-1]}", ncollege), [])

                if not matches:
                    log.debug(f"  No match: {a['name']} @ {school}")
                    stats["no_match"] += 1
                    continue
                if len(matches) > 1:
                    log.debug(f"  Ambiguous: {a['name']} @ {school}")
                    continue

                row = matches[0]
                stats["matched"] += 1
                existing = (row.get("hometown") or "").strip()

                if not overwrite and existing and len(existing) > 3:
                    log.debug(f"  Skip (has hometown): {a['name']} → {existing}")
                    stats["skipped"] += 1
                    continue

                log.info(f"  ✓ {a['name']} ({school}) → {a['hometown']}")
                pending.append({
                    "id": row["id"], "name": a["name"],
                    "college": school, "hometown": a["hometown"],
                })

                if len(pending) >= 50:
                    if not dry_run:
                        for u in pending:
                            state = u["hometown"].split(", ")[-1] if ", " in u["hometown"] else None
                            try:
                                supabase.table("athletes").update({
                                    "hometown": u["hometown"],
                                    "hometown_state": state,
                                    "updated_at": datetime.utcnow().isoformat(),
                                }).eq("id", u["id"]).execute()
                            except Exception as e:
                                log.error(f"  DB error {u['name']}: {e}")
                    stats["updated"] += len(pending)
                    pending = []

        # Final flush
        if pending:
            if not dry_run:
                for u in pending:
                    state = u["hometown"].split(", ")[-1] if ", " in u["hometown"] else None
                    try:
                        supabase.table("athletes").update({
                            "hometown": u["hometown"],
                            "hometown_state": state,
                            "updated_at": datetime.utcnow().isoformat(),
                        }).eq("id", u["id"]).execute()
                    except Exception as e:
                        log.error(f"  DB error {u['name']}: {e}")
            stats["updated"] += len(pending)

        browser.close()

    log.info("\n" + "=" * 60)
    log.info("SUMMARY")
    log.info(f"  Pages scraped    : {stats['pages']}")
    log.info(f"  Athletes parsed  : {stats['found']}")
    log.info(f"  Matched to DB    : {stats['matched']}")
    log.info(f"  Updated          : {stats['updated']}")
    log.info(f"  Skipped (exists) : {stats['skipped']}")
    log.info(f"  No DB match      : {stats['no_match']}")
    if dry_run:
        log.info("  ** DRY RUN — no DB writes made **")
    log.info("=" * 60)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Scrape official roster pages for athlete hometowns")
    p.add_argument("--conf",    help="Conference filter (e.g. ACC, SEC, 'Big Ten')")
    p.add_argument("--school",  help="School filter (e.g. Syracuse, Iowa)")
    p.add_argument("--limit",   type=int, default=9999, help="Max roster pages to process")
    p.add_argument("--dry-run", action="store_true", help="Parse + match only, no DB writes")
    p.add_argument("--all",     action="store_true",  help="Overwrite existing hometowns too")
    args = p.parse_args()
    run(args.conf, args.school, args.limit, args.dry_run, args.all)
