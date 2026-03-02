import { useState, useEffect, useRef, useMemo } from "react";
import * as d3 from "d3";

// ── THEME ─────────────────────────────────────────────────────────────────────
const T = {
  orange:    "#F76900",
  orangeD:   "#D74100",
  orangeL:   "#FF8E00",
  orangeM:   "#FF431B",
  orangeGlow:"rgba(247,105,0,0.10)",
  white:     "#FFFFFF",
  black:     "#000000",
  bg:        "#FFFFFF",
  bgPanel:   "#F7F7F8",
  bgCard:    "#EFF0F1",
  border:    "#D4D6D9",
  borderH:   "#9BA0A6",
  muted:     "#404040",
  dim:       "#404040",
  offWhite:  "#000000",
  green:  "#2B72D7",
  yellow: "#FF8E00",
  red:    "#FF431B",
  blueP:  "#000E54",
  blueL:  "#2B72D7",
  blueM:  "#203299",
  grayM:  "#707780",
  grayL:  "#ADB3B8",
};

// ── EVENT CONFIG ──────────────────────────────────────────────────────────────
const EVENTS_CFG = [
  {id:"60m",    label:"60m",    season:"indoor" },
  {id:"60mH",   label:"60mH",   season:"indoor" },
  {id:"100m",   label:"100m",   season:"outdoor"},
  {id:"200m",   label:"200m",   season:"both"   },
  {id:"400m",   label:"400m",   season:"both"   },
  {id:"800m",   label:"800m",   season:"both"   },
  {id:"1500m",  label:"1500m",  season:"both"   },
  {id:"Mile",   label:"Mile",   season:"both"   },
  {id:"3000m",  label:"3000m",  season:"indoor" },
  {id:"5000m",  label:"5000m",  season:"both"   },
  {id:"10000m", label:"10000m", season:"outdoor"},
  {id:"110mH",  label:"110mH",  season:"outdoor"},
  {id:"400mH",  label:"400mH",  season:"outdoor"},
  {id:"3000SC", label:"Steeple", season:"outdoor"},
];

// Field events: higher mark = better
const FIELD_EVENTS = new Set(["LJ","TJ","HJ","PV","SP","DT","HT","JT","WT","Hept","Dec","Pent"]);
const isFieldEvent = e => FIELD_EVENTS.has(e);

// ── COLLEGE COORDINATES ───────────────────────────────────────────────────────
const COLLEGE_COORDS = {
  // SEC
  "Alabama":[33.211,-87.535],"Arkansas":[36.068,-94.174],"Auburn":[32.603,-85.481],
  "Florida":[29.651,-82.325],"Georgia":[33.958,-83.376],"Kentucky":[38.029,-84.504],
  "LSU":[30.413,-91.180],"Mississippi State":[33.456,-88.789],"Missouri":[38.952,-92.328],
  "Ole Miss":[34.365,-89.538],"South Carolina":[33.996,-81.027],"Tennessee":[35.955,-83.923],
  "Texas A&M":[30.618,-96.340],"Vanderbilt":[36.144,-86.803],
  // Big Ten
  "Illinois":[40.102,-88.228],"Indiana":[39.165,-86.526],"Iowa":[41.661,-91.534],
  "Maryland":[38.986,-76.943],"Michigan":[42.278,-83.738],"Michigan State":[42.731,-84.481],
  "Minnesota":[44.974,-93.228],"Nebraska":[40.820,-96.706],"Northwestern":[42.055,-87.675],
  "Ohio State":[40.006,-83.021],"Penn State":[40.797,-77.860],"Purdue":[40.425,-86.913],
  "Rutgers":[40.500,-74.450],"Wisconsin":[43.076,-89.412],
  // ACC
  "Boston College":[42.337,-71.168],"Clemson":[34.677,-82.837],"Duke":[36.001,-78.939],
  "Florida State":[30.441,-84.298],"Georgia Tech":[33.776,-84.396],"Louisville":[38.211,-85.758],
  "Miami":[25.756,-80.371],"Miami (FL)":[25.756,-80.371],"NC State":[35.786,-78.686],
  "North Carolina":[35.905,-79.047],"Notre Dame":[41.701,-86.238],"Pittsburgh":[40.444,-79.960],
  "Syracuse":[43.036,-76.134],"Virginia":[38.033,-78.508],"Virginia Tech":[37.228,-80.421],
  "Wake Forest":[36.134,-80.274],
  // Big 12
  "Baylor":[31.548,-97.116],"BYU":[40.252,-111.649],"Iowa State":[42.026,-93.648],
  "Kansas":[38.971,-95.253],"Kansas State":[39.191,-96.578],"Oklahoma State":[36.127,-97.068],
  "TCU":[32.710,-97.363],"Texas":[30.284,-97.735],"Texas Tech":[33.584,-101.875],
  "West Virginia":[39.635,-79.954],
  // Pac-12
  "Arizona":[32.232,-110.953],"Arizona State":[33.424,-111.928],"Cal":[37.872,-122.260],
  "California":[37.872,-122.260],"Colorado":[40.007,-105.266],"Oregon":[44.045,-123.073],
  "Oregon State":[44.564,-123.278],"Stanford":[37.427,-122.170],"UCLA":[34.068,-118.445],
  "USC":[34.022,-118.285],"Utah":[40.762,-111.836],"Washington":[47.655,-122.303],
  "Washington State":[46.730,-117.158],
  // Ivy League
  "Brown":[41.826,-71.403],"Columbia":[40.808,-73.962],"Cornell":[42.453,-76.473],
  "Dartmouth":[43.705,-72.288],"Harvard":[42.377,-71.117],"Penn":[39.952,-75.193],
  "Princeton":[40.343,-74.651],"Yale":[41.316,-72.923],
  // Big East
  "Butler":[39.839,-86.172],"Connecticut":[41.808,-72.253],"UConn":[41.808,-72.253],
  "Georgetown":[38.907,-77.072],"Marquette":[43.038,-87.930],"Providence":[41.826,-71.403],
  "Seton Hall":[40.746,-74.236],"Villanova":[40.036,-75.343],"DePaul":[41.931,-87.654],
  "Creighton":[41.258,-95.943],"St. John's":[40.723,-73.794],"Xavier":[39.147,-84.472],
  // Mountain West
  "Air Force":[38.997,-104.861],"Boise State":[43.602,-116.199],"Colorado State":[40.574,-105.085],
  "Fresno State":[36.812,-119.748],"Hawaii":[21.297,-157.817],"Nevada":[39.547,-119.816],
  "New Mexico":[35.084,-106.620],"San Diego State":[32.776,-117.071],"San Jose State":[37.335,-121.881],
  "UNLV":[36.108,-115.142],"Utah State":[41.742,-111.810],"Wyoming":[41.314,-105.576],
  // Big Sky
  "Eastern Washington":[47.673,-117.401],"Idaho":[46.726,-117.008],"Idaho State":[42.861,-112.431],
  "Montana":[46.861,-113.985],"Montana State":[45.670,-111.047],"Northern Arizona":[35.184,-111.657],
  "Northern Colorado":[40.406,-104.700],"Portland State":[45.511,-122.683],
  "Sacramento State":[38.561,-121.423],"Southern Utah":[37.677,-113.061],"Weber State":[41.195,-111.973],
  // American Athletic
  "East Carolina":[35.607,-77.366],"Florida Atlantic":[26.370,-80.103],"Memphis":[35.118,-89.938],
  "North Texas":[33.208,-97.147],"Rice":[29.717,-95.403],"South Florida":[28.061,-82.414],
  "Temple":[39.981,-75.155],"Tulane":[29.939,-90.121],"Tulsa":[36.151,-95.947],
  "UAB":[33.502,-86.808],"UTSA":[29.576,-98.614],"Wichita State":[37.719,-97.295],
  // Atlantic 10
  "Davidson":[35.499,-80.849],"Dayton":[39.740,-84.183],"Duquesne":[40.436,-79.992],
  "Fordham":[40.862,-73.884],"George Mason":[38.832,-77.308],"George Washington":[38.900,-77.048],
  "UMass":[42.391,-72.526],"Rhode Island":[41.484,-71.526],"Richmond":[37.574,-77.540],
  "Saint Louis":[38.637,-90.235],"St. Bonaventure":[42.079,-78.471],"VCU":[37.549,-77.453],
  // West Coast
  "Gonzaga":[47.667,-117.402],"Loyola Marymount":[33.970,-118.416],"Pepperdine":[34.035,-118.710],
  "Portland":[45.557,-122.676],"Saint Mary's":[37.837,-122.115],"San Diego":[32.771,-117.192],
  "San Francisco":[37.776,-122.451],"Santa Clara":[37.349,-121.939],
};

const getCollegeCoords = (college) => COLLEGE_COORDS[college] || [39.5, -98.35];

// ── STATE DATA ────────────────────────────────────────────────────────────────
const STATE_NAMES = {
  AL:"Alabama",AK:"Alaska",AZ:"Arizona",AR:"Arkansas",CA:"California",
  CO:"Colorado",CT:"Connecticut",DE:"Delaware",FL:"Florida",GA:"Georgia",
  HI:"Hawaii",ID:"Idaho",IL:"Illinois",IN:"Indiana",IA:"Iowa",KS:"Kansas",
  KY:"Kentucky",LA:"Louisiana",ME:"Maine",MD:"Maryland",MA:"Massachusetts",
  MI:"Michigan",MN:"Minnesota",MS:"Mississippi",MO:"Missouri",MT:"Montana",
  NE:"Nebraska",NV:"Nevada",NH:"New Hampshire",NJ:"New Jersey",NM:"New Mexico",
  NY:"New York",NC:"North Carolina",ND:"North Dakota",OH:"Ohio",OK:"Oklahoma",
  OR:"Oregon",PA:"Pennsylvania",RI:"Rhode Island",SC:"South Carolina",
  SD:"South Dakota",TN:"Tennessee",TX:"Texas",UT:"Utah",VT:"Vermont",
  VA:"Virginia",WA:"Washington",WV:"West Virginia",WI:"Wisconsin",WY:"Wyoming",
  DC:"Washington D.C.",
};

// ── CITY COORDINATES (real coords for accurate distance + heatmap placement) ─
const CITY_COORDS = {
  "New York, NY":[40.712,-74.006],"Los Angeles, CA":[34.052,-118.244],"Chicago, IL":[41.878,-87.630],
  "Houston, TX":[29.760,-95.370],"Phoenix, AZ":[33.448,-112.074],"Philadelphia, PA":[39.953,-75.163],
  "San Antonio, TX":[29.424,-98.494],"San Diego, CA":[32.716,-117.161],"Dallas, TX":[32.780,-96.801],
  "San Jose, CA":[37.338,-121.886],"Austin, TX":[30.267,-97.743],"Jacksonville, FL":[30.332,-81.656],
  "Fort Worth, TX":[32.755,-97.330],"Columbus, OH":[39.961,-82.999],"Charlotte, NC":[35.227,-80.843],
  "Indianapolis, IN":[39.768,-86.158],"San Francisco, CA":[37.773,-122.432],"Seattle, WA":[47.608,-122.335],
  "Denver, CO":[39.739,-104.984],"Nashville, TN":[36.165,-86.784],"Oklahoma City, OK":[35.467,-97.516],
  "El Paso, TX":[31.761,-106.487],"Las Vegas, NV":[36.175,-115.136],"Louisville, KY":[38.253,-85.759],
  "Baltimore, MD":[39.291,-76.609],"Milwaukee, WI":[43.049,-87.907],"Albuquerque, NM":[35.085,-106.651],
  "Tucson, AZ":[32.222,-110.975],"Fresno, CA":[36.737,-119.787],"Sacramento, CA":[38.576,-121.487],
  "Kansas City, MO":[39.098,-94.582],"Atlanta, GA":[33.749,-84.388],"Omaha, NE":[41.257,-95.934],
  "Colorado Springs, CO":[38.834,-104.821],"Raleigh, NC":[35.787,-78.644],"Long Beach, CA":[33.770,-118.194],
  "Minneapolis, MN":[44.980,-93.265],"Tampa, FL":[27.948,-82.457],"New Orleans, LA":[29.950,-90.066],
  "Wichita, KS":[37.697,-97.316],"Lexington, KY":[38.045,-84.497],"St. Louis, MO":[38.627,-90.199],
  "Pittsburgh, PA":[40.440,-79.996],"Cincinnati, OH":[39.103,-84.512],"Greensboro, NC":[36.073,-79.792],
  "Lincoln, NE":[40.813,-96.702],"Buffalo, NY":[42.887,-78.879],"Fort Wayne, IN":[41.130,-85.128],
  "Orlando, FL":[28.538,-81.380],"Madison, WI":[43.073,-89.401],"Durham, NC":[35.994,-78.899],
  "Lubbock, TX":[33.578,-101.855],"Reno, NV":[39.530,-119.814],"Baton Rouge, LA":[30.458,-91.154],
  "Richmond, VA":[37.541,-77.433],"Des Moines, IA":[41.600,-93.609],"Montgomery, AL":[32.361,-86.279],
  "Shreveport, LA":[32.526,-93.750],"Akron, OH":[41.081,-81.519],"Little Rock, AR":[34.736,-92.331],
  "Augusta, GA":[33.471,-82.011],"Grand Rapids, MI":[42.963,-85.668],"Knoxville, TN":[35.961,-83.921],
  "Salt Lake City, UT":[40.760,-111.891],"Huntsville, AL":[34.730,-86.586],"Worcester, MA":[42.263,-71.803],
  "Providence, RI":[41.824,-71.413],"Dayton, OH":[39.758,-84.192],"Lansing, MI":[42.733,-84.556],
  "Hartford, CT":[41.763,-72.685],"Birmingham, AL":[33.521,-86.803],"Rochester, NY":[43.157,-77.616],
  "Columbia, SC":[34.000,-81.035],"Savannah, GA":[32.081,-81.099],"Chattanooga, TN":[35.046,-85.311],
  "Syracuse, NY":[43.048,-76.148],"Columbia, MO":[38.952,-92.333],"Gainesville, FL":[29.652,-82.325],
  "Boise, ID":[43.615,-116.202],"Tallahassee, FL":[30.455,-84.253],"Waco, TX":[31.549,-97.147],
  "Cedar Rapids, IA":[41.979,-91.662],"South Bend, IN":[41.684,-86.252],"Norman, OK":[35.222,-97.439],
  "Provo, UT":[40.234,-111.659],"Springfield, MO":[37.216,-93.292],"Jackson, MS":[32.298,-90.184],
  "Fort Collins, CO":[40.585,-105.084],"Sioux Falls, SD":[43.549,-96.700],"Boston, MA":[42.360,-71.058],
  "Portland, ME":[43.658,-70.259],"Portland, OR":[45.523,-122.676],"Memphis, TN":[35.149,-90.048],
  "Miami, FL":[25.775,-80.209],"Detroit, MI":[42.331,-83.046],"Cleveland, OH":[41.499,-81.695],
  "Fayetteville, NC":[35.053,-78.878],"Fayetteville, AR":[36.062,-94.158],"Asheville, NC":[35.574,-82.551],
  "Iowa City, IA":[41.661,-91.530],"Champaign, IL":[40.116,-88.243],"Ann Arbor, MI":[42.281,-83.743],
  "Tuscaloosa, AL":[33.210,-87.569],"Eugene, OR":[44.052,-123.087],"College Station, TX":[30.628,-96.334],
  "Ames, IA":[42.034,-93.620],"Bloomington, IN":[39.165,-86.526],"Bloomington, IL":[40.484,-88.994],
  "Lawrence, KS":[38.973,-95.235],"State College, PA":[40.793,-77.860],"Clemson, SC":[34.683,-82.837],
  "Oxford, OH":[39.508,-84.746],"West Lafayette, IN":[40.428,-86.910],"East Lansing, MI":[42.737,-84.484],
  "DeKalb, IL":[41.929,-88.750],"Normal, IL":[40.515,-88.990],"Carbondale, IL":[37.727,-89.218],
  "Mankato, MN":[44.165,-93.999],"Duluth, MN":[46.787,-92.100],"Fargo, ND":[46.877,-96.790],
  "Grand Forks, ND":[47.925,-97.033],"Brookings, SD":[44.312,-96.798],"Kearney, NE":[40.700,-99.082],
  "Manhattan, KS":[39.183,-96.572],"Emporia, KS":[38.404,-96.182],"Warrensburg, MO":[38.762,-93.736],
  "Cape Girardeau, MO":[37.306,-89.518],"Edmond, OK":[35.653,-97.478],"Stillwater, OK":[36.122,-97.058],
  "Denton, TX":[33.215,-97.133],"San Marcos, TX":[29.883,-97.941],"Nacogdoches, TX":[31.604,-94.655],
  "Ruston, LA":[32.523,-92.638],"Monroe, LA":[32.510,-92.120],"Hammond, LA":[30.504,-90.462],
  "Jonesboro, AR":[35.842,-90.704],"Conway, AR":[35.089,-92.442],"Florence, AL":[34.800,-87.677],
  "Jacksonville, AL":[33.814,-85.765],"Troy, AL":[31.810,-85.969],"Florence, SC":[34.195,-79.763],
  "Conway, SC":[33.836,-79.047],"Rock Hill, SC":[34.925,-81.025],"Statesboro, GA":[32.449,-81.783],
  "Carrollton, GA":[33.580,-85.077],"Athens, GA":[33.961,-83.378],"Valdosta, GA":[30.833,-83.278],
  "Albany, GA":[31.579,-84.156],"Kennesaw, GA":[34.023,-84.616],"Marietta, GA":[33.953,-84.550],
  "Harrisonburg, VA":[38.436,-78.869],"Radford, VA":[37.131,-80.576],"Charlottesville, VA":[38.030,-78.480],
  "Blacksburg, VA":[37.229,-80.414],"Norfolk, VA":[36.851,-76.286],"Lynchburg, VA":[37.414,-79.142],
  "Morgantown, WV":[39.634,-79.956],"Huntington, WV":[38.415,-82.445],"Charleston, WV":[38.350,-81.633],
  "Cambridge, MA":[42.374,-71.105],"Amherst, MA":[42.375,-72.519],"Burlington, VT":[44.476,-73.212],
  "Princeton, NJ":[40.357,-74.668],"Ithaca, NY":[42.443,-76.502],"Starkville, MS":[33.460,-88.822],
  "Oxford, MS":[34.366,-89.519],"Pullman, WA":[46.730,-117.180],"Missoula, MT":[46.872,-113.993],
  "Bozeman, MT":[45.680,-111.044],"Flagstaff, AZ":[35.198,-111.651],"Davis, CA":[38.544,-121.741],
  "Santa Barbara, CA":[34.421,-119.699],"Evanston, IL":[42.048,-87.679],"Notre Dame, IN":[41.705,-86.235],
  "Chapel Hill, NC":[35.913,-79.055],"Greenville, SC":[34.852,-82.394],"Boone, NC":[36.217,-81.675],
  "Wilmington, NC":[34.226,-77.946],"Spokane, WA":[47.659,-117.426],"Corvallis, OR":[44.564,-123.263],
  "Pocatello, ID":[42.871,-112.446],"Twin Falls, ID":[42.563,-114.460],"Tempe, AZ":[33.426,-111.940],
  "Logan, UT":[41.730,-111.834],"Ogden, UT":[41.223,-111.974],"Laramie, WY":[41.312,-105.591],
  "Las Cruces, NM":[32.312,-106.778],"Evansville, IN":[37.972,-87.571],"Terre Haute, IN":[39.466,-87.414],
  "Muncie, IN":[40.193,-85.387],"Springfield, IL":[39.798,-89.644],"Peoria, IL":[40.694,-89.589],
  "Clarksville, TN":[36.530,-87.360],"Bowling Green, KY":[36.990,-86.444],"Owensboro, KY":[37.774,-87.113],
  "Midland, TX":[31.997,-102.078],"Abilene, TX":[32.449,-99.733],"Amarillo, TX":[35.222,-101.831],
  "Corpus Christi, TX":[27.800,-97.397],"Killeen, TX":[31.117,-97.728],"Tyler, TX":[32.351,-95.301],
  "Beaumont, TX":[30.086,-94.102],"Plano, TX":[33.020,-96.700],"Garland, TX":[32.913,-96.639],
  "Tulsa, OK":[36.154,-95.993],"Billings, MT":[45.784,-108.501],"Helena, MT":[46.596,-112.027],
  "Cheyenne, WY":[41.140,-104.820],"Casper, WY":[42.867,-106.313],"Rapid City, SD":[44.080,-103.231],
  "Sioux City, IA":[42.500,-96.400],"Davenport, IA":[41.524,-90.578],"Waterloo, IA":[42.497,-92.343],
  "Springfield, MA":[42.101,-72.590],"New Haven, CT":[41.308,-72.928],"Newark, NJ":[40.736,-74.172],
  "Albany, NY":[42.651,-73.755],"Binghamton, NY":[42.099,-75.917],"Scranton, PA":[41.409,-75.665],
  "Erie, PA":[42.129,-80.085],"Harrisburg, PA":[40.265,-76.885],"Lancaster, PA":[40.038,-76.306],
  "Allentown, PA":[40.602,-75.470],"Bethlehem, PA":[40.626,-75.370],"Wilmington, DE":[39.746,-75.547],
  "Alexandria, VA":[38.805,-77.047],"Roanoke, VA":[37.271,-79.941],"Newport News, VA":[37.101,-76.493],
  "Chesapeake, VA":[36.820,-76.287],"Virginia Beach, VA":[36.853,-75.978],"Hampton, VA":[37.031,-76.343],
  "High Point, NC":[35.956,-80.006],"Gastonia, NC":[35.262,-81.187],"Concord, NC":[35.409,-80.580],
  "Spartanburg, SC":[34.946,-81.931],"Charleston, SC":[32.784,-79.940],"Myrtle Beach, SC":[33.689,-78.888],
  "Gainesville, GA":[34.302,-83.824],"Rome, GA":[34.257,-85.165],"Columbus, GA":[32.461,-84.988],
  "Pensacola, FL":[30.421,-87.217],"Daytona Beach, FL":[29.211,-81.023],"Fort Lauderdale, FL":[26.122,-80.143],
  "West Palm Beach, FL":[26.715,-80.053],"Boca Raton, FL":[26.368,-80.128],"Sarasota, FL":[27.337,-82.531],
  "Fort Myers, FL":[26.640,-81.873],"Cape Coral, FL":[26.561,-81.949],"Lakeland, FL":[28.040,-81.951],
  "Ocala, FL":[29.188,-82.140],"Clearwater, FL":[27.966,-82.800],"Mobile, AL":[30.694,-88.043],
  "Dothan, AL":[31.224,-85.390],"Auburn, AL":[32.610,-85.480],"Hattiesburg, MS":[31.329,-89.290],
  "Biloxi, MS":[30.396,-88.886],"Gulfport, MS":[30.367,-89.093],"Tupelo, MS":[34.259,-88.704],
  "Manchester, NH":[42.996,-71.455],"Nashua, NH":[42.766,-71.468],"Concord, NH":[43.208,-71.538],
  "Bellingham, WA":[48.746,-122.476],"Yakima, WA":[46.602,-120.505],"Olympia, WA":[47.042,-122.893],
  "Bend, OR":[44.058,-121.315],"Medford, OR":[42.327,-122.875],"Salem, OR":[44.942,-123.030],
  "Rexburg, ID":[43.826,-111.790],"Nampa, ID":[43.541,-116.567],"Idaho Falls, ID":[43.492,-112.034],
  "Henderson, NV":[36.040,-114.982],"Carson City, NV":[39.164,-119.767],"St. George, UT":[37.105,-113.584],
  "Mesa, AZ":[33.415,-111.831],"Chandler, AZ":[33.303,-111.841],"Scottsdale, AZ":[33.494,-111.926],
  "Gilbert, AZ":[33.353,-111.789],"Glendale, AZ":[33.539,-112.186],"Prescott, AZ":[34.540,-112.469],
  "Surprise, AZ":[33.630,-112.368],"Yuma, AZ":[32.693,-114.628],"Aurora, CO":[39.729,-104.832],
  "Lakewood, CO":[39.705,-105.082],"Thornton, CO":[39.868,-104.972],"Pueblo, CO":[38.255,-104.609],
  "Boulder, CO":[40.015,-105.270],"Greeley, CO":[40.423,-104.709],"Loveland, CO":[40.398,-105.075],
  "Aurora, IL":[41.760,-88.320],"Rockford, IL":[42.271,-89.094],"Joliet, IL":[41.525,-88.082],
  "Naperville, IL":[41.786,-88.148],"Elgin, IL":[42.037,-88.281],"Waukegan, IL":[42.364,-87.845],
  "Independence, MO":[39.091,-94.415],"Joplin, MO":[37.084,-94.513],"St. Joseph, MO":[39.769,-94.847],
  "Overland Park, KS":[38.983,-94.671],"Olathe, KS":[38.879,-94.820],"Salina, KS":[38.840,-97.611],
  "Topeka, KS":[39.048,-95.678],"Grand Island, NE":[40.925,-98.342],"Fremont, NE":[41.433,-96.498],
  "Council Bluffs, IA":[41.262,-95.861],"Dubuque, IA":[42.500,-90.664],"La Crosse, WI":[43.801,-91.240],
  "Eau Claire, WI":[44.812,-91.499],"Oshkosh, WI":[44.025,-88.543],"Green Bay, WI":[44.520,-88.016],
  "Kenosha, WI":[42.585,-87.821],"Racine, WI":[42.728,-87.783],"Appleton, WI":[44.262,-88.415],
  "Janesville, WI":[42.683,-89.019],"St. Paul, MN":[44.954,-93.090],"Rochester, MN":[44.022,-92.470],
  "St. Cloud, MN":[45.560,-94.163],"Moorhead, MN":[46.873,-96.768],"Bismarck, ND":[46.808,-100.784],
  "Lowell, MA":[42.633,-71.317],"Brockton, MA":[42.083,-71.018],"New Bedford, MA":[41.636,-70.934],
  "Quincy, MA":[42.252,-71.002],"Lynn, MA":[42.467,-70.943],"Fall River, MA":[41.701,-71.155],
  "Newton, MA":[42.337,-71.209],"Somerville, MA":[42.388,-71.100],"Cranston, RI":[41.780,-71.438],
  "Warwick, RI":[41.700,-71.418],"Lewiston, ME":[44.100,-70.215],"Bangor, ME":[44.801,-68.778],
  "Kirksville, MO":[40.195,-92.583],"Warrensburg, MO":[38.762,-93.736],"Rolla, MO":[37.951,-91.771],
  "St. Cloud, MN":[45.560,-94.163],"Bemidji, MN":[47.475,-94.880],"Winona, MN":[44.050,-91.639],
  "Aberdeen, SD":[45.464,-98.486],"Vermillion, SD":[42.779,-96.929],"Wayne, NE":[42.233,-97.017],
  "Hays, KS":[38.879,-99.326],"Pittsburg, KS":[37.411,-94.705],"Tahlequah, OK":[35.915,-94.970],
  "Stephenville, TX":[32.220,-98.202],"Commerce, TX":[33.249,-95.902],"Huntsville, TX":[30.724,-95.551],
  "Natchitoches, LA":[31.761,-93.087],"Thibodaux, LA":[29.797,-90.814],"Grambling, LA":[32.526,-92.718],
  "Monticello, AR":[33.630,-91.790],"Magnolia, AR":[33.267,-93.240],"Arkadelphia, AR":[34.120,-93.054],
  "Russellville, AR":[35.279,-93.134],"Pine Bluff, AR":[34.229,-92.003],"Livingston, AL":[32.587,-88.191],
  "Montevallo, AL":[33.101,-86.865],"Milledgeville, GA":[33.080,-83.231],"Dahlonega, GA":[34.532,-83.988],
  "Warner Robins, GA":[32.614,-83.600],"Farmville, VA":[37.302,-78.395],"Fredericksburg, VA":[38.304,-77.461],
  "Shippensburg, PA":[40.050,-77.521],"Bloomsburg, PA":[41.005,-76.455],"Lock Haven, PA":[41.138,-77.448],
  "Slippery Rock, PA":[41.064,-80.057],"Indiana, PA":[40.621,-79.152],"Millersville, PA":[39.999,-76.360],
  "West Chester, PA":[39.958,-75.606],"Edinboro, PA":[41.877,-80.126],"Cheney, WA":[47.488,-117.576],
  "Ellensburg, WA":[46.997,-120.548],"Monmouth, OR":[44.849,-123.231],"Ashland, OR":[42.195,-122.710],
  "La Grande, OR":[45.325,-118.088],"Caldwell, ID":[43.663,-116.688],"Lewiston, ID":[46.416,-117.018],
  "Cedar City, UT":[37.677,-113.061],"West Valley City, UT":[40.689,-112.001],"Logan, UT":[41.730,-111.834],
};

const STATE_CAPITALS = {
  AL:[32.361,-86.279],AK:[61.218,-149.900],AZ:[33.448,-112.074],AR:[34.746,-92.289],
  CA:[38.576,-121.487],CO:[39.739,-104.984],CT:[41.764,-72.685],DE:[39.157,-75.524],
  FL:[30.455,-84.253],GA:[33.749,-84.388],HI:[21.307,-157.858],ID:[43.615,-116.202],
  IL:[39.798,-89.654],IN:[39.768,-86.158],IA:[41.600,-93.609],KS:[39.048,-95.678],
  KY:[38.253,-85.759],LA:[30.443,-91.187],ME:[43.661,-70.255],MD:[38.972,-76.501],
  MA:[42.360,-71.058],MI:[42.733,-84.556],MN:[44.954,-93.090],MS:[32.299,-90.184],
  MO:[38.572,-92.189],MT:[46.596,-112.027],NE:[40.813,-96.702],NV:[39.161,-119.754],
  NH:[43.208,-71.538],NJ:[40.221,-74.756],NM:[35.667,-105.964],NY:[42.651,-73.755],
  NC:[35.771,-78.638],ND:[46.813,-100.779],OH:[39.961,-82.999],OK:[35.467,-97.516],
  OR:[44.931,-123.029],PA:[40.270,-76.887],RI:[41.824,-71.413],SC:[34.000,-81.035],
  SD:[44.368,-100.352],TN:[36.165,-86.784],TX:[30.267,-97.743],UT:[40.760,-111.891],
  VT:[44.476,-73.212],VA:[37.541,-77.433],WA:[47.042,-122.893],WV:[38.350,-81.633],
  WI:[43.073,-89.412],WY:[41.140,-104.820],DC:[38.907,-77.036],
};

const getState = (hometown) => {
  if (!hometown) return null;
  const parts = hometown.split(", ");
  return parts.length >= 2 ? parts[parts.length - 1].trim() : null;
};

// ── HOMETOWN COORDINATE RESOLVER ──────────────────────────────────────────────
// Returns null if no real hometown — callers use this to skip rendering
function resolveHometownCoords(hometown) {
  if (!hometown || typeof hometown !== "string") return null;
  const s = hometown.trim();
  const match = s.match(/^([A-Za-z][A-Za-z\s\.\-']{1,40}),\s+([A-Z]{2})$/);
  if (!match) return null;
  const city = match[1].trim();
  const state = match[2];
  // Word boundaries prevent rejecting Boston, Houston, Washington, Lexington, etc.
  const bad = /\b(championship|championships|invitational|invit|classic|relays?|cross.?country|indoor|outdoor|university|college)\b/i;
  if (bad.test(city)) return null;
  if (!STATE_CAPITALS[state]) return null;
  // Use real city coords if known, fall back to state centroid
  return CITY_COORDS[s] || STATE_CAPITALS[state];
}

// ── SUPABASE DATA TRANSFORMER ─────────────────────────────────────────────────
const RELAY_EVENTS = new Set(["4x100", "4x400", "4x100m", "4x400m", "XC", "DMR", "SMR"]);

function transformAthlete(raw, index) {
  const perfs = raw.performances || [];
  const collegeTimes = {};
  const hsTimes = {};
  const rawPerformances = [];
  const PERF_ALIASES = {
    "60H":"60mH","110H":"110mH","400H":"400mH","100H":"100mH",
    "Steeple":"3000SC","3000S":"3000SC","Steeplechase":"3000SC",
    "High Jump":"HJ","Long Jump":"LJ","Triple Jump":"TJ",
    "Pole Vault":"PV","Shot Put":"SP","Discus":"DT",
    "Javelin":"JT","Weight Throw":"WT","Hammer":"HT",
  };
  perfs.forEach(p => {
    if (!p.mark || !p.event) return;
    p = {...p, event: PERF_ALIASES[p.event] || p.event};
    if (RELAY_EVENTS.has(p.event)) return;
    rawPerformances.push(p);
    const bucket = p.level === "hs" ? hsTimes : collegeTimes;
    const isBetter = isFieldEvent(p.event)
      ? (!bucket[p.event] || p.mark > bucket[p.event])
      : (!bucket[p.event] || p.mark < bucket[p.event]);
    if (isBetter) bucket[p.event] = p.mark;
  });
  const college = raw.college || "Unknown";
  // Supabase may return events as:
  // - JS array already: ["Mile","800m"]
  // - JSON string: '["Mile","800m"]'
  // - PG array string: '{Mile,800m}'
  // - null/undefined
  let eventsRaw = raw.events || [];
  if (typeof eventsRaw === "string") {
    const s = eventsRaw.trim();
    if (s.startsWith("[")) {
      try { eventsRaw = JSON.parse(s); } catch(e) { eventsRaw = []; }
    } else {
      eventsRaw = s.replace(/^\{|\}$/g, "").split(",").map(s => s.trim()).filter(Boolean);
    }
  }
  if (!Array.isArray(eventsRaw)) eventsRaw = [];

  // Normalize hurdle event names to match EVENTS_CFG
  const EVENT_ALIASES = {
    "60H":"60mH","60mh":"60mH","60MH":"60mH",
    "100H":"100mH","100mh":"100mH",
    "110H":"110mH","110mh":"110mH","110MH":"110mH",
    "400H":"400mH","400mh":"400mH","400MH":"400mH",
    "Steeple":"3000SC","3000S":"3000SC","3kS":"3000SC",
    "Steeplechase":"3000SC","3000 Steeplechase":"3000SC",
    "High Jump":"HJ","Long Jump":"LJ","Triple Jump":"TJ",
    "Pole Vault":"PV","Shot Put":"SP","Discus":"DT",
    "Javelin":"JT","Weight Throw":"WT","Hammer":"HT",
    "Pentathlon":"Pent","Heptathlon":"Hept","Decathlon":"Dec",
  };
  eventsRaw = eventsRaw.map(e => EVENT_ALIASES[e] || e);
  const eventsFromColumn = new Set(eventsRaw.filter(e => !RELAY_EVENTS.has(e)));
  // Also derive events from actual performances (source of truth)
  // This catches events the scraper missed in the events column
  const eventsFromPerfs = new Set(
    rawPerformances.map(p => p.event).filter(e => e && !RELAY_EVENTS.has(e))
  );
  // Merge both sources
  const events = [...new Set([...eventsFromColumn, ...eventsFromPerfs])];

  // Only use hometown if it's a real "City, ST" value
  const hometown = raw.hometown || "";
  const hometownCoords = resolveHometownCoords(hometown);

  return {
    id: raw.id || `idx_${index}`,
    name: raw.name || "Unknown",
    hometown,
    hometownCoords,             // null means no real data — map will skip this athlete
    hasHometown: !!hometownCoords,
    hsName: raw.high_school || "",
    college,
    conference: raw.conference || "",
    events,
    hsTimes,
    collegeTimes,
    rawPerformances,
    hsYear: raw.hs_grad_year || null,
    collegeYear: raw.college_year || null,
    gender: raw.gender || "M",
    collegeCoords: getCollegeCoords(college),
    tfrrsUrl: raw.tfrrs_url || null,
  };
}

const HS_YEARS = [2019, 2020, 2021, 2022, 2023, 2024, 2025];
const COLLEGE_YEARS = [1, 2, 3, 4];
const ALL_STATE_ABBRS = Object.keys(STATE_NAMES).sort((a, b) => STATE_NAMES[a].localeCompare(STATE_NAMES[b]));

// ── UTILS ─────────────────────────────────────────────────────────────────────
const haversine = (p1,p2) => { if(!p1||!p2) return 0; const [a,b]=p1,[c,d]=p2,R=3958.8,dL=(c-a)*Math.PI/180,dl=(d-b)*Math.PI/180,x=Math.sin(dL/2)**2+Math.cos(a*Math.PI/180)*Math.cos(c*Math.PI/180)*Math.sin(dl/2)**2; return R*2*Math.atan2(Math.sqrt(x),Math.sqrt(1-x)); };
const fmtDist = d => d ? `${Math.round(d).toLocaleString()} mi` : "—";
const fmtTime = v => { if (!v) return "—"; if (v > 200) { const m=Math.floor(v/60); return `${m}:${(v%60).toFixed(2).padStart(5,"0")}`; } return v.toFixed(2); };
const distBucket = d => d<100?"local":d<400?"regional":d<800?"far":"extreme";
const DIST_COLORS = {local:"#22C55E", regional:"#3B82F6", far:"#F76900", extreme:"#EF4444"};
const distColor = d => !d ? T.dim : d<100?DIST_COLORS.local:d<400?DIST_COLORS.regional:d<800?DIST_COLORS.far:DIST_COLORS.extreme;
const distLabel = d => !d ? "Unknown" : d<100?"Local (<100 mi)":d<400?"Regional (100-400 mi)":d<800?"Long Haul (400-800 mi)":"Cross-Country (800+ mi)";

// ── HEATMAP CANVAS ────────────────────────────────────────────────────────────
function drawHeatmap(canvas, athletes, projection) {
  if (!canvas || !projection || athletes.length === 0) return;
  // Always sync canvas pixel dims to its CSS display size first
  const rect = canvas.getBoundingClientRect();
  if (rect.width > 10) { canvas.width = Math.round(rect.width); canvas.height = Math.round(rect.height); }
  const W = canvas.width, H = canvas.height;
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, W, H);

  const pts = athletes
    .filter(a => a.hometownCoords)
    .map(a => projection([a.hometownCoords[1], a.hometownCoords[0]]))
    .filter(Boolean);
  if (!pts.length) return;

  const density = new Float32Array(W * H);

  // Pass 1: tight city-level kernel
  const R1 = Math.max(10, Math.round(W * 0.012)), bw1 = R1 / 1.5;
  pts.forEach(([px, py]) => {
    const x0=Math.max(0,(px-R1)|0), x1=Math.min(W-1,(px+R1+1)|0);
    const y0=Math.max(0,(py-R1)|0), y1=Math.min(H-1,(py+R1+1)|0);
    for (let y=y0;y<=y1;y++) for (let x=x0;x<=x1;x++) {
      const d2=(x-px)*(x-px)+(y-py)*(y-py);
      density[y*W+x] += Math.exp(-d2/(2*bw1*bw1));
    }
  });

  // Pass 2: wide regional kernel — NOAA-style smooth blending
  const R2 = Math.max(40, Math.round(W * 0.042)), bw2 = R2 / 1.9;
  pts.forEach(([px, py]) => {
    const x0=Math.max(0,(px-R2)|0), x1=Math.min(W-1,(px+R2+1)|0);
    const y0=Math.max(0,(py-R2)|0), y1=Math.min(H-1,(py+R2+1)|0);
    for (let y=y0;y<=y1;y++) for (let x=x0;x<=x1;x++) {
      const d2=(x-px)*(x-px)+(y-py)*(y-py);
      density[y*W+x] += 0.38 * Math.exp(-d2/(2*bw2*bw2));
    }
  });

  const vals = Array.from(density).filter(v=>v>0).sort((a,b)=>a-b);
  const mx = vals[Math.floor(vals.length * 0.995)] || vals[vals.length-1] || 1;

  // Warm palette: white → peach → salmon → brick → dark red (matches NOAA reference)
  const STOPS = [
    [0.00, null],
    [0.02, [255,248,244, 20]],
    [0.07, [252,222,205, 70]],
    [0.16, [243,188,160,120]],
    [0.28, [228,148,108,158]],
    [0.44, [202, 96, 58,190]],
    [0.63, [172, 48, 22,212]],
    [0.82, [138, 16,  5,228]],
    [1.00, [ 98,  3,  1,242]],
  ];
  const lerp=(a,b,t)=>a+(b-a)*t;
  const img = ctx.createImageData(W, H);
  for (let i=0; i<density.length; i++) {
    const t = Math.min(1, density[i]/mx);
    if (t < STOPS[1][0]) continue;
    let s0=STOPS[1], s1=STOPS[2];
    for (let k=1; k<STOPS.length-1; k++) {
      if (t>=STOPS[k][0] && t<=STOPS[k+1][0]) { s0=STOPS[k]; s1=STOPS[k+1]; break; }
    }
    if (t > STOPS[STOPS.length-1][0]) { s0=STOPS[STOPS.length-2]; s1=STOPS[STOPS.length-1]; }
    const f = s1[0]===s0[0] ? 1 : (t-s0[0])/(s1[0]-s0[0]);
    const c0=s0[1], c1=s1[1], ii=i*4;
    img.data[ii]  =Math.round(lerp(c0[0],c1[0],f));
    img.data[ii+1]=Math.round(lerp(c0[1],c1[1],f));
    img.data[ii+2]=Math.round(lerp(c0[2],c1[2],f));
    img.data[ii+3]=Math.round(lerp(c0[3],c1[3],f));
  }
  ctx.putImageData(img, 0, 0);
}

// ── US MAP ────────────────────────────────────────────────────────────────────
function USMap({athletes, onAthleteClick, selectedAthlete, highlightCollege, highlightHometown, mapMode, selectedStates}) {
  const svgRef=useRef(null), canvasRef=useRef(null), projRef=useRef(null), containerRef=useRef(null);
  const [geo,setGeo]=useState(null), [tooltip,setTooltip]=useState(null), [dims,setDims]=useState({W:960,H:560});
  const FIPS_ABBR={"01":"AL","02":"AK","04":"AZ","05":"AR","06":"CA","08":"CO","09":"CT","10":"DE","11":"DC","12":"FL","13":"GA","15":"HI","16":"ID","17":"IL","18":"IN","19":"IA","20":"KS","21":"KY","22":"LA","23":"ME","24":"MD","25":"MA","26":"MI","27":"MN","28":"MS","29":"MO","30":"MT","31":"NE","32":"NV","33":"NH","34":"NJ","35":"NM","36":"NY","37":"NC","38":"ND","39":"OH","40":"OK","41":"OR","42":"PA","44":"RI","45":"SC","46":"SD","47":"TN","48":"TX","49":"UT","50":"VT","51":"VA","53":"WA","54":"WV","55":"WI","56":"WY"};

  useEffect(() => {
    const load = tj => { fetch("https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json").then(r=>r.json()).then(us=>setGeo(tj.feature(us,us.objects.states))); };
    if (window.topojson) { load(window.topojson); return; }
    const sc=document.createElement("script"); sc.src="https://cdn.jsdelivr.net/npm/topojson-client@3/dist/topojson-client.min.js"; sc.onload=()=>load(window.topojson); document.head.appendChild(sc);
  }, []);

  // ResizeObserver prevents canvas warping when panels open/close
  useEffect(() => {
    if (!containerRef.current) return;
    const ro = new ResizeObserver(e => {
      for (const entry of e) {
        const W=Math.round(entry.contentRect.width), H=Math.round(entry.contentRect.height);
        if (W>10&&H>10) { setDims({W,H}); if(canvasRef.current){canvasRef.current.width=W;canvasRef.current.height=H;} }
      }
    });
    ro.observe(containerRef.current);
    return ()=>ro.disconnect();
  }, []);

  useEffect(() => {
    if (!geo || !svgRef.current) return;
    const svg=d3.select(svgRef.current);
    const W=svgRef.current.clientWidth||960, H=svgRef.current.clientHeight||560;
    svg.selectAll("*").remove();
    const proj=d3.geoAlbersUsa().fitSize([W,H],geo);
    projRef.current=proj;
    const path=d3.geoPath().projection(proj);
    const g=svg.append("g");
    const px=(coord)=>{ if(!coord) return null; const [lat,lon]=coord; return proj([lon,lat]); };
    const hasStateFilter = selectedStates.length > 0;

    g.selectAll("path.state").data(geo.features).join("path")
      .attr("class","state").attr("d",path)
      .attr("fill",d=>{
        if(mapMode==="heatmap")return"#FFFFFF";
        const abbr=FIPS_ABBR[String(d.id).padStart(2,"0")];
        if(!hasStateFilter)return"#E6E7EE";
        return selectedStates.includes(abbr)?"#FCC399":"#F1F2F5";
      })
      .attr("stroke",d=>{
        if(mapMode==="heatmap")return"rgba(110,100,95,0.4)";
        const abbr=FIPS_ABBR[String(d.id).padStart(2,"0")];
        return hasStateFilter&&selectedStates.includes(abbr)?T.orange:"#CCCFDD";
      })
      .attr("stroke-width",d=>{
        if(mapMode==="heatmap")return 0.6;
        const abbr=FIPS_ABBR[String(d.id).padStart(2,"0")];
        return hasStateFilter&&selectedStates.includes(abbr)?2:0.8;
      })
      .style("cursor","default");

    if (mapMode === "heatmap") return;

    try {
    const stateFiltered = (hasStateFilter && mapMode==="flows")
      ? athletes.filter(a=>selectedStates.includes(getState(a.hometown)))
      : athletes;

    const anyFocus=highlightCollege||highlightHometown;
    const active=anyFocus?stateFiltered.filter(a=>(highlightCollege?a.college===highlightCollege:true)&&(highlightHometown?a.hometown===highlightHometown:true)):stateFiltered;
    const dimmed=athletes.filter(a=>!stateFiltered.includes(a));

    if (mapMode === "flows") {
      // Cap dimmed arcs to avoid visual overload — shuffle and take first 300
      const dimmedCapped = dimmed.length > 300
        ? dimmed.slice().sort(()=>Math.random()-0.5).slice(0,300)
        : dimmed;
      dimmedCapped.forEach(a=>{
        const h=px(a.hometownCoords),c=px(a.collegeCoords); if(!h||!c) return;
        const dx=c[0]-h[0],dy=c[1]-h[1],dr=Math.sqrt(dx*dx+dy*dy)*0.42;
        g.append("path").attr("d",`M${h[0]},${h[1]} A${dr},${dr} 0 0,1 ${c[0]},${c[1]}`).attr("fill","none").attr("stroke","rgba(0,0,0,0.03)").attr("stroke-width",0.7);
      });
      active.forEach(a=>{
        const h=px(a.hometownCoords),c=px(a.collegeCoords); if(!h||!c) return;
        if(!a.hometownCoords) return;
        const dist=haversine(a.hometownCoords,a.collegeCoords), col=distColor(dist), isSel=selectedAthlete?.id===a.id;
        const dx=c[0]-h[0],dy=c[1]-h[1],dr=Math.sqrt(dx*dx+dy*dy)*0.42;
        const arc=`M${h[0]},${h[1]} A${dr},${dr} 0 0,1 ${c[0]},${c[1]}`;
        if (isSel) g.append("path").attr("d",arc).attr("fill","none").attr("stroke",`${T.orange}55`).attr("stroke-width",8);
        g.append("path").attr("d",arc).attr("fill","none").attr("stroke",isSel?T.orange:col).attr("stroke-width",isSel?2.5:1.6).attr("stroke-opacity",isSel?1:0.82).style("cursor","pointer")
          .on("mouseover",function(ev){d3.select(this).attr("stroke-width",3).attr("stroke-opacity",1);setTooltip({x:ev.offsetX,y:ev.offsetY,a,dist:Math.round(dist)});})
          .on("mouseout",function(){d3.select(this).attr("stroke-width",isSel?2.5:1.6).attr("stroke-opacity",isSel?1:0.82);setTooltip(null);})
          .on("click",ev=>{ev.stopPropagation();onAthleteClick(a);});
        const sz=isSel?5.5:3.5;
        g.append("circle").attr("cx",h[0]).attr("cy",h[1]).attr("r",sz).attr("fill",col).attr("stroke","#FFFFFF").attr("stroke-width",1.5).attr("opacity",0.95);
        const s=isSel?7:5;
        g.append("rect").attr("x",c[0]-s/2).attr("y",c[1]-s/2).attr("width",s).attr("height",s).attr("fill",col).attr("stroke","#FFFFFF").attr("stroke-width",1.5).attr("opacity",0.95).attr("rx",1);
      });
    } else {
      athletes.forEach(a=>{
        const coord=mapMode==="college"?a.collegeCoords:a.hometownCoords;
        const p=px(coord); if(!p) return;
        const isSel=selectedAthlete?.id===a.id, isAct=active.includes(a), rr=isSel?9:5.5;
        if (isSel) g.append("circle").attr("cx",p[0]).attr("cy",p[1]).attr("r",16).attr("fill",`${T.orange}20`).attr("stroke","none");
        g.append("circle").attr("cx",p[0]).attr("cy",p[1]).attr("r",rr)
          .attr("fill",isSel?T.orange:isAct?T.orangeL:"rgba(200,120,60,0.2)")
          .attr("stroke",isSel?T.white:"#FFFFFF").attr("stroke-width",isSel?2:1).attr("opacity",isAct?0.9:0.25)
          .style("cursor","pointer")
          .on("mouseover",function(ev){d3.select(this).attr("r",rr+3);setTooltip({x:ev.offsetX,y:ev.offsetY,a,dist:a.hometownCoords?Math.round(haversine(a.hometownCoords,a.collegeCoords)):0});})
          .on("mouseout",function(){d3.select(this).attr("r",rr);setTooltip(null);})
          .on("click",()=>onAthleteClick(a));
      });
    }
    } catch(e) { console.error("Map render error:", e.message, e.stack); }
  }, [geo,athletes,selectedAthlete,highlightCollege,highlightHometown,mapMode,selectedStates,dims]);

  useEffect(() => {
    if (!canvasRef.current) return;
    if (mapMode !== "heatmap" || !projRef.current) {
      canvasRef.current.getContext("2d").clearRect(0,0,canvasRef.current.width,canvasRef.current.height);
      return;
    }
    drawHeatmap(canvasRef.current, athletes, projRef.current);
  }, [mapMode, athletes, geo, dims]);

  return (
    <div ref={containerRef} style={{position:"relative",width:"100%",height:"100%"}}>
      <canvas ref={canvasRef} style={{position:"absolute",top:0,left:0,pointerEvents:"none",width:"100%",height:"100%",display:mapMode==="heatmap"?"block":"none"}}
        width={dims.W} height={dims.H}/>
      <svg ref={svgRef} style={{position:"absolute",top:0,left:0,width:"100%",height:"100%",display:"block"}}/>
      {tooltip && (
        <div style={{position:"absolute",left:tooltip.x+14,top:tooltip.y-8,background:"#FFFFFF",border:`1px solid ${T.orange}`,borderRadius:9,padding:"10px 14px",pointerEvents:"none",zIndex:100,boxShadow:`0 6px 28px rgba(247,105,0,0.18)`,minWidth:180}}>
          <div style={{color:T.orange,fontWeight:800,fontSize:14,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>{tooltip.a.name}</div>
          <div style={{color:T.offWhite,fontSize:11,marginTop:2}}>{tooltip.a.hometown}</div>
          <div style={{color:T.muted,fontSize:11}}>to {tooltip.a.college} ({tooltip.a.conference})</div>
          <div style={{display:"flex",alignItems:"center",gap:8,marginTop:6}}>
            <div style={{width:8,height:8,borderRadius:"50%",background:distColor(tooltip.dist)}}/>
            <span style={{color:distColor(tooltip.dist),fontSize:13,fontWeight:800,fontFamily:"monospace"}}>{fmtDist(tooltip.dist)}</span>
          </div>
          <div style={{color:T.dim,fontSize:10,marginTop:3}}>{tooltip.a.events.slice(0,3).join(" · ")}</div>
        </div>
      )}
    </div>
  );
}

// ── UI ATOMS ──────────────────────────────────────────────────────────────────
const Chip = ({label,active,onClick,color}) => (
  <button onClick={onClick} style={{background:active?`rgba(${color||"247,105,0"},0.18)`:"rgba(255,255,255,0.03)",border:`1px solid ${active?`rgba(${color||"247,105,0"},0.9)`:T.border}`,color:active?`rgb(${color||"247,105,0"})`:T.muted,borderRadius:4,padding:"3px 8px",fontSize:11,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,cursor:"pointer",transition:"all 0.12s",whiteSpace:"nowrap"}}>{label}</button>
);

const Sel = ({value,onChange,options,placeholder}) => (
  <select value={value} onChange={e=>onChange(e.target.value)} style={{background:T.bgCard,border:`1px solid ${T.border}`,color:value?T.offWhite:T.muted,borderRadius:6,padding:"5px 8px",fontSize:12,width:"100%",cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,outline:"none"}}>
    <option value="">{placeholder}</option>
    {options.map(o=><option key={o.value||o} value={o.value||o}>{o.label||o}</option>)}
  </select>
);

const StatCard = ({label,value,color}) => (
  <div style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:7,padding:"8px 10px"}}>
    <div style={{color:T.muted,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:2,textTransform:"uppercase"}}>{label}</div>
    <div style={{color:color||T.orange,fontSize:16,fontWeight:800,fontFamily:"'Barlow Condensed',sans-serif",marginTop:2}}>{value}</div>
  </div>
);

const SectionHead = ({children}) => (
  <div style={{display:"flex",alignItems:"center",gap:8,margin:"14px 0 8px"}}>
    <div style={{flex:1,height:1,background:`${T.orange}28`}}/>
    <span style={{color:T.orange,fontSize:10,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:2,textTransform:"uppercase"}}>{children}</span>
    <div style={{flex:1,height:1,background:`${T.orange}28`}}/>
  </div>
);

function DistBar({athletes}) {
  const total=athletes.length; if (!total) return null;
  const b={local:0,regional:0,far:0,extreme:0};
  athletes.forEach(a=>{ if(!a.hometownCoords) return; b[distBucket(haversine(a.hometownCoords,a.collegeCoords))]++;});
  const cols={local:T.green,regional:T.yellow,far:T.orange,extreme:T.red};
  const lbl={local:"<100",regional:"100-400",far:"400-800",extreme:"800+"};
  return (
    <div>
      <div style={{display:"flex",height:6,borderRadius:3,overflow:"hidden",gap:1,marginBottom:5}}>
        {Object.entries(b).filter(([,n])=>n>0).map(([k,n])=><div key={k} style={{flex:n,background:cols[k],opacity:0.85,minWidth:2}}/>)}
      </div>
      <div style={{display:"flex",gap:8,flexWrap:"wrap"}}>
        {Object.entries(b).map(([k,n])=>(
          <div key={k} style={{display:"flex",gap:4,alignItems:"center"}}>
            <div style={{width:6,height:6,borderRadius:"50%",background:cols[k]}}/>
            <span style={{color:T.muted,fontSize:9}}>{lbl[k]}</span>
            <span style={{color:T.offWhite,fontSize:9,fontWeight:700}}>{n}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── STATE FILTER DROPDOWN ─────────────────────────────────────────────────────
function StateFilterDropdown({selectedStates, onChange}) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const dropRef = useRef(null);

  useEffect(() => {
    const handler = e => { if (dropRef.current && !dropRef.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const filtered = ALL_STATE_ABBRS.filter(abbr =>
    STATE_NAMES[abbr].toLowerCase().includes(search.toLowerCase()) ||
    abbr.toLowerCase().includes(search.toLowerCase())
  );
  const toggle = abbr => onChange(selectedStates.includes(abbr) ? selectedStates.filter(s=>s!==abbr) : [...selectedStates, abbr]);
  const count = selectedStates.length;

  return (
    <div ref={dropRef} style={{position:"relative",marginBottom:9}}>
      <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:5}}>
        Origin State {count>0&&<span style={{color:T.orange,fontWeight:800}}>({count} selected)</span>}
      </div>
      <button onClick={()=>setOpen(o=>!o)} style={{width:"100%",display:"flex",justifyContent:"space-between",alignItems:"center",background:T.bg,border:`1px solid ${count>0?T.orange:T.border}`,borderRadius:6,padding:"5px 8px",cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",fontSize:12,color:count>0?T.orange:T.muted,letterSpacing:1}}>
        <span>{count===0?"All States":count===1?STATE_NAMES[selectedStates[0]]:`${count} States`}</span>
        <span style={{fontSize:9,opacity:0.6}}>{open?"▲":"▼"}</span>
      </button>
      {open && (
        <div style={{position:"absolute",top:"100%",left:0,right:0,zIndex:200,background:T.bg,border:`1px solid ${T.border}`,borderRadius:7,boxShadow:"0 8px 24px rgba(0,0,0,0.12)",marginTop:3,display:"flex",flexDirection:"column",maxHeight:280,overflow:"hidden"}}>
          <div style={{padding:"7px 8px",borderBottom:`1px solid ${T.border}`,flexShrink:0}}>
            <input autoFocus value={search} onChange={e=>setSearch(e.target.value)} placeholder="Search states..."
              style={{width:"100%",background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:5,padding:"4px 8px",fontSize:11,color:T.offWhite,fontFamily:"'Barlow',sans-serif",outline:"none"}}/>
          </div>
          <div style={{display:"flex",gap:6,padding:"5px 8px",borderBottom:`1px solid ${T.border}`,flexShrink:0}}>
            <button onClick={()=>onChange(ALL_STATE_ABBRS)} style={{flex:1,background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:4,padding:"3px 0",fontSize:10,color:T.orange,cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>SELECT ALL</button>
            <button onClick={()=>onChange([])} style={{flex:1,background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:4,padding:"3px 0",fontSize:10,color:T.muted,cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>SELECT NONE</button>
          </div>
          <div style={{overflowY:"auto",flex:1}}>
            {filtered.length===0 ? (
              <div style={{padding:"12px 8px",color:T.dim,fontSize:11,textAlign:"center"}}>No states found</div>
            ) : filtered.map(abbr => {
              const checked = selectedStates.includes(abbr);
              return (
                <label key={abbr} style={{display:"flex",alignItems:"center",gap:8,padding:"5px 10px",cursor:"pointer",background:checked?`${T.orange}08`:"transparent",borderBottom:`1px solid ${T.border}22`}}>
                  <div style={{width:14,height:14,borderRadius:3,flexShrink:0,background:checked?T.orange:T.bg,border:`2px solid ${checked?T.orange:T.border}`,display:"flex",alignItems:"center",justifyContent:"center",transition:"all 0.1s"}}>
                    {checked && <span style={{color:"#fff",fontSize:9,lineHeight:1,fontWeight:900}}>✓</span>}
                  </div>
                  <input type="checkbox" checked={checked} onChange={()=>toggle(abbr)} style={{display:"none"}}/>
                  <span style={{color:checked?T.orange:T.offWhite,fontSize:12,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:0.5,fontWeight:checked?700:400}}>{STATE_NAMES[abbr]}</span>
                  <span style={{color:T.dim,fontSize:10,fontFamily:"monospace",marginLeft:"auto"}}>{abbr}</span>
                </label>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

// ── FILTER CONTROLS ───────────────────────────────────────────────────────────
function FilterControls({filters, setFilters, showSeason=false, selectedStates=[], onStatesChange=()=>{}, mapMode="flows", allConferences=[], getConfColleges=()=>[], allAthletes=[], performanceRanges={}, onRangeChange=()=>{}}) {
  const confColleges = getConfColleges(filters.conference);
  const season = filters.season || "all";
  const visibleEvents = EVENTS_CFG.filter(e => season==="all" || e.season==="both" || e.season===season);
  const toggleEvent = ev => setFilters(f=>({...f,events:f.events.includes(ev)?f.events.filter(e=>e!==ev):[...f.events,ev]}));
  const setConf = v => setFilters(f=>({...f,conference:v,college:""}));

  return (
    <div>
      <div style={{marginBottom:11}}>
        <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>Gender</div>
        <div style={{display:"flex",gap:3}}>
          {[["","All"],["M","Men"],["F","Women"]].map(([v,l])=>(
            <Chip key={v} label={l} active={(filters.gender||"")===v} onClick={()=>setFilters(f=>({...f,gender:v}))} color="247,105,0"/>
          ))}
        </div>
      </div>
      {showSeason && (
        <div style={{marginBottom:11}}>
          <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>Season</div>
          <div style={{display:"flex",gap:3}}>
            {[["all","All"],["indoor","Indoor"],["outdoor","Outdoor"]].map(([v,l])=>(
              <Chip key={v} label={l} active={season===v} onClick={()=>setFilters(f=>({...f,season:v,events:[]}))} color="247,105,0"/>
            ))}
          </div>
        </div>
      )}
      <div style={{marginBottom:11}}>
        <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>Events</div>
        <div style={{display:"flex",flexWrap:"wrap",gap:3}}>
          {visibleEvents.map(e=>{
            const isIndoor=e.season==="indoor", isOutdoor=e.season==="outdoor";
            const col=isIndoor?"43,114,215":isOutdoor?"32,50,153":"247,105,0";
            return <Chip key={e.id} label={e.label} active={filters.events.includes(e.id)} onClick={()=>toggleEvent(e.id)} color={col}/>;
          })}
        </div>
        <div style={{display:"flex",gap:10,marginTop:5}}>
          {[["247,105,0","Both"],["43,114,215","Indoor"],["32,50,153","Outdoor"]].map(([c,l])=>(
            <div key={l} style={{display:"flex",gap:4,alignItems:"center"}}>
              <div style={{width:6,height:6,borderRadius:"50%",background:`rgb(${c})`}}/>
              <span style={{color:T.dim,fontSize:9}}>{l}</span>
            </div>
          ))}
        </div>
        {/* Performance range sliders — shown for each active event */}
        {filters.events.length > 0 && (
          <div style={{marginTop:8,padding:"8px 10px",background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:7}}>
            <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>Performance Range</div>
            {filters.events.map(ev => (
              <PerformanceRangeInput
                key={ev}
                event={ev}
                allAthletes={allAthletes}
                value={performanceRanges[ev] || null}
                onChange={range => onRangeChange(ev, range)}
              />
            ))}
          </div>
        )}
      </div>
      <div style={{marginBottom:9}}>
        <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:5}}>Conference</div>
        <Sel value={filters.conference} onChange={setConf} options={allConferences} placeholder="All Conferences"/>
      </div>
      <div style={{marginBottom:9}}>
        <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:5}}>College{filters.conference?` (${filters.conference})`:""}</div>
        <Sel value={filters.college} onChange={v=>setFilters(f=>({...f,college:v}))} options={confColleges.sort()} placeholder={filters.conference?`All ${filters.conference}`:"All Colleges"}/>
      </div>
      <div style={{marginBottom:9}}>
        <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:5}}>HS Grad Year</div>
        <Sel value={filters.hsYear||""} onChange={v=>setFilters(f=>({...f,hsYear:v}))} options={HS_YEARS.map(y=>({value:y,label:`Class of ${y}`}))} placeholder="All Years"/>
      </div>
      <div style={{marginBottom:9}}>
        <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>College Year</div>
        <div style={{display:"flex",gap:3}}>
          {COLLEGE_YEARS.map(y=><Chip key={y} label={`Y${y}`} active={filters.collegeYear===String(y)} onClick={()=>setFilters(f=>({...f,collegeYear:f.collegeYear===String(y)?"":String(y)}))}/>)}
        </div>
      </div>
      {mapMode==="flows" && <StateFilterDropdown selectedStates={selectedStates} onChange={onStatesChange}/>}
    </div>
  );
}

function applyFilters(athletes, filters, search="", performanceRanges={}) {
  return athletes.filter(a => {
    if (filters.season && filters.season !== "all") {
      const eventsInSeason = EVENTS_CFG.filter(e=>e.season==="both"||e.season===filters.season).map(e=>e.id);
      if (!Array.isArray(a.events) || !a.events.some(e=>eventsInSeason.includes(e))) return false;
    }
    if (filters.events?.length>0 && !Array.isArray(a.events)) return false;
    if (filters.events?.length>0 && !filters.events.some(e=>a.events.includes(e))) return false;
    if (filters.gender && a.gender !== filters.gender) return false;
    if (filters.college && a.college!==filters.college) return false;
    if (filters.hsYear && a.hsYear!==parseInt(filters.hsYear)) return false;
    if (filters.collegeYear && a.collegeYear!==parseInt(filters.collegeYear)) return false;
    // Performance range filter: athlete must have a mark in range for EACH active event with a range set
    for (const [ev, range] of Object.entries(performanceRanges)) {
      if (!range) continue;
      const [lo, hi] = range;
      const mark = a.collegeTimes[ev];
      if (mark === undefined) return false; // no mark for this event at all
      if (isFieldEvent(ev)) {
        if (mark < lo || mark > hi) return false;
      } else {
        if (mark < lo || mark > hi) return false;
      }
    }
    if (search) {
      const q=search.toLowerCase();
      if (!a.name.toLowerCase().includes(q) && !a.college.toLowerCase().includes(q) && !a.hometown.toLowerCase().includes(q)) return false;
    }
    return true;
  });
}

// ── HEATMAP PANEL ─────────────────────────────────────────────────────────────
function HeatmapPanel({athletes}) {
  const [rankTab, setRankTab] = useState("cities");

  // Filter athletes where hometown = college city (data pollution like "Chestnut Hill, MA" = Boston College)
  const realHomers = useMemo(() =>
    athletes.filter(a => a.hometownCoords && a.collegeCoords &&
      haversine(a.hometownCoords, a.collegeCoords) > 5),
    [athletes]);

  const allCities = useMemo(() => {
    const map={};
    realHomers.forEach(a=>{if(!a.hometown)return;if(!map[a.hometown])map[a.hometown]={city:a.hometown,count:0};map[a.hometown].count++;});
    return Object.values(map).sort((a,b)=>b.count-a.count);
  }, [realHomers]);
  const topCities = useMemo(() => allCities.slice(0,20), [allCities]);

  const topStates = useMemo(() => {
    const map={};
    realHomers.forEach(a=>{
      if(!a.hometown)return;
      const st=getState(a.hometown); if(!st) return;
      if(!map[st]) map[st]={abbr:st,name:STATE_NAMES[st]||st,count:0,cities:new Set()};
      map[st].count++; map[st].cities.add(a.hometown);
    });
    return Object.values(map).map(s=>({...s,cityCount:s.cities.size})).sort((a,b)=>b.count-a.count).slice(0,15);
  }, [athletes]);

  const uniqueStateCount = useMemo(() => {
    const s=new Set(); realHomers.forEach(a=>{const st=getState(a.hometown);if(st)s.add(st);}); return s.size;
  }, [realHomers]);

  return (
    <div style={{padding:"14px",height:"100%",overflowY:"auto"}}>
      <div style={{marginBottom:12}}>
        <div style={{color:T.orange,fontFamily:"'Barlow Condensed',sans-serif",fontSize:12,letterSpacing:2,textTransform:"uppercase",marginBottom:6}}>Hometown Density Map</div>
        <div style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:8,padding:"10px 12px",marginBottom:10}}>
          <div style={{color:T.muted,fontSize:11,lineHeight:1.6}}>
            The heatmap reflects your <span style={{color:T.orange,fontWeight:700}}>active filters</span> in the left panel.
          </div>
        </div>
        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:6}}>
          <StatCard label="Athletes" value={athletes.length} color={T.orange}/>
          <StatCard label="Cities" value={allCities.length} color={T.blueL}/>
          <StatCard label="States" value={uniqueStateCount} color={T.blueM}/>
        </div>
      </div>
      <div style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:8,padding:"10px 12px",marginBottom:14}}>
        <div style={{color:T.muted,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:7}}>Density Scale</div>
        <div style={{display:"flex",height:10,borderRadius:4,overflow:"hidden",marginBottom:5}}>
          <div style={{flex:1,background:"linear-gradient(to right,#FFF8F4,#F3BC9F,#CA6038,#880302)",borderRadius:4}}/>
        </div>
        <div style={{display:"flex",justifyContent:"space-between"}}>
          <span style={{color:T.dim,fontSize:9}}>Sparse</span>
          <span style={{color:T.dim,fontSize:9}}>Dense</span>
        </div>
      </div>
      <div style={{display:"flex",background:T.bg,borderRadius:7,border:`1px solid ${T.border}`,overflow:"hidden",marginBottom:12}}>
        {[["cities","Top Cities"],["states","Top States"]].map(([tab,lbl])=>(
          <button key={tab} onClick={()=>setRankTab(tab)} style={{flex:1,padding:"7px 6px",background:rankTab===tab?T.orange:"transparent",border:"none",color:rankTab===tab?T.white:T.muted,fontSize:11,cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase",transition:"all 0.15s",fontWeight:rankTab===tab?700:400}}>{lbl}</button>
        ))}
      </div>
      {rankTab === "cities" && (
        <>
          <div style={{display:"grid",gridTemplateColumns:"auto 1fr auto",gap:"0 10px",marginBottom:4,paddingBottom:5,borderBottom:`1px solid ${T.border}`}}>
            {["#","City","Athletes"].map(h=><span key={h} style={{color:T.dim,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase",textAlign:h==="Athletes"?"right":"left"}}>{h}</span>)}
          </div>
          {topCities.length===0 ? <div style={{color:T.dim,fontSize:11,textAlign:"center",padding:"20px 0"}}>No athletes match current filters</div>
          : topCities.map((c,i)=>(
            <div key={c.city} style={{display:"grid",gridTemplateColumns:"auto 1fr auto",gap:"0 10px",alignItems:"center",padding:"6px 0",borderBottom:`1px solid ${T.border}44`}}>
              <span style={{color:i<3?T.orange:T.dim,fontSize:11,fontFamily:"monospace",minWidth:20,fontWeight:i<3?800:400}}>#{i+1}</span>
              <div>
                <div style={{color:T.offWhite,fontSize:12}}>{c.city}</div>
                <div style={{height:3,borderRadius:2,background:T.orange,opacity:0.5,marginTop:3,width:Math.max(6,Math.round(c.count/topCities[0].count*100))+'%'}}/>
              </div>
              <span style={{color:i<3?T.orange:T.offWhite,fontSize:13,fontWeight:800,fontFamily:"monospace",textAlign:"right"}}>{c.count}</span>
            </div>
          ))}
        </>
      )}
      {rankTab === "states" && (
        <>
          <div style={{display:"grid",gridTemplateColumns:"auto 1fr auto auto",gap:"0 8px",marginBottom:4,paddingBottom:5,borderBottom:`1px solid ${T.border}`}}>
            {["#","State","Cities","Athletes"].map(h=><span key={h} style={{color:T.dim,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase"}}>{h}</span>)}
          </div>
          {topStates.length===0 ? <div style={{color:T.dim,fontSize:11,textAlign:"center",padding:"20px 0"}}>No athletes match current filters</div>
          : topStates.map((s,i)=>(
            <div key={s.abbr} style={{display:"grid",gridTemplateColumns:"auto 1fr auto auto",gap:"0 8px",alignItems:"center",padding:"6px 0",borderBottom:`1px solid ${T.border}44`}}>
              <span style={{color:i<3?T.orange:T.dim,fontSize:11,fontFamily:"monospace",minWidth:20,fontWeight:i<3?800:400}}>#{i+1}</span>
              <div>
                <div style={{display:"flex",gap:6,alignItems:"center"}}>
                  <span style={{color:T.white,fontSize:10,fontWeight:800,fontFamily:"'Barlow Condensed',sans-serif",background:i<3?T.orange:T.border,borderRadius:3,padding:"1px 5px",letterSpacing:1}}>{s.abbr}</span>
                  <span style={{color:T.offWhite,fontSize:12}}>{s.name}</span>
                </div>
                <div style={{height:3,borderRadius:2,background:T.orange,opacity:0.5,marginTop:3,width:Math.max(6,Math.round(s.count/topStates[0].count*100))+'%'}}/>
              </div>
              <span style={{color:T.muted,fontSize:10,textAlign:"center",fontFamily:"monospace"}}>{s.cityCount}</span>
              <span style={{color:i<3?T.orange:T.offWhite,fontSize:13,fontWeight:800,fontFamily:"monospace",textAlign:"right"}}>{s.count}</span>
            </div>
          ))}
        </>
      )}
    </div>
  );
}

// ── COLLEGE PULL PANEL ────────────────────────────────────────────────────────
function CollegePullPanel({athletes, focusedCollege, onFocusCollege}) {
  const stats = useMemo(() => {
    const map={};
    athletes.forEach(a=>{
      if(!map[a.college]) map[a.college]={college:a.college,conference:a.conference,list:[],hometowns:new Set()};
      map[a.college].list.push({...a,dist:a.hometownCoords?haversine(a.hometownCoords,a.collegeCoords):0});
      map[a.college].hometowns.add(a.hometown);
    });
    return Object.values(map).map(c=>({...c,count:c.list.length,avgDist:c.list.reduce((s,a)=>s+a.dist,0)/c.list.length,maxDist:c.list.reduce((mx,a)=>Math.max(mx,a.dist),0),hometownCount:c.hometowns.size})).sort((a,b)=>b.count-a.count);
  }, [athletes]);
  const focused = stats.find(c=>c.college===focusedCollege);

  return (
    <div style={{height:"100%",overflowY:"auto",padding:"14px"}}>
      <SectionHead>College Pull Analysis</SectionHead>
      {!focusedCollege ? (
        <>
          <div style={{color:T.muted,fontSize:11,marginBottom:12,lineHeight:1.5}}>Which schools pull from where & how far?</div>
          {stats.map(c=>(
            <button key={c.college} onClick={()=>onFocusCollege(c.college)} style={{display:"block",width:"100%",textAlign:"left",background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:8,padding:"9px 11px",marginBottom:5,cursor:"pointer",transition:"border-color 0.12s"}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start"}}>
                <div><div style={{color:T.offWhite,fontWeight:700,fontSize:13,fontFamily:"'Barlow Condensed',sans-serif"}}>{c.college}</div><div style={{color:T.dim,fontSize:10,marginTop:1}}>{c.conference} · {c.hometownCount} cities</div></div>
                <div style={{textAlign:"right"}}><div style={{color:T.orange,fontWeight:800,fontSize:14,fontFamily:"'Barlow Condensed',sans-serif"}}>{c.count}</div><div style={{color:distColor(c.avgDist),fontSize:10}}>avg {fmtDist(c.avgDist)}</div></div>
              </div>
              <div style={{marginTop:7}}><DistBar athletes={c.list}/></div>
            </button>
          ))}
        </>
      ) : focused ? (
        <>
          <button onClick={()=>onFocusCollege("")} style={{background:"none",border:`1px solid ${T.border}`,color:T.orange,borderRadius:5,padding:"4px 10px",fontSize:11,cursor:"pointer",marginBottom:12,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>Back to All Colleges</button>
          <div style={{background:T.bgCard,border:`1px solid ${T.orange}44`,borderRadius:9,padding:"12px",marginBottom:12}}>
            <div style={{color:T.orange,fontFamily:"'Barlow Condensed',sans-serif",fontSize:19,fontWeight:900,letterSpacing:1}}>{focused.college}</div>
            <div style={{color:T.muted,fontSize:11,marginBottom:10}}>{focused.conference}</div>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:6,marginBottom:10}}>
              <StatCard label="Athletes" value={focused.count}/>
              <StatCard label="Hometowns" value={focused.hometownCount} color={T.offWhite}/>
              <StatCard label="Avg Distance" value={fmtDist(focused.avgDist)} color={distColor(focused.avgDist)}/>
              <StatCard label="Farthest" value={fmtDist(focused.maxDist)} color={T.red}/>
            </div>
            <DistBar athletes={focused.list}/>
          </div>
          <SectionHead>Athletes & Origins</SectionHead>
          {focused.list.sort((a,b)=>b.dist-a.dist).map(a=>(
            <div key={a.id} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"7px 0",borderBottom:`1px solid ${T.border}`}}>
              <div><div style={{color:T.offWhite,fontSize:12,fontWeight:600}}>{a.name}</div><div style={{color:T.muted,fontSize:10,marginTop:1}}>📍 {a.hometown}</div></div>
              <div style={{color:distColor(a.dist),fontSize:12,fontWeight:800,fontFamily:"monospace"}}>{fmtDist(a.dist)}</div>
            </div>
          ))}
        </>
      ) : null}
    </div>
  );
}

// ── HOMETOWN PANEL ────────────────────────────────────────────────────────────
function HometownPanel({athletes, focusedHometown, onFocusHometown}) {
  const stats = useMemo(() => {
    const map={};
    athletes.forEach(a=>{
      if(!map[a.hometown]) map[a.hometown]={hometown:a.hometown,list:[],colleges:new Set()};
      map[a.hometown].list.push({...a,dist:a.hometownCoords?haversine(a.hometownCoords,a.collegeCoords):0});
      map[a.hometown].colleges.add(a.college);
    });
    return Object.values(map).map(h=>({...h,count:h.list.length,avgDist:h.list.reduce((s,a)=>s+a.dist,0)/h.list.length,maxDist:h.list.reduce((mx,a)=>Math.max(mx,a.dist),0),collegeCount:h.colleges.size})).sort((a,b)=>b.count-a.count);
  }, [athletes]);
  const focused = stats.find(h=>h.hometown===focusedHometown);

  return (
    <div style={{height:"100%",overflowY:"auto",padding:"14px"}}>
      <SectionHead>Hometown Destinations</SectionHead>
      {!focusedHometown ? (
        <>
          <div style={{color:T.muted,fontSize:11,marginBottom:12,lineHeight:1.5}}>Where do athletes from each city go?</div>
          {stats.map(h=>(
            <button key={h.hometown} onClick={()=>onFocusHometown(h.hometown)} style={{display:"block",width:"100%",textAlign:"left",background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:8,padding:"9px 11px",marginBottom:5,cursor:"pointer"}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start"}}>
                <div><div style={{color:T.offWhite,fontWeight:700,fontSize:13,fontFamily:"'Barlow Condensed',sans-serif"}}>{h.hometown}</div><div style={{color:T.dim,fontSize:10,marginTop:1}}>{h.collegeCount} colleges</div></div>
                <div style={{textAlign:"right"}}><div style={{color:T.orange,fontWeight:800,fontSize:14,fontFamily:"'Barlow Condensed',sans-serif"}}>{h.count}</div><div style={{color:distColor(h.avgDist),fontSize:10}}>avg {fmtDist(h.avgDist)}</div></div>
              </div>
              <div style={{marginTop:7}}><DistBar athletes={h.list}/></div>
            </button>
          ))}
        </>
      ) : focused ? (
        <>
          <button onClick={()=>onFocusHometown("")} style={{background:"none",border:`1px solid ${T.border}`,color:T.orange,borderRadius:5,padding:"4px 10px",fontSize:11,cursor:"pointer",marginBottom:12,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>Back to All Hometowns</button>
          <div style={{background:T.bgCard,border:`1px solid ${T.orange}44`,borderRadius:9,padding:"12px",marginBottom:12}}>
            <div style={{color:T.orange,fontFamily:"'Barlow Condensed',sans-serif",fontSize:19,fontWeight:900,letterSpacing:1}}>📍 {focused.hometown}</div>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:6,marginTop:10,marginBottom:10}}>
              <StatCard label="Athletes" value={focused.count}/>
              <StatCard label="Colleges" value={focused.collegeCount} color={T.offWhite}/>
              <StatCard label="Avg Distance" value={fmtDist(focused.avgDist)} color={distColor(focused.avgDist)}/>
              <StatCard label="Farthest" value={fmtDist(focused.maxDist)} color={T.red}/>
            </div>
            <DistBar athletes={focused.list}/>
          </div>
          <SectionHead>Where Athletes Went</SectionHead>
          {focused.list.sort((a,b)=>b.dist-a.dist).map(a=>(
            <div key={a.id} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"7px 0",borderBottom:`1px solid ${T.border}`}}>
              <div><div style={{color:T.offWhite,fontSize:12,fontWeight:600}}>{a.name}</div><div style={{color:T.muted,fontSize:10,marginTop:1}}>🎓 {a.college} · {a.conference}</div></div>
              <div style={{color:distColor(a.dist),fontSize:12,fontWeight:800,fontFamily:"monospace"}}>{fmtDist(a.dist)}</div>
            </div>
          ))}
        </>
      ) : null}
    </div>
  );
}


// ── EVENT PROGRESS CHART ──────────────────────────────────────────────────────
// Season colors
const SEASON_COLOR = {indoor:"#3B82F6", outdoor:"#F76900", xc:"#22C55E"};
const seasonColor = s => SEASON_COLOR[s] || T.grayM;

// Parse place finish out of meet_name e.g. "1st(F)" → {place:1, round:"F"}
function parsePlace(meetName) {
  if (!meetName) return null;
  const m = meetName.match(/^(\d+)(?:st|nd|rd|th)\(([^)]+)\)/i);
  if (!m) return null;
  return {place: parseInt(m[1]), round: m[2]};
}
function placeColor(place) {
  if (place === 1) return "#F59E0B";
  if (place === 2) return "#94A3B8";
  if (place === 3) return "#CD7F32";
  if (place <= 8)  return T.blueL;
  return null;
}

function EventProgressChart({event, performances, allEventMarks=[]}) {
  const field = isFieldEvent(event);

  const perfs = performances
    .filter(p => p.event === event && p.mark && p.year)
    .sort((a,b) => a.year - b.year || (field ? b.mark - a.mark : a.mark - b.mark));

  if (perfs.length < 2) return null;

  // Filter out outdoor 2026 — outdoor season hasn't started yet (indoor only as of Mar 2026)
  // Only filter if season is genuinely labeled outdoor (scraper now infers this from meet date)
  const CURRENT_YEAR = 2026;
  const filteredPerfs = perfs.filter(p => !(p.year === CURRENT_YEAR && p.season === "outdoor"));

  const marks = filteredPerfs.map(p=>p.mark);
  if (filteredPerfs.length < 2) return null;
  const minMark = Math.min(...marks), maxMark = Math.max(...marks);
  const markRange = maxMark - minMark || 1;
  const prMark = field ? Math.max(...marks) : Math.min(...marks);

  // ── PR PROGRESSION: build step-line of running best ────────────────────────
  let runningBest = null;
  const prPoints = [];
  filteredPerfs.forEach((p, i) => {
    if (runningBest === null || (field ? p.mark > runningBest : p.mark < runningBest)) {
      runningBest = p.mark;
      prPoints.push({i, mark: p.mark});
    }
  });

  // Build step-line SVG path
  const VW=260, VH=180, P={top:20,right:16,bottom:36,left:46};
  const ex = i => P.left + (i / Math.max(filteredPerfs.length-1, 1)) * (VW - P.left - P.right);
  const ey = m => { const n=field?(m-minMark)/markRange:1-(m-minMark)/markRange; return P.top+n*(VH-P.top-P.bottom); };

  let stepPath = "";
  for (let k=0; k<prPoints.length; k++) {
    const {i, mark} = prPoints[k];
    const x = ex(i), y = ey(mark);
    if (k === 0) {
      stepPath += `M ${P.left} ${y} H ${x} V ${y}`;
    } else {
      stepPath += ` H ${x} V ${y}`;
    }
    const nextX = k+1 < prPoints.length ? ex(prPoints[k+1].i) : VW - P.right;
    stepPath += ` H ${nextX}`;
  }

  // Year boundaries
  const years = [...new Set(filteredPerfs.map(p=>p.year))].sort();
  const yearBounds = years.map(yr => ({yr, i: filteredPerfs.findIndex(p=>p.year===yr)}));

  // Season/year groups — SB only shown if group has 2+ performances
  const grouped = {};
  filteredPerfs.forEach(p => {
    const key = `${p.year}-${p.season}`;
    if (!grouped[key]) grouped[key] = [];
    grouped[key].push(p);
  });
  const seasonBestSet = new Set();
  Object.values(grouped).forEach(group => {
    if (group.length < 2) return; // skip single-perf groups
    const best = field ? group.reduce((b,p)=>p.mark>b.mark?p:b) : group.reduce((b,p)=>p.mark<b.mark?p:b);
    seasonBestSet.add(best);
  });

  // Percentile vs all athletes (exclude self, compare prMark against others' PRs)
  let percentile = null;
  if (allEventMarks.length > 1) {
    // "faster than X%" = X% of athletes have a WORSE mark than this athlete
    const worse = field
      ? allEventMarks.filter(m => m < prMark).length   // field: lower = worse
      : allEventMarks.filter(m => m > prMark).length;  // time: higher = worse
    percentile = Math.round((worse / allEventMarks.length) * 100);
  }

  return (
    <div style={{marginBottom:16,borderBottom:`1px solid ${T.border}`,paddingBottom:14}}>
      {/* Header */}
      <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:8}}>
        <div style={{display:"flex",alignItems:"center",gap:8}}>
          <span style={{color:T.orange,fontSize:13,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase",fontWeight:800}}>{event}</span>
          <span style={{color:T.orange,fontSize:14,fontWeight:900,fontFamily:"monospace"}}>{fmtTime(prMark)}</span>
          <span style={{fontSize:8,background:T.orange,color:"#fff",borderRadius:3,padding:"2px 5px",letterSpacing:0.5,fontFamily:"'Barlow Condensed',sans-serif"}}>PR</span>
        </div>
        <span style={{color:T.grayM,fontSize:10,fontFamily:"monospace"}}>{filteredPerfs.length} marks</span>
      </div>

      {/* Percentile bar */}
      {percentile !== null && (
        <div style={{marginBottom:10}}>
          <div style={{display:"flex",justifyContent:"space-between",marginBottom:3}}>
            <span style={{color:T.grayM,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase"}}>Faster than</span>
            <span style={{color:percentile>=75?T.orange:percentile>=50?T.blueL:T.grayM,fontSize:10,fontWeight:700,fontFamily:"monospace"}}>{percentile}% of athletes</span>
          </div>
          <div style={{height:5,background:T.bgCard,borderRadius:3,border:`1px solid ${T.border}`,overflow:"hidden"}}>
            <div style={{height:"100%",width:`${percentile}%`,background:percentile>=75?T.orange:percentile>=50?T.blueL:T.grayM,borderRadius:3,transition:"width 0.4s"}}/>
          </div>
        </div>
      )}

      {/* Chart: PR step-line + season-colored dots */}
      <svg viewBox={`0 0 ${VW} ${VH}`} width="100%" style={{display:"block"}}>
        {/* Y gridlines */}
        {[0, 0.5, 1].map(t => {
          const y = P.top + t*(VH-P.top-P.bottom);
          const val = field ? maxMark - t*markRange : minMark + t*markRange;
          return <g key={t}>
            <line x1={P.left} x2={VW-P.right} y1={y} y2={y} stroke={T.border} strokeWidth={0.5}/>
            <text x={P.left-4} y={y+3} textAnchor="end" fontSize={8} fill={T.grayM} fontFamily="monospace">{fmtTime(val)}</text>
          </g>;
        })}

        {/* Year boundary dividers + labels */}
        {yearBounds.map(({yr,i},idx) => {
          const x = ex(i);
          const isLast = idx === yearBounds.length-1;
          return <g key={yr}>
            {i > 0 && <line x1={x} x2={x} y1={P.top} y2={VH-P.bottom+4} stroke={T.border} strokeWidth={0.7} strokeDasharray="3,2"/>}
            <text x={isLast ? Math.min(x, VW-P.right-6) : x} y={VH-P.bottom+14} textAnchor={isLast?"end":"start"} fontSize={9} fill={T.grayM} fontFamily="'Barlow Condensed',sans-serif" fontWeight="600">{yr}</text>
          </g>;
        })}

        {/* PR step-line */}
        <path d={stepPath} fill="none" stroke={T.orange} strokeWidth={2} strokeOpacity={0.35} strokeDasharray="none"/>

        {/* Season-colored dots for all perfs */}
        {filteredPerfs.map((p, i) => {
          const col = seasonColor(p.season);
          const isPR = p.mark === prMark;
          const pp = parsePlace(p.meet_name);
          return <g key={i}>
            <circle cx={ex(i)} cy={ey(p.mark)} r={isPR ? 6 : pp ? 4.5 : 3.5}
              fill={isPR ? T.orange : col}
              stroke={isPR ? T.orangeD : pp ? placeColor(pp.place)||col : col}
              strokeWidth={isPR ? 2 : pp ? 2 : 1}
              fillOpacity={isPR ? 1 : 0.8}>
              <title>{p.mark_display||fmtTime(p.mark)} · {p.season} {p.year}{p.meet_name ? ` · ${p.meet_name}` : ""}</title>
            </circle>
            {isPR && <text x={ex(i)} y={ey(p.mark)-10} textAnchor="middle" fontSize={8} fill={T.orange} fontFamily="monospace" fontWeight="800">{p.mark_display||fmtTime(p.mark)}</text>}
          </g>;
        })}

        {/* Season legend bottom-right */}
        {[["indoor","In"],["outdoor","Out"],["xc","XC"]].map(([s,l],i) => (
          <g key={s} transform={`translate(${VW-P.right - (2-i)*42}, ${VH-2})`}>
            <circle cx={0} cy={-3} r={3.5} fill={seasonColor(s)} fillOpacity={0.85}/>
            <text x={6} y={0} fontSize={8} fill={T.grayM} fontFamily="'Barlow Condensed',sans-serif">{l}</text>
          </g>
        ))}
      </svg>

      {/* Performance list */}
      <div style={{marginTop:8,maxHeight:200,overflowY:"auto",borderTop:`1px solid ${T.border}`,paddingTop:6}}>
        {(() => {
          const sorted = [...filteredPerfs].reverse();
          return sorted.map((p, i) => {
            const isPR = p.mark === prMark;
            const isSB = !isPR && seasonBestSet.has(p);
            const prevP = sorted[i-1];
            const seasonChanged = i > 0 && prevP && `${prevP.year}-${prevP.season}` !== `${p.year}-${p.season}`;
            const markColor = isPR ? T.orange : isSB ? T.blueL : T.offWhite;
            const pp = parsePlace(p.meet_name);
            const pc = pp ? placeColor(pp.place) : null;
            const sCol = seasonColor(p.season);
            return (
              <div key={i}>
                {seasonChanged && <div style={{borderTop:`1px dashed ${T.border}`,margin:"5px 0"}}/>}
                <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"4px 2px",borderBottom:`1px solid ${T.border}22`,gap:6}}>
                  {/* Season color dot */}
                  <div style={{width:6,height:6,borderRadius:"50%",background:sCol,flexShrink:0}}/>
                  {/* Mark + badges */}
                  <span style={{fontFamily:"monospace",fontSize:12,color:markColor,fontWeight:(isPR||isSB)?700:400,display:"flex",alignItems:"center",gap:5,minWidth:60}}>
                    {p.mark_display||fmtTime(p.mark)}
                    {isPR && <span style={{fontSize:8,background:T.orange,color:"#fff",borderRadius:3,padding:"1px 4px"}}>PR</span>}
                    {isSB && <span style={{fontSize:8,background:T.blueL,color:"#fff",borderRadius:3,padding:"1px 4px"}}>SB</span>}
                  </span>
                  {/* Place finish pill */}
                  {pp && (
                    <span style={{fontSize:10,fontWeight:700,fontFamily:"'Barlow Condensed',sans-serif",color:pc||T.grayM,background:pc?`${pc}22`:`${T.border}44`,borderRadius:4,padding:"1px 6px",border:`1px solid ${pc||T.border}55`,flexShrink:0}}>
                      {pp.place}{pp.place===1?"st":pp.place===2?"nd":pp.place===3?"rd":"th"} {pp.round}
                    </span>
                  )}
                  {/* Meet + year */}
                  <span style={{color:T.grayM,fontSize:10,flex:1,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",textAlign:"right"}}>
                    {pp ? p.meet_name.replace(/^\d+(?:st|nd|rd|th)\([^)]+\)\s*/i,"") : (p.meet_name||"—")}
                    <span style={{color:T.grayL,marginLeft:4}}>{p.year}</span>
                  </span>
                </div>
              </div>
            );
          });
        })()}
      </div>
    </div>
  );
}

// ── DUAL RANGE SLIDER ─────────────────────────────────────────────────────────
// Parse a time string like "4:30.25" or "13.45" or "6.52" into seconds (float)
function parseTimeInput(s) {
  if (!s || !s.trim()) return null;
  s = s.trim();
  // mm:ss.ms  or  h:mm:ss.ms
  if (s.includes(":")) {
    const parts = s.split(":");
    if (parts.length === 2) {
      const [m, sec] = parts;
      const val = parseFloat(m) * 60 + parseFloat(sec);
      return isNaN(val) ? null : val;
    }
    if (parts.length === 3) {
      const [h, m, sec] = parts;
      const val = parseFloat(h) * 3600 + parseFloat(m) * 60 + parseFloat(sec);
      return isNaN(val) ? null : val;
    }
  }
  const val = parseFloat(s);
  return isNaN(val) ? null : val;
}

function PerformanceRangeInput({event, allAthletes, value, onChange}) {
  const field = isFieldEvent(event);
  const marks = allAthletes.map(a => a.collegeTimes[event]).filter(Boolean);
  if (marks.length < 2) return null;

  const globalMin = marks.reduce((a,b) => Math.min(a,b), marks[0]);
  const globalMax = marks.reduce((a,b) => Math.max(a,b), marks[0]);

  const [loStr, setLoStr] = useState(value ? fmtTime(value[0]) : "");
  const [hiStr, setHiStr] = useState(value ? fmtTime(value[1]) : "");
  const [loErr, setLoErr] = useState(false);
  const [hiErr, setHiErr] = useState(false);

  const commit = (newLoStr, newHiStr) => {
    const lo = parseTimeInput(newLoStr);
    const hi = parseTimeInput(newHiStr);
    const loOk = lo !== null && lo >= 0;
    const hiOk = hi !== null && hi >= 0;
    setLoErr(newLoStr !== "" && !loOk);
    setHiErr(newHiStr !== "" && !hiOk);
    if (newLoStr === "" && newHiStr === "") {
      onChange(null); // clear filter
    } else if (loOk || hiOk) {
      const effectiveLo = loOk ? lo : globalMin;
      const effectiveHi = hiOk ? hi : globalMax;
      onChange([Math.min(effectiveLo, effectiveHi), Math.max(effectiveLo, effectiveHi)]);
    }
  };

  const placeholder = field ? "e.g. 6.52" : event.includes("m") && !event.includes("H") && parseFloat(event) >= 800
    ? "e.g. 4:05.00" : "e.g. 10.50";

  const inputStyle = (err) => ({
    flex:1, background: err ? "#FFF0F0" : T.bg,
    border:`1px solid ${err ? T.red : T.border}`,
    borderRadius:5, padding:"4px 6px", fontSize:11,
    color: T.offWhite, fontFamily:"monospace",
    outline:"none", minWidth:0,
  });

  return (
    <div style={{marginBottom:8}}>
      <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:4}}>
        <span style={{color:T.orange,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase",fontWeight:700}}>{event}</span>
        <span style={{color:T.dim,fontSize:8,fontFamily:"monospace"}}>
          {fmtTime(globalMin)} – {fmtTime(globalMax)}
        </span>
      </div>
      <div style={{display:"flex",gap:4,alignItems:"center"}}>
        <input
          value={loStr}
          onChange={e => setLoStr(e.target.value)}
          onBlur={() => commit(loStr, hiStr)}
          placeholder={`Min (${placeholder})`}
          style={inputStyle(loErr)}
        />
        <span style={{color:T.dim,fontSize:10}}>–</span>
        <input
          value={hiStr}
          onChange={e => setHiStr(e.target.value)}
          onBlur={() => commit(loStr, hiStr)}
          placeholder={`Max (${placeholder})`}
          style={inputStyle(hiErr)}
        />
        {(loStr||hiStr) && (
          <button onClick={()=>{setLoStr("");setHiStr("");setLoErr(false);setHiErr(false);onChange(null);}}
            style={{background:"none",border:"none",color:T.muted,cursor:"pointer",fontSize:12,padding:"0 2px",flexShrink:0}}>✕</button>
        )}
      </div>
      {(loErr||hiErr) && <div style={{color:T.red,fontSize:9,marginTop:2}}>Use format: 4:05.00 or 13.45</div>}
    </div>
  );
}

// ── ATHLETE DETAIL ────────────────────────────────────────────────────────────
function AthleteDetail({athlete, onClose, allAthletes=[]}) {
  if (!athlete) return (
    <div style={{display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",height:"100%",gap:12,padding:24}}>
      <div style={{fontSize:44,opacity:0.15}}>🌍</div>
      <div style={{color:T.dim,fontFamily:"'Barlow Condensed',sans-serif",fontSize:11,letterSpacing:3,textTransform:"uppercase",textAlign:"center",lineHeight:1.9}}>Select an athlete<br/>on the map</div>
    </div>
  );

  const dist=athlete.hometownCoords?haversine(athlete.hometownCoords,athlete.collegeCoords):0, dc=distColor(dist);
  const colEvents = Object.keys(athlete.collegeTimes);

  return (
    <div style={{padding:"16px",overflowY:"auto",height:"100%"}}>
      {/* Name + close */}
      <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:10}}>
        <div>
          <div style={{color:T.orange,fontFamily:"'Barlow Condensed',sans-serif",fontSize:22,fontWeight:900,letterSpacing:2}}>{athlete.name}</div>
          <div style={{color:T.offWhite,fontSize:13,marginTop:2,fontWeight:600}}>{athlete.college}</div>
          <div style={{color:T.muted,fontSize:11,marginTop:1}}>{athlete.conference}{athlete.collegeYear ? ` · Year ${athlete.collegeYear}` : ""}{athlete.gender ? ` · ${athlete.gender==="M"?"Men":"Women"}` : ""}</div>
        </div>
        <button onClick={onClose} style={{background:"none",border:`1px solid ${T.border}`,color:T.muted,borderRadius:6,cursor:"pointer",width:26,height:26,fontSize:13,display:"flex",alignItems:"center",justifyContent:"center"}}>✕</button>
      </div>

      {/* Recruitment distance */}
      <div style={{background:T.bgCard,border:`1px solid ${dc}55`,borderRadius:9,padding:"10px 12px",marginBottom:10}}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}>
          <div>
            <div style={{color:T.muted,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:2,textTransform:"uppercase",marginBottom:3}}>Recruitment Distance</div>
            <div style={{color:dc,fontSize:22,fontWeight:900,fontFamily:"'Barlow Condensed',sans-serif",lineHeight:1}}>{fmtDist(dist)}</div>
            <div style={{color:T.muted,fontSize:10,marginTop:3}}>{athlete.hometown||"Unknown"} → {athlete.college}</div>
          </div>
          <div style={{fontSize:28}}>{dist<100?"🏠":dist<400?"🚗":dist<800?"✈️":"🌎"}</div>
        </div>
      </div>

      {/* Meta grid */}
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:6,marginBottom:12}}>
        {[{l:"Hometown",v:athlete.hometown||"—"},{l:"HS Grad",v:athlete.hsYear||"—"},{l:"College Year",v:athlete.collegeYear?`Year ${athlete.collegeYear}`:"—"},{l:"Gender",v:athlete.gender==="M"?"Men":"Women"}].map(({l,v})=>(
          <div key={l} style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:6,padding:"7px 9px"}}>
            <div style={{color:T.dim,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase"}}>{l}</div>
            <div style={{color:T.offWhite,fontSize:11,marginTop:2,fontWeight:500}}>{v}</div>
          </div>
        ))}
      </div>

      {athlete.tfrrsUrl && (
        <a href={athlete.tfrrsUrl} target="_blank" rel="noopener noreferrer"
          style={{display:"block",marginBottom:14,textAlign:"center",color:T.orange,fontSize:11,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textDecoration:"none",border:`1px solid ${T.orange}44`,borderRadius:6,padding:"5px 0"}}>
          View on TFRRS →
        </a>
      )}

      {/* Charts — one per event, always shown */}
      {athlete.rawPerformances?.length > 0 && (() => {
        const chartableEvents = [...new Set(athlete.rawPerformances.filter(p=>p.year && !(p.year===2026 && p.season==="outdoor")).map(p=>p.event))].filter(ev => {
          const years = new Set(athlete.rawPerformances.filter(p=>p.event===ev && p.year && !(p.year===2026 && p.season==="outdoor")).map(p=>p.year));
          return years.size >= 2;
        });
        if (!chartableEvents.length) return null;
        return (
          <>
            <SectionHead>Progress & Performances</SectionHead>
            <div style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:8,padding:"12px 14px",marginBottom:4}}>
              {chartableEvents.map(ev => {
                // Build allEventMarks from all athletes for percentile
                const allEventMarks = allAthletes
                  .filter(a => a.id !== athlete.id)
                  .map(a => a.collegeTimes?.[ev])
                  .filter(Boolean);
                return <EventProgressChart key={ev} event={ev} performances={athlete.rawPerformances} allEventMarks={allEventMarks}/>;
              })}
            </div>
          </>
        );
      })()}
    </div>
  );
}

// ── APP ───────────────────────────────────────────────────────────────────────
const BLANK_FILTERS = {events:[],conference:"",college:"",hsYear:"",collegeYear:"",season:"all",gender:""};

export default function App() {
  // ── Data fetching ──────────────────────────────────────────────────────────
  const [athletes, setAthletes] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchAll = async () => {
      const PAGE = 1000;
      let all = [], page = 0;
      try {
        while (true) {
          const r = await fetch(`/api/athletes?limit=${PAGE}&offset=${page * PAGE}`);
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          const batch = await r.json();
          if (!Array.isArray(batch) || batch.length === 0) break;
          all = all.concat(batch);
          if (batch.length < PAGE) break;
          page++;
        }
        const transformed = [];
        all.forEach((raw, i) => {
          try { transformed.push(transformAthlete(raw, i)); }
          catch(e) { console.warn("Bad athlete row:", raw?.id, e.message); }
        });
        setAthletes(transformed);
        setLoading(false);
      } catch(err) {
        setError(err.message);
        setLoading(false);
      }
    };
    fetchAll();
  }, []);

  // ── Derived conference/college lists from real data ────────────────────────
  const allConferences = useMemo(() =>
    [...new Set(athletes.map(a=>a.conference).filter(Boolean))].sort()
  , [athletes]);

  const getConfColleges = (conf) => {
    const list = conf
      ? athletes.filter(a=>a.conference===conf).map(a=>a.college)
      : athletes.map(a=>a.college);
    return [...new Set(list.filter(Boolean))].sort();
  };

  // ── UI state ───────────────────────────────────────────────────────────────
  const [selectedAthlete, setSelectedAthlete] = useState(null);
  const [mapMode, setMapMode] = useState("flows");
  const [rightTab, setRightTab] = useState("athlete");
  const [focusedCollege, setFocusedCollege] = useState("");
  const [focusedHometown, setFocusedHometown] = useState("");
  const [selectedStates, setSelectedStates] = useState([]);
  const [filters, setFilters] = useState({...BLANK_FILTERS});
  const [search, setSearch] = useState("");
  const [performanceRanges, setPerformanceRanges] = useState({});
  const [leftCollapsed, setLeftCollapsed] = useState(false);
  const [rightCollapsed, setRightCollapsed] = useState(false);
  const handleRangeChange = (ev, range) => setPerformanceRanges(prev => ({...prev, [ev]: range}));
  // Wrapper that also cleans up ranges for deselected events
  const setFiltersWithCleanup = updater => {
    setFilters(prev => {
      const next = typeof updater === "function" ? updater(prev) : updater;
      if (next.events !== prev.events) {
        setPerformanceRanges(r => Object.fromEntries(Object.entries(r).filter(([k]) => (next.events||[]).includes(k))));
      }
      return next;
    });
  };

  // ── Filtered list — depends on athletes state ──────────────────────────────
  const filtered = useMemo(() => applyFilters(athletes, filters, search, performanceRanges), [athletes, filters, search, performanceRanges]);
  // Exclude athletes where hometown ≈ college city (data quality: college stored as hometown)
  const overallAvg = useMemo(() => {
    const valid=filtered.filter(a=>a.hometownCoords&&a.collegeCoords&&haversine(a.hometownCoords,a.collegeCoords)>5);
    return valid.length ? Math.round(valid.reduce((s,a)=>s+haversine(a.hometownCoords,a.collegeCoords),0)/valid.length) : 0;
  }, [filtered]);
  const hasFilters = filters.events.length>0||filters.conference||filters.college||filters.hsYear||filters.collegeYear||search||filters.season!=="all"||selectedStates.length>0;

  const handleAthleteClick = a => { setSelectedAthlete(s=>s?.id===a.id?null:a); setRightTab("athlete"); };
  const handleFocusCollege = c => { setFocusedCollege(c); if (c) setFocusedHometown(""); };
  const handleFocusHometown = h => { setFocusedHometown(h); if (h) setFocusedCollege(""); };
  const switchMapMode = m => { setMapMode(m); if (m==="heatmap") setRightTab("heatmap"); if (m!=="flows") setSelectedStates([]); };
  const switchRightTab = t => { setRightTab(t); if (t==="heatmap") setMapMode("heatmap"); else if (mapMode==="heatmap") setMapMode("flows"); };

  const highlightCollege = rightTab==="college" ? focusedCollege : "";
  const highlightHometown = rightTab==="hometown" ? focusedHometown : "";

  return (
    <>
      <link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700;800;900&family=Barlow:wght@400;500;600&display=swap" rel="stylesheet"/>
      <style>{`*{box-sizing:border-box;margin:0;padding:0;}::-webkit-scrollbar{width:5px;}::-webkit-scrollbar-track{background:#F7F7F8;}::-webkit-scrollbar-thumb{background:#D4D6D9;border-radius:3px;}::-webkit-scrollbar-thumb:hover{background:#F76900;}button:hover{filter:brightness(0.95);}select option{background:#FFFFFF;color:#404040;}input::placeholder{color:#ADB3B8;}`}</style>
      <div style={{height:"100vh",display:"flex",flexDirection:"column",background:T.bg,color:T.offWhite,fontFamily:"'Barlow',sans-serif",overflow:"hidden"}}>

        {/* HEADER */}
        <div style={{padding:"9px 18px",background:T.bgPanel,borderBottom:`2px solid ${T.orange}`,display:"flex",alignItems:"center",gap:14,flexShrink:0,boxShadow:`0 2px 20px rgba(247,105,0,0.2)`}}>
          <div style={{display:"flex",alignItems:"center",gap:10}}>
            <div style={{fontSize:28}}>🌍</div>
            <div>
              <div style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:20,fontWeight:900,letterSpacing:4,color:T.blueP,textTransform:"uppercase"}}>Run Stats</div>
              <div style={{fontSize:9,color:T.muted,letterSpacing:3,textTransform:"uppercase",marginTop:-2}}>Interactive Map</div>
            </div>
          </div>

          <input value={search} onChange={e=>setSearch(e.target.value)} placeholder="Search athletes, colleges, cities..."
            style={{flex:1,maxWidth:300,background:"#F7F7F8",border:`1px solid ${T.border}`,borderRadius:7,padding:"6px 12px",color:T.offWhite,fontSize:12,fontFamily:"'Barlow',sans-serif",outline:"none"}}/>

          <div style={{display:"flex",background:T.bgCard,borderRadius:7,border:`1px solid ${T.border}`,overflow:"hidden"}}>
            {[["flows","Flows"],["hometown","Home"],["college","College"],["heatmap","Heat"]].map(([m,l])=>(
              <button key={m} onClick={()=>switchMapMode(m)} style={{background:mapMode===m?T.orange:"transparent",border:"none",color:mapMode===m?T.white:T.muted,padding:"6px 14px",fontSize:13,cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase",transition:"all 0.15s",fontWeight:mapMode===m?700:400}}>{l}</button>
            ))}
          </div>

          <div style={{background:T.orangeGlow,border:`1px solid ${T.orange}44`,borderRadius:7,padding:"5px 14px",textAlign:"center"}}>
            <div style={{color:T.muted,fontSize:10,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase"}}>Avg Distance</div>
            <div style={{color:T.orange,fontSize:20,fontWeight:900,fontFamily:"'Barlow Condensed',sans-serif"}}>{overallAvg.toLocaleString()} mi</div>
          </div>

          <div style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:7,padding:"5px 14px",textAlign:"center",minWidth:90}}>
            <div style={{color:T.muted,fontSize:10,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase"}}>Athletes</div>
            <div style={{color:T.offWhite,fontSize:20,fontWeight:900,fontFamily:"'Barlow Condensed',sans-serif"}}>
              {loading ? <span style={{color:T.grayM,fontSize:14}}>...</span> : <>{filtered.length}<span style={{color:T.dim,fontSize:13}}>/{athletes.length}</span></>}
            </div>
          </div>
        </div>

        <div style={{flex:1,display:"flex",overflow:"hidden"}}>

          {/* LEFT FILTER PANEL */}
          <div style={{width:leftCollapsed?36:210,background:T.bgPanel,borderRight:`1px solid ${T.border}`,display:"flex",flexDirection:"column",flexShrink:0,transition:"width 0.2s",overflow:"hidden"}}>
            {/* Collapse toggle */}
            <div style={{display:"flex",alignItems:"center",justifyContent:leftCollapsed?"center":"space-between",padding:leftCollapsed?"8px 0":"8px 11px",borderBottom:`1px solid ${T.border}`,flexShrink:0}}>
              {!leftCollapsed && <span style={{fontFamily:"'Barlow Condensed',sans-serif",fontSize:11,letterSpacing:2,color:T.orange,textTransform:"uppercase"}}>Filters</span>}
              <button onClick={()=>setLeftCollapsed(v=>!v)} title={leftCollapsed?"Expand filters":"Collapse filters"} style={{background:"none",border:`1px solid ${T.border}`,borderRadius:4,color:T.grayM,fontSize:11,cursor:"pointer",padding:"2px 6px",lineHeight:1}}>
                {leftCollapsed?"›":"‹"}
              </button>
            </div>
            {!leftCollapsed && (
              <div style={{flex:1,overflowY:"auto",padding:"10px 11px"}}>
                <div style={{display:"flex",justifyContent:"flex-end",marginBottom:6}}>
                  {hasFilters && <button onClick={()=>{setFilters({...BLANK_FILTERS});setSearch("");setSelectedStates([]);}} style={{background:"none",border:"none",color:T.red,fontSize:10,cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>CLEAR</button>}
                </div>

                <FilterControls
                  filters={filters}
                  setFilters={setFiltersWithCleanup}
                  showSeason={true}
                  selectedStates={selectedStates}
                  onStatesChange={setSelectedStates}
                  mapMode={mapMode}
                  allConferences={allConferences}
                  getConfColleges={getConfColleges}
                  allAthletes={athletes}
                  performanceRanges={performanceRanges}
                  onRangeChange={handleRangeChange}
                />

                <div style={{borderTop:`1px solid ${T.border}`,paddingTop:10,marginTop:4}}>
                  <div style={{color:T.dim,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>
                    Athletes ({filtered.length})
                  </div>
                  {loading && <div style={{color:T.dim,fontSize:11,textAlign:"center",padding:"20px 0"}}>Loading...</div>}
                  {error && <div style={{color:T.red,fontSize:11,textAlign:"center",padding:"12px 0"}}>Error: {error}</div>}
                  {!loading && !error && filtered.length===0 && (
                    <div style={{color:T.dim,fontSize:11,textAlign:"center",padding:"20px 0"}}>No athletes match filters</div>
                  )}
                  {filtered.slice(0,80).map(a=>{
                    const d=a.hometownCoords?Math.round(haversine(a.hometownCoords,a.collegeCoords)):null;
                    const isSel=selectedAthlete?.id===a.id;
                    return (
                      <button key={a.id} onClick={()=>handleAthleteClick(a)} style={{display:"block",width:"100%",textAlign:"left",background:isSel?T.orangeGlow:"rgba(255,255,255,0.02)",border:`1px solid ${isSel?T.orange:T.border}`,borderRadius:5,padding:"5px 7px",marginBottom:2,cursor:"pointer",transition:"all 0.12s"}}>
                        <div style={{color:isSel?T.orange:T.offWhite,fontSize:12,fontWeight:700,fontFamily:"'Barlow Condensed',sans-serif"}}>{a.name}</div>
                        <div style={{display:"flex",justifyContent:"space-between",marginTop:1}}>
                          <span style={{color:T.dim,fontSize:10,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",maxWidth:110}}>{a.college}</span>
                          <span style={{color:distColor(d),fontSize:10,fontFamily:"monospace"}}>{fmtDist(d)}</span>
                        </div>
                      </button>
                    );
                  })}
                  {filtered.length>80 && <div style={{color:T.dim,fontSize:10,textAlign:"center",marginTop:6}}>+{filtered.length-80} more — use filters</div>}
                </div>
              </div>
            )}
          </div>

          {/* MAP */}
          <div style={{flex:1,position:"relative",overflow:"hidden"}}>
            <USMap athletes={filtered} onAthleteClick={handleAthleteClick} selectedAthlete={selectedAthlete} highlightCollege={highlightCollege} highlightHometown={highlightHometown} mapMode={mapMode} selectedStates={selectedStates}/>

            {selectedStates.length>0 && mapMode==="flows" && (
              <div style={{position:"absolute",top:14,left:"50%",transform:"translateX(-50%)",background:"#FFFFFF",border:`2px solid ${T.orange}`,borderRadius:20,padding:"5px 16px",boxShadow:`0 2px 16px rgba(247,105,0,0.2)`,display:"flex",alignItems:"center",gap:10,zIndex:10,whiteSpace:"nowrap"}}>
                <div style={{width:7,height:7,borderRadius:"50%",background:T.orange}}/>
                <span style={{color:T.orange,fontFamily:"'Barlow Condensed',sans-serif",fontSize:12,fontWeight:800,letterSpacing:1}}>
                  {selectedStates.length===1?STATE_NAMES[selectedStates[0]]:selectedStates.length+" States"} · {filtered.filter(a=>selectedStates.includes(getState(a.hometown))).length} athletes
                </span>
                <button onClick={()=>setSelectedStates([])} style={{background:"none",border:`1px solid ${T.border}`,color:T.muted,borderRadius:10,cursor:"pointer",fontSize:10,padding:"1px 7px",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>✕ Clear</button>
              </div>
            )}

            <div style={{position:"absolute",bottom:14,left:14,background:"rgba(255,255,255,0.96)",border:`1px solid ${T.border}`,borderRadius:9,padding:"9px 13px",boxShadow:`0 2px 16px rgba(0,0,0,0.10)`}}>
              {mapMode==="heatmap" ? (
                <div>
                  <div style={{color:T.muted,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>Hometown Density</div>
                  <div style={{display:"flex",gap:3,alignItems:"center",marginBottom:4}}>
                    <div style={{height:7,borderRadius:3,background:"linear-gradient(to right,#FFF8F4,#F3BC9F,#CA6038,#880302)"}}/>
                  </div>
                  <div style={{display:"flex",justifyContent:"space-between"}}><span style={{color:T.dim,fontSize:9}}>Low</span><span style={{color:T.dim,fontSize:9}}>High</span></div>
                  <div style={{color:T.muted,fontSize:9,marginTop:4}}>{filtered.length} athletes shown</div>
                </div>
              ) : mapMode==="flows" ? (
                <div>
                  <div style={{color:T.muted,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>Arc = Distance</div>
                  <div style={{display:"flex",gap:12,flexWrap:"wrap"}}>
                    {[[DIST_COLORS.local,"Local"],[DIST_COLORS.regional,"Regional"],[DIST_COLORS.far,"Long Haul"],[DIST_COLORS.extreme,"Cross-Country"]].map(([c,l])=>(
                      <div key={l} style={{display:"flex",gap:5,alignItems:"center"}}><div style={{width:18,height:2,background:c,borderRadius:1}}/><span style={{color:T.muted,fontSize:10}}>{l}</span></div>
                    ))}
                  </div>
                  <div style={{display:"flex",gap:12,marginTop:5}}>
                    <div style={{display:"flex",gap:5,alignItems:"center"}}><div style={{width:7,height:7,borderRadius:"50%",background:T.muted}}/><span style={{color:T.muted,fontSize:10}}>Hometown</span></div>
                    <div style={{display:"flex",gap:5,alignItems:"center"}}><div style={{width:7,height:7,background:T.muted,borderRadius:1}}/><span style={{color:T.muted,fontSize:10}}>College</span></div>
                  </div>
                </div>
              ) : (
                <div style={{display:"flex",gap:12,alignItems:"center"}}>
                  <div style={{display:"flex",gap:5,alignItems:"center"}}><div style={{width:8,height:8,borderRadius:"50%",background:T.orangeL}}/><span style={{color:T.muted,fontSize:10}}>Athlete</span></div>
                  <div style={{display:"flex",gap:5,alignItems:"center"}}><div style={{width:10,height:10,borderRadius:"50%",background:T.orange,border:`2px solid ${T.white}`}}/><span style={{color:T.muted,fontSize:10}}>Selected</span></div>
                  <span style={{color:T.dim,fontSize:10}}>{mapMode==="college"?"College":"Hometown"} view</span>
                </div>
              )}
            </div>
          </div>

          {/* RIGHT PANEL */}
          <div style={{width:rightCollapsed?36:360,background:T.bgPanel,borderLeft:`1px solid ${T.border}`,display:"flex",flexDirection:"column",flexShrink:0,transition:"width 0.2s",overflow:"hidden"}}>
            {/* Collapse toggle + tabs */}
            <div style={{display:"flex",alignItems:"center",borderBottom:`1px solid ${T.border}`,flexShrink:0}}>
              <button onClick={()=>setRightCollapsed(v=>!v)} title={rightCollapsed?"Expand panel":"Collapse panel"} style={{background:"none",border:"none",borderRight:`1px solid ${T.border}`,color:T.grayM,fontSize:11,cursor:"pointer",padding:"10px 10px",lineHeight:1,flexShrink:0}}>
                {rightCollapsed?"‹":"›"}
              </button>
              {!rightCollapsed && [["athlete","Athlete"],["college","Pull"],["hometown","Origin"],["heatmap","Heat"]].map(([tab,label])=>(
                <button key={tab} onClick={()=>switchRightTab(tab)} style={{flex:1,padding:"10px 3px",background:rightTab===tab?T.orangeGlow:"transparent",border:"none",borderBottom:rightTab===tab?`2px solid ${T.orange}`:"2px solid transparent",color:rightTab===tab?T.orange:T.muted,fontSize:11,cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase",transition:"all 0.12s",fontWeight:rightTab===tab?700:400}}>{label}</button>
              ))}
            </div>
            {!rightCollapsed && (
              <div style={{flex:1,overflowY:"auto"}}>
                {rightTab==="athlete"  && <AthleteDetail athlete={selectedAthlete} onClose={()=>setSelectedAthlete(null)} allAthletes={athletes}/>}
                {rightTab==="college"  && <CollegePullPanel athletes={filtered} focusedCollege={focusedCollege} onFocusCollege={handleFocusCollege}/>}
                {rightTab==="hometown" && <HometownPanel athletes={filtered} focusedHometown={focusedHometown} onFocusHometown={handleFocusHometown}/>}
                {rightTab==="heatmap"  && <HeatmapPanel athletes={filtered}/>}
              </div>
            )}
          </div>

        </div>
      </div>
    </>
  );
}
