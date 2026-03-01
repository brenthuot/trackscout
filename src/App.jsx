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
  {id:"60m",    label:"60m",           season:"indoor" },
  {id:"60mH",   label:"60mH",          season:"indoor" },
  {id:"100m",   label:"100m",          season:"outdoor"},
  {id:"200m",   label:"200m",          season:"both"   },
  {id:"400m",   label:"400m",          season:"both"   },
  {id:"800m",   label:"800m",          season:"both"   },
  {id:"1500m",  label:"1500m",         season:"both"   },
  {id:"Mile",   label:"Mile",          season:"both"   },
  {id:"3000m",  label:"3000m",         season:"indoor" },
  {id:"5000m",  label:"5000m",         season:"both"   },
  {id:"10000m", label:"10000m",        season:"outdoor"},
  {id:"110mH",  label:"110mH",         season:"outdoor"},
  {id:"400mH",  label:"400mH",         season:"outdoor"},
  {id:"LJ",     label:"Long Jump",     season:"both"   },
  {id:"TJ",     label:"Triple Jump",   season:"both"   },
  {id:"HJ",     label:"High Jump",     season:"both"   },
  {id:"PV",     label:"Pole Vault",    season:"both"   },
  {id:"SP",     label:"Shot Put",      season:"both"   },
  {id:"WT",     label:"Weight Throw",  season:"indoor" },
  {id:"DT",     label:"Discus",        season:"outdoor"},
  {id:"JT",     label:"Javelin",       season:"outdoor"},
  {id:"Pent",   label:"Pentathlon",    season:"indoor" },
  {id:"Hept",   label:"Heptathlon",    season:"outdoor"},
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
  // Must match exactly: "City Name, ST" — city part only letters/spaces/periods/hyphens/apostrophes
  // Reject anything with digits, "on ", "Championships", "Meet", etc.
  const match = s.match(/^([A-Za-z][A-Za-z\s\.\-']{1,40}),\s+([A-Z]{2})$/);
  if (!match) return null;
  const city = match[1].trim();
  const state = match[2];
  // Reject if city contains suspicious words
  const bad = /championship|meet|invit|classic|relay|cross.?country|track|indoor|outdoor|university|college|vs|on/i;
  if (bad.test(city)) return null;
  if (!STATE_CAPITALS[state]) return null;
  return STATE_CAPITALS[state];
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
const distColor = d => !d ? T.dim : d<100?T.green:d<400?T.yellow:d<800?T.orange:T.red;
const distLabel = d => !d ? "Unknown" : d<100?"Local (<100 mi)":d<400?"Regional (100-400 mi)":d<800?"Long Haul (400-800 mi)":"Cross-Country (800+ mi)";

// ── HEATMAP CANVAS ────────────────────────────────────────────────────────────
function drawHeatmap(canvas, athletes, projection) {
  if (!canvas || !projection || athletes.length === 0) return;
  const W = canvas.width, H = canvas.height;
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, W, H);
  const pts = athletes.filter(a=>a.hometownCoords).map(a => projection([a.hometownCoords[1], a.hometownCoords[0]])).filter(Boolean);
  if (!pts.length) return;
  const R = 14, bw = R / 2.8;
  const density = new Float32Array(W * H);
  pts.forEach(([px, py]) => {
    const x0=Math.max(0,(px-R)|0), x1=Math.min(W-1,(px+R+1)|0);
    const y0=Math.max(0,(py-R)|0), y1=Math.min(H-1,(py+R+1)|0);
    for (let y=y0; y<=y1; y++) for (let x=x0; x<=x1; x++) {
      const dx=x-px, dy=y-py, d2=dx*dx+dy*dy;
      if (d2 < R*R) density[y*W+x] += Math.exp(-d2/(2*bw*bw));
    }
  });
  const vals = Array.from(density).filter(v=>v>0).sort((a,b)=>a-b);
  const mx = vals[Math.floor(vals.length*0.92)] || vals[vals.length-1] || 1;
  const STOPS = [
    [0.00, null],
    [0.06, [0,0,200,160]],
    [0.22, [0,180,220,190]],
    [0.42, [0,210,80,210]],
    [0.62, [255,230,0,225]],
    [0.80, [255,120,0,235]],
    [1.00, [255,0,0,245]],
  ];
  const lerp = (a,b,t) => a+(b-a)*t;
  const img = ctx.createImageData(W, H);
  for (let i=0; i<density.length; i++) {
    const t = Math.min(1, density[i]/mx);
    if (t < STOPS[1][0]) continue;
    let s0=STOPS[1], s1=STOPS[2];
    for (let k=1; k<STOPS.length-1; k++) {
      if (t >= STOPS[k][0] && t <= STOPS[k+1][0]) { s0=STOPS[k]; s1=STOPS[k+1]; break; }
    }
    if (t > STOPS[STOPS.length-1][0]) { s0=STOPS[STOPS.length-2]; s1=STOPS[STOPS.length-1]; }
    const f = s1[0]===s0[0] ? 1 : (t-s0[0])/(s1[0]-s0[0]);
    const c0=s0[1], c1=s1[1], idx=i*4;
    img.data[idx]   = Math.round(lerp(c0[0],c1[0],f));
    img.data[idx+1] = Math.round(lerp(c0[1],c1[1],f));
    img.data[idx+2] = Math.round(lerp(c0[2],c1[2],f));
    img.data[idx+3] = Math.round(lerp(c0[3],c1[3],f));
  }
  ctx.putImageData(img, 0, 0);
}

// ── US MAP ────────────────────────────────────────────────────────────────────
function USMap({athletes, onAthleteClick, selectedAthlete, highlightCollege, highlightHometown, mapMode, selectedStates}) {
  const svgRef=useRef(null), canvasRef=useRef(null), projRef=useRef(null);
  const [geo,setGeo]=useState(null), [tooltip,setTooltip]=useState(null);
  const FIPS_ABBR={"01":"AL","02":"AK","04":"AZ","05":"AR","06":"CA","08":"CO","09":"CT","10":"DE","11":"DC","12":"FL","13":"GA","15":"HI","16":"ID","17":"IL","18":"IN","19":"IA","20":"KS","21":"KY","22":"LA","23":"ME","24":"MD","25":"MA","26":"MI","27":"MN","28":"MS","29":"MO","30":"MT","31":"NE","32":"NV","33":"NH","34":"NJ","35":"NM","36":"NY","37":"NC","38":"ND","39":"OH","40":"OK","41":"OR","42":"PA","44":"RI","45":"SC","46":"SD","47":"TN","48":"TX","49":"UT","50":"VT","51":"VA","53":"WA","54":"WV","55":"WI","56":"WY"};

  useEffect(() => {
    const load = tj => { fetch("https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json").then(r=>r.json()).then(us=>setGeo(tj.feature(us,us.objects.states))); };
    if (window.topojson) { load(window.topojson); return; }
    const sc=document.createElement("script"); sc.src="https://cdn.jsdelivr.net/npm/topojson-client@3/dist/topojson-client.min.js"; sc.onload=()=>load(window.topojson); document.head.appendChild(sc);
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
      .attr("fill",d=>{ const abbr=FIPS_ABBR[String(d.id).padStart(2,"0")]; if(!hasStateFilter) return "#E6E7EE"; return selectedStates.includes(abbr)?"#FCC399":"#F1F2F5"; })
      .attr("stroke",d=>{ const abbr=FIPS_ABBR[String(d.id).padStart(2,"0")]; return hasStateFilter&&selectedStates.includes(abbr)?T.orange:"#CCCFDD"; })
      .attr("stroke-width",d=>{ const abbr=FIPS_ABBR[String(d.id).padStart(2,"0")]; return hasStateFilter&&selectedStates.includes(abbr)?2:0.8; })
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
      dimmed.forEach(a=>{
        const h=px(a.hometownCoords),c=px(a.collegeCoords); if(!h||!c) return;
        const dx=c[0]-h[0],dy=c[1]-h[1],dr=Math.sqrt(dx*dx+dy*dy)*0.55;
        g.append("path").attr("d",`M${h[0]},${h[1]} A${dr},${dr} 0 0,1 ${c[0]},${c[1]}`).attr("fill","none").attr("stroke","rgba(0,0,0,0.04)").attr("stroke-width",0.8);
      });
      active.forEach(a=>{
        const h=px(a.hometownCoords),c=px(a.collegeCoords); if(!h||!c) return;
        if(!a.hometownCoords) return;
        const dist=haversine(a.hometownCoords,a.collegeCoords), col=distColor(dist), isSel=selectedAthlete?.id===a.id;
        const dx=c[0]-h[0],dy=c[1]-h[1],dr=Math.sqrt(dx*dx+dy*dy)*0.55;
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
  }, [geo,athletes,selectedAthlete,highlightCollege,highlightHometown,mapMode,selectedStates]);

  useEffect(() => {
    if (mapMode!=="heatmap"||!canvasRef.current||!projRef.current) return;
    drawHeatmap(canvasRef.current, athletes, projRef.current);
  }, [mapMode, athletes, geo]);

  return (
    <div style={{position:"relative",width:"100%",height:"100%"}}>
      <svg ref={svgRef} style={{width:"100%",height:"100%",display:"block"}}/>
      <canvas ref={canvasRef} style={{position:"absolute",top:0,left:0,pointerEvents:"none",opacity:mapMode==="heatmap"?1:0,transition:"opacity 0.3s",width:"100%",height:"100%"}}
        width={svgRef.current?.clientWidth||960} height={svgRef.current?.clientHeight||560}/>
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
            return <Chip key={e.id} label={e.id} active={filters.events.includes(e.id)} onClick={()=>toggleEvent(e.id)} color={col}/>;
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
    if (filters.conference && a.conference!==filters.conference) return false;
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

  const topCities = useMemo(() => {
    const map={};
    athletes.forEach(a=>{if(!a.hometown||!a.hometownCoords)return;if(!map[a.hometown])map[a.hometown]={city:a.hometown,count:0};map[a.hometown].count++;});
    return Object.values(map).sort((a,b)=>b.count-a.count).slice(0,15);
  }, [athletes]);

  const topStates = useMemo(() => {
    const map={};
    athletes.forEach(a=>{
      if(!a.hometown||!a.hometownCoords)return;
      const st=getState(a.hometown); if(!st) return;
      if(!map[st]) map[st]={abbr:st,name:STATE_NAMES[st]||st,count:0,cities:new Set()};
      map[st].count++; map[st].cities.add(a.hometown);
    });
    return Object.values(map).map(s=>({...s,cityCount:s.cities.size})).sort((a,b)=>b.count-a.count).slice(0,15);
  }, [athletes]);

  const uniqueStateCount = useMemo(() => {
    const s=new Set(); athletes.forEach(a=>{const st=getState(a.hometown);if(st)s.add(st);}); return s.size;
  }, [athletes]);

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
          <StatCard label="Cities" value={topCities.length} color={T.blueL}/>
          <StatCard label="States" value={uniqueStateCount} color={T.blueM}/>
        </div>
      </div>
      <div style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:8,padding:"10px 12px",marginBottom:14}}>
        <div style={{color:T.muted,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:7}}>Density Scale</div>
        <div style={{display:"flex",height:10,borderRadius:4,overflow:"hidden",marginBottom:5}}>
          {["#0000C8","#00B4DC","#00D250","#FFE600","#FF7800","#FF0000"].map((c,i)=><div key={i} style={{flex:1,background:c}}/>)}
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
function EventProgressChart({event, performances}) {
  const [expanded, setExpanded] = useState(false);
  const field = isFieldEvent(event);

  const perfs = performances
    .filter(p => p.event === event && p.mark && p.year)
    .sort((a,b) => a.year - b.year || (field ? b.mark - a.mark : a.mark - b.mark));

  if (perfs.length < 2) return null;

  const years = [...new Set(perfs.map(p=>p.year))].sort();
  const marks = perfs.map(p=>p.mark);
  const minMark = Math.min(...marks), maxMark = Math.max(...marks);
  const markRange = maxMark - minMark || 1;
  const prMark = field ? Math.max(...marks) : Math.min(...marks);

  const yearBests = years.map(yr => {
    const yp = perfs.filter(p=>p.year===yr);
    const best = field ? yp.reduce((b,p)=>p.mark>b.mark?p:b) : yp.reduce((b,p)=>p.mark<b.mark?p:b);
    return {yr, mark:best.mark, display:best.mark_display||fmtTime(best.mark)};
  });

  // ── COLLAPSED: one dot per year best ──────────────────────────────────────
  const CVW=260, CVH=120, CP={top:18,right:14,bottom:26,left:44};
  const cx = yr => CP.left + (years.indexOf(yr)/Math.max(years.length-1,1))*(CVW-CP.left-CP.right);
  const cy = m => { const n=field?(m-minMark)/markRange:1-(m-minMark)/markRange; return CP.top+n*(CVH-CP.top-CP.bottom); };
  const cLine = yearBests.map(b=>`${cx(b.yr)},${cy(b.mark)}`).join(" ");

  // ── EXPANDED: every performance, evenly spaced, year labels at boundaries ─
  const EVW=260, EVH=220, EP={top:18,right:14,bottom:40,left:44};
  const ex = i => EP.left + (i/Math.max(perfs.length-1,1))*(EVW-EP.left-EP.right);
  const ey = m => { const n=field?(m-minMark)/markRange:1-(m-minMark)/markRange; return EP.top+n*(EVH-EP.top-EP.bottom); };
  const eLine = perfs.map((_,i)=>`${ex(i)},${ey(perfs[i].mark)}`).join(" ");

  // Year boundary lines & labels: first perf index of each year
  const yearBounds = years.map(yr => ({yr, i: perfs.findIndex(p=>p.year===yr)}));

  return (
    <div style={{marginBottom:14,borderBottom:`1px solid ${T.border}`,paddingBottom:10}}>
      <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:6}}>
        <span style={{color:T.orange,fontSize:11,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase",fontWeight:700}}>{event}</span>
        <div style={{display:"flex",alignItems:"center",gap:8}}>
          <span style={{color:T.grayM,fontSize:9,fontFamily:"monospace"}}>
            {perfs.length} marks · PR: <span style={{color:T.orange,fontWeight:700}}>{fmtTime(prMark)}</span>
          </span>
          <button onClick={()=>setExpanded(e=>!e)} style={{
            background:"transparent",border:`1px solid ${T.border}`,borderRadius:4,
            color:T.grayM,fontSize:9,cursor:"pointer",padding:"2px 7px",
            fontFamily:"'Barlow Condensed',sans-serif",
          }}>{expanded?"▲":"▼"}</button>
        </div>
      </div>

      {/* COLLAPSED */}
      {!expanded && (
        <svg viewBox={`0 0 ${CVW} ${CVH}`} width="100%" style={{display:"block"}}>
          {[0,0.5,1].map(t=>{
            const y=CP.top+t*(CVH-CP.top-CP.bottom);
            const val=field?maxMark-t*markRange:minMark+t*markRange;
            return <g key={t}>
              <line x1={CP.left} x2={CVW-CP.right} y1={y} y2={y} stroke={T.border} strokeWidth={0.5}/>
              <text x={CP.left-4} y={y+3} textAnchor="end" fontSize={8} fill={T.grayM} fontFamily="monospace">{fmtTime(val)}</text>
            </g>;
          })}
          {years.map(yr=><text key={yr} x={cx(yr)} y={CVH-6} textAnchor="middle" fontSize={9} fill={T.grayM} fontFamily="'Barlow Condensed',sans-serif">{yr}</text>)}
          <polyline points={cLine} fill="none" stroke={T.orange} strokeWidth={1.8} strokeOpacity={0.6}/>
          {yearBests.map((b,i)=>{
            const isPR=b.mark===prMark;
            return <g key={i}>
              <circle cx={cx(b.yr)} cy={cy(b.mark)} r={isPR?4.5:3} fill={isPR?T.orange:T.bgCard} stroke={isPR?T.orangeD:T.borderH} strokeWidth={isPR?1.5:1}><title>{b.display} ({b.yr})</title></circle>
              <text x={cx(b.yr)} y={cy(b.mark)-8} textAnchor="middle" fontSize={8} fill={T.orange} fontFamily="monospace" fontWeight="700">{b.display}</text>
            </g>;
          })}
        </svg>
      )}

      {/* EXPANDED: all perfs, x = chronological order, year dividers on x-axis */}
      {expanded && (
        <>
          <svg viewBox={`0 0 ${EVW} ${EVH}`} width="100%" style={{display:"block"}}>
            {/* Y gridlines + labels */}
            {[0,0.5,1].map(t=>{
              const y=EP.top+t*(EVH-EP.top-EP.bottom);
              const val=field?maxMark-t*markRange:minMark+t*markRange;
              return <g key={t}>
                <line x1={EP.left} x2={EVW-EP.right} y1={y} y2={y} stroke={T.border} strokeWidth={0.5}/>
                <text x={EP.left-4} y={y+3} textAnchor="end" fontSize={8} fill={T.grayM} fontFamily="monospace">{fmtTime(val)}</text>
              </g>;
            })}
            {/* Year boundary vertical lines + year labels below x-axis */}
            {yearBounds.map(({yr,i},idx)=>{
              const x=ex(i);
              const isLast=idx===yearBounds.length-1;
              return <g key={yr}>
                {i>0 && <line x1={x} x2={x} y1={EP.top} y2={EVH-EP.bottom+4} stroke={T.border} strokeWidth={0.8} strokeDasharray="3,2"/>}
                <text x={isLast ? Math.min(x, EVW-EP.right-4) : x} y={EVH-EP.bottom+14} textAnchor={isLast?"end":"start"} fontSize={9} fill={T.grayM} fontFamily="'Barlow Condensed',sans-serif" fontWeight="600">{yr}</text>
              </g>;
            })}
            {/* Connect-the-dots line */}
            <polyline points={eLine} fill="none" stroke={T.orange} strokeWidth={1.5} strokeOpacity={0.5}/>
            {/* Individual performance dots */}
            {perfs.map((p,i)=>{
              const isPR=p.mark===prMark;
              return <g key={i}>
                <circle cx={ex(i)} cy={ey(p.mark)} r={isPR?5:3}
                  fill={isPR?T.orange:"rgba(0,0,0,0.07)"}
                  stroke={isPR?T.orangeD:T.borderH}
                  strokeWidth={isPR?1.5:0.8}>
                  <title>{p.mark_display||fmtTime(p.mark)}{p.meet_name?` — ${p.meet_name}`:""} ({p.year})</title>
                </circle>
                {isPR && <text x={ex(i)} y={ey(p.mark)-9} textAnchor="middle" fontSize={8} fill={T.orange} fontFamily="monospace" fontWeight="700">{p.mark_display||fmtTime(p.mark)}</text>}
              </g>;
            })}
          </svg>

          {/* Performance list */}
          <div style={{marginTop:6,maxHeight:180,overflowY:"auto",borderTop:`1px solid ${T.border}`,paddingTop:6}}>
            {(() => {
              const sorted = [...perfs].reverse();
              // Compute season bests: best mark per (year+season) combo
              const seasonBestSet = new Set();
              const grouped = {};
              perfs.forEach(p => {
                const key = `${p.year}-${p.season}`;
                if (!grouped[key]) grouped[key] = [];
                grouped[key].push(p);
              });
              Object.values(grouped).forEach(group => {
                const best = field
                  ? group.reduce((b,p)=>p.mark>b.mark?p:b)
                  : group.reduce((b,p)=>p.mark<b.mark?p:b);
                seasonBestSet.add(best);
              });

              return sorted.map((p, i) => {
                const isPR = p.mark === prMark;
                const isSB = !isPR && seasonBestSet.has(p);
                const prevP = sorted[i - 1];
                const seasonChanged = prevP && (prevP.season !== p.season || prevP.year !== p.year);
                const markColor = isPR ? T.orange : isSB ? T.blueL : T.offWhite;
                const markWeight = (isPR || isSB) ? 700 : 400;
                return (
                  <div key={i}>
                    {seasonChanged && (
                      <div style={{borderTop:`1px dashed ${T.border}`,margin:"4px 0"}}/>
                    )}
                    <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"3px 2px",borderBottom:`1px solid ${T.border}22`}}>
                      <span style={{fontFamily:"monospace",fontSize:11,color:markColor,fontWeight:markWeight,display:"flex",alignItems:"center",gap:5}}>
                        {p.mark_display||fmtTime(p.mark)}
                        {isPR && <span style={{fontSize:8,background:T.orange,color:"#fff",borderRadius:3,padding:"1px 4px",letterSpacing:0.5}}>PR</span>}
                        {isSB && <span style={{fontSize:8,background:T.blueL,color:"#fff",borderRadius:3,padding:"1px 4px",letterSpacing:0.5}}>SB</span>}
                      </span>
                      <span style={{color:T.grayM,fontSize:10,maxWidth:"58%",overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",textAlign:"right"}}>
                        {p.meet_name||"—"} <span style={{color:T.grayL,marginLeft:4}}>{p.season ? `${p.season} ` : ""}{p.year}</span>
                      </span>
                    </div>
                  </div>
                );
              });
            })()}
          </div>
        </>
      )}
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
function AthleteDetail({athlete, onClose}) {
  if (!athlete) return (
    <div style={{display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",height:"100%",gap:12,padding:24}}>
      <div style={{fontSize:44,opacity:0.15}}>🌍</div>
      <div style={{color:T.dim,fontFamily:"'Barlow Condensed',sans-serif",fontSize:11,letterSpacing:3,textTransform:"uppercase",textAlign:"center",lineHeight:1.9}}>Select an athlete<br/>on the map</div>
    </div>
  );

  const dist=athlete.hometownCoords?haversine(athlete.hometownCoords,athlete.collegeCoords):0, dc=distColor(dist);
  const colEvents = Object.keys(athlete.collegeTimes);
  const hsEvents = Object.keys(athlete.hsTimes);

  return (
    <div style={{padding:"16px",overflowY:"auto",height:"100%"}}>
      <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:12}}>
        <div>
          <div style={{color:T.orange,fontFamily:"'Barlow Condensed',sans-serif",fontSize:21,fontWeight:900,letterSpacing:2}}>{athlete.name}</div>
          <div style={{color:T.offWhite,fontSize:12,marginTop:2}}>{athlete.college}</div>
          <div style={{color:T.muted,fontSize:11}}>{athlete.conference}{athlete.collegeYear ? ` · Year ${athlete.collegeYear}` : ""}</div>
        </div>
        <button onClick={onClose} style={{background:"none",border:`1px solid ${T.border}`,color:T.muted,borderRadius:6,cursor:"pointer",width:26,height:26,fontSize:13,display:"flex",alignItems:"center",justifyContent:"center"}}>✕</button>
      </div>

      <div style={{background:T.bgCard,border:`1px solid ${dc}55`,borderRadius:9,padding:"10px 12px",marginBottom:12}}>
        <div style={{color:T.muted,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:2,textTransform:"uppercase"}}>Recruitment Distance</div>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginTop:4}}>
          <div>
            <div style={{color:dc,fontSize:24,fontWeight:900,fontFamily:"'Barlow Condensed',sans-serif",lineHeight:1}}>{fmtDist(dist)}</div>
            <div style={{color:T.muted,fontSize:10,marginTop:3}}>{athlete.hometown} → {athlete.college}</div>
          </div>
          <div style={{fontSize:26}}>{dist<100?"🏠":dist<400?"🚗":dist<800?"✈️":"🌎"}</div>
        </div>
        <div style={{marginTop:7,display:"flex",gap:5,alignItems:"center"}}>
          <div style={{width:7,height:7,borderRadius:"50%",background:dc}}/>
          <span style={{color:dc,fontSize:11}}>{distLabel(dist)}</span>
        </div>
      </div>

      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:6,marginBottom:12}}>
        {[{l:"Hometown",v:athlete.hometown||"—"},{l:"High School",v:athlete.hsName||"—"},{l:"HS Grad",v:athlete.hsYear||"—"},{l:"College Year",v:athlete.collegeYear?`Year ${athlete.collegeYear}`:"—"}].map(({l,v})=>(
          <div key={l} style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:6,padding:"7px 9px"}}>
            <div style={{color:T.dim,fontSize:9,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase"}}>{l}</div>
            <div style={{color:T.offWhite,fontSize:11,marginTop:2,fontWeight:500}}>{v}</div>
          </div>
        ))}
      </div>

      {athlete.events.length > 0 && (
        <div style={{display:"flex",gap:3,flexWrap:"wrap",marginBottom:14}}>
          {athlete.events.map(e=>{
            const cfg=EVENTS_CFG.find(x=>x.id===e);
            const col=cfg?.season==="indoor"?T.blueL:cfg?.season==="outdoor"?T.green:T.orange;
            return <span key={e} style={{background:`${col}22`,color:col,borderRadius:4,padding:"2px 8px",fontSize:11,border:`1px solid ${col}44`,fontFamily:"'Barlow Condensed',sans-serif"}}>{e}</span>;
          })}
        </div>
      )}

      {(colEvents.length > 0 || hsEvents.length > 0) && (
        <>
          <SectionHead>Best Performances</SectionHead>
          {colEvents.length > 0 && (
            <div style={{marginBottom:10}}>
              <div style={{color:T.dim,fontSize:9,letterSpacing:1,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>College PRs</div>
              <div style={{display:"flex",gap:10,flexWrap:"wrap"}}>
                {colEvents.map(ev=>(
                  <div key={ev} style={{display:"flex",gap:4,alignItems:"baseline"}}>
                    <span style={{color:T.muted,fontSize:10,fontFamily:"'Barlow Condensed',sans-serif"}}>{ev}</span>
                    <span style={{color:T.orange,fontSize:13,fontWeight:800,fontFamily:"monospace"}}>{fmtTime(athlete.collegeTimes[ev])}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
          {hsEvents.length > 0 && (
            <div style={{marginBottom:10}}>
              <div style={{color:T.dim,fontSize:9,letterSpacing:1,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>HS Best Marks</div>
              <div style={{display:"flex",gap:10,flexWrap:"wrap"}}>
                {hsEvents.map(ev=>(
                  <div key={ev} style={{display:"flex",gap:4,alignItems:"baseline"}}>
                    <span style={{color:T.muted,fontSize:10,fontFamily:"'Barlow Condensed',sans-serif"}}>{ev}</span>
                    <span style={{color:T.offWhite,fontSize:13,fontWeight:800,fontFamily:"monospace"}}>{fmtTime(athlete.hsTimes[ev])}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
          {colEvents.filter(ev=>athlete.hsTimes[ev]).length > 0 && (
            <div style={{marginTop:6,padding:"8px 10px",background:T.bgCard,borderRadius:7,border:`1px solid ${T.border}`}}>
              <div style={{color:T.dim,fontSize:9,letterSpacing:1,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>HS Best → College PR</div>
              {colEvents.filter(ev=>athlete.hsTimes[ev]).map(ev=>{
                const diff = athlete.hsTimes[ev] - athlete.collegeTimes[ev];
                return (
                  <div key={ev} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"3px 0"}}>
                    <span style={{color:T.muted,fontSize:11,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1}}>{ev}</span>
                    <div style={{display:"flex",gap:8,alignItems:"center"}}>
                      <span style={{color:T.dim,fontSize:11,fontFamily:"monospace"}}>{fmtTime(athlete.hsTimes[ev])}</span>
                      <span style={{color:T.dim,fontSize:10}}>→</span>
                      <span style={{color:T.orange,fontSize:12,fontWeight:800,fontFamily:"monospace"}}>{fmtTime(athlete.collegeTimes[ev])}</span>
                      {diff>0 && <span style={{color:T.green,fontSize:10,fontFamily:"monospace",background:`${T.green}18`,borderRadius:3,padding:"1px 4px"}}>▼{diff.toFixed(2)}</span>}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
          {athlete.tfrrsUrl && (
            <a href={athlete.tfrrsUrl} target="_blank" rel="noopener noreferrer"
              style={{display:"block",marginTop:12,textAlign:"center",color:T.orange,fontSize:11,fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textDecoration:"none",border:`1px solid ${T.orange}44`,borderRadius:6,padding:"5px 0"}}>
              View on TFRRS →
            </a>
          )}
        </>
      )}

      {/* Year-over-year progress charts */}
      {athlete.rawPerformances?.length > 0 && (() => {
        const eventsWithData = [...new Set(athlete.rawPerformances.filter(p=>p.year).map(p=>p.event))];
        const chartableEvents = eventsWithData.filter(ev => {
          const years = new Set(athlete.rawPerformances.filter(p=>p.event===ev && p.year).map(p=>p.year));
          return years.size >= 2;
        });
        if (!chartableEvents.length) return null;
        return (
          <>
            <SectionHead>Year-over-Year Progress</SectionHead>
            <div style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:8,padding:"10px 12px",marginBottom:4}}>
              {chartableEvents.map(ev => (
                <EventProgressChart key={ev} event={ev} performances={athlete.rawPerformances}/>
              ))}
            </div>
          </>
        );
      })()}
    </div>
  );
}

// ── APP ───────────────────────────────────────────────────────────────────────
const BLANK_FILTERS = {events:[],conference:"",college:"",hsYear:"",collegeYear:"",season:"all"};

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
  const overallAvg = useMemo(() => { const withCoords=filtered.filter(a=>a.hometownCoords); return withCoords.length ? Math.round(withCoords.reduce((s,a)=>s+haversine(a.hometownCoords,a.collegeCoords),0)/withCoords.length) : 0; }, [filtered]);
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
              <button key={m} onClick={()=>switchMapMode(m)} style={{background:mapMode===m?T.orange:"transparent",border:"none",color:mapMode===m?T.white:T.muted,padding:"6px 12px",fontSize:11,cursor:"pointer",fontFamily:"'Barlow Condensed',sans-serif",letterSpacing:1,textTransform:"uppercase",transition:"all 0.15s",fontWeight:mapMode===m?700:400}}>{l}</button>
            ))}
          </div>

          <div style={{background:T.orangeGlow,border:`1px solid ${T.orange}44`,borderRadius:7,padding:"5px 12px",textAlign:"center"}}>
            <div style={{color:T.muted,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase"}}>Avg Distance</div>
            <div style={{color:T.orange,fontSize:16,fontWeight:900,fontFamily:"'Barlow Condensed',sans-serif"}}>{overallAvg.toLocaleString()} mi</div>
          </div>

          <div style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:7,padding:"5px 12px",textAlign:"center",minWidth:80}}>
            <div style={{color:T.muted,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase"}}>Athletes</div>
            <div style={{color:T.offWhite,fontSize:16,fontWeight:900,fontFamily:"'Barlow Condensed',sans-serif"}}>
              {loading ? <span style={{color:T.grayM,fontSize:12}}>...</span> : <>{filtered.length}<span style={{color:T.dim,fontSize:11}}>/{athletes.length}</span></>}
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
                    {["#0000C8","#00B4DC","#00D250","#FFE600","#FF7800","#FF0000"].map((c,i)=><div key={i} style={{width:22,height:7,background:c,borderRadius:1}}/>)}
                  </div>
                  <div style={{display:"flex",justifyContent:"space-between"}}><span style={{color:T.dim,fontSize:9}}>Low</span><span style={{color:T.dim,fontSize:9}}>High</span></div>
                  <div style={{color:T.muted,fontSize:9,marginTop:4}}>{filtered.length} athletes shown</div>
                </div>
              ) : mapMode==="flows" ? (
                <div>
                  <div style={{color:T.muted,fontSize:9,letterSpacing:2,fontFamily:"'Barlow Condensed',sans-serif",textTransform:"uppercase",marginBottom:6}}>Arc = Distance</div>
                  <div style={{display:"flex",gap:12,flexWrap:"wrap"}}>
                    {[[T.green,"Local"],[T.yellow,"Regional"],[T.orange,"Long Haul"],[T.red,"Cross-Country"]].map(([c,l])=>(
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
                {rightTab==="athlete"  && <AthleteDetail athlete={selectedAthlete} onClose={()=>setSelectedAthlete(null)}/>}
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
